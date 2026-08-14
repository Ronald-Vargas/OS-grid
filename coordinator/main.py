"""
Coordinador de OS Grid — Fases 1-6+

Cambios respecto a la versión anterior:
  - NO arranca automático al llegar el MIN_WORKERS-ésimo worker.
  - Espera que el dashboard envíe start_mission con tipo+intensidad.
  - Soporta tres tipos de tarea: hash, sort, primos.
  - Soporta tres intensidades: ligero, normal, pesado.
  - Se auto-reinicia después de cada corrida (3 s de gracia).
  - El canal /ws/dashboard ahora procesa mensajes entrantes.
"""

import asyncio
import json
import sys
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

import protocol as P
from coordinator import stats
from worker.tasks import hash_de

# ── Número secreto (SHA-256) ─────────────────────────────────────────────────
NUMERO_SECRETO = 8_450_137
OBJETIVO_HASH  = hash_de(NUMERO_SECRETO)

# ── Configuraciones por tipo × intensidad ────────────────────────────────────
CONFIGURACIONES = {
    "hash": {
        "ligero": {"rango": 3_000_000,  "chunks": 6},
        "normal": {"rango": 12_000_000, "chunks": 12},
        "pesado": {"rango": 40_000_000, "chunks": 18},
    },
    "sort": {
        "ligero": {"tam": 200_000,   "chunks": 6},
        "normal": {"tam": 1_000_000, "chunks": 12},
        "pesado": {"tam": 4_000_000, "chunks": 18},
    },
    "primos": {
        "ligero": {"rango": 1_000_000,  "chunks": 6},
        "normal": {"rango": 5_000_000,  "chunks": 12},
        "pesado": {"rango": 20_000_000, "chunks": 18},
    },
}

MIN_WORKERS = 3


# ── Construcción de la cola según tipo ───────────────────────────────────────

def construir_cola(tipo: str, intensidad: str) -> list:
    cfg = CONFIGURACIONES[tipo][intensidad]
    n   = cfg["chunks"]
    cola = []

    if tipo in ("hash", "primos"):
        rango   = cfg["rango"]
        tam_ch  = rango // n
        for i in range(n):
            cola.append({
                "id": i,
                "tipo_tarea": tipo,
                "inicio": i * tam_ch,
                "fin":    (i + 1) * tam_ch,
                "tam": 0,
            })

    elif tipo == "sort":
        tam = cfg["tam"]
        for i in range(n):
            cola.append({
                "id": i,
                "tipo_tarea": tipo,
                "inicio": 0,
                "fin": 0,
                "tam": tam,
            })

    return cola


# ── Estado global ─────────────────────────────────────────────────────────────

class Estado:
    def __init__(self):
        self.tipo_tarea  = "hash"
        self.intensidad  = "normal"
        self.cola        = []
        self.lock        = asyncio.Lock()
        self.workers     = {}          # nombre → {os, cpu, chunks_hechos}
        self.workers_ws  = {}          # nombre → WebSocket (para enviar RESET)
        self.resultados  = []
        self.encontrado  = None
        self.total_chunks = 0
        self.corrida_id  = None
        self.dashboards  = []
        self.arranque    = asyncio.Event()  # se dispara en start_mission

    async def broadcast(self, evento: dict):
        muertos = []
        for dash in self.dashboards:
            try:
                await dash.send_text(json.dumps(evento))
            except Exception:
                muertos.append(dash)
        for d in muertos:
            if d in self.dashboards:
                self.dashboards.remove(d)

    async def siguiente_chunk(self):
        async with self.lock:
            return self.cola.pop(0) if self.cola else None

    def reset(self):
        """Reinicia el estado de misión (mantiene workers conectados)."""
        self.cola         = []
        self.resultados   = []
        self.encontrado   = None
        self.total_chunks = 0
        self.corrida_id   = None
        self.arranque     = asyncio.Event()
        for w in self.workers.values():
            w["chunks_hechos"] = 0


estado = Estado()

# ── App FastAPI ───────────────────────────────────────────────────────────────

app = FastAPI(title="OS Grid Coordinator")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.get("/api/status")
def api_status():
    return {
        "servicio": "OS Grid Coordinator",
        "tipo_tarea": estado.tipo_tarea,
        "intensidad": estado.intensidad,
        "chunks_totales": estado.total_chunks,
        "chunks_restantes": len(estado.cola),
        "workers": list(estado.workers.keys()),
    }


# ── WebSocket de workers ──────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_worker(websocket: WebSocket):
    await websocket.accept()
    nombre = None
    try:
        while True:
            crudo = await websocket.receive_text()
            data  = json.loads(crudo)
            tipo  = data.get("type")

            # ── REGISTER ──────────────────────────────────────────────────
            if tipo == P.REGISTER:
                nombre = data.get("nombre", "desconocido")
                estado.workers[nombre]    = {
                    "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "chunks_hechos": 0,
                }
                estado.workers_ws[nombre] = websocket

                if estado.corrida_id is None and estado.cola:
                    # Corrida ya iniciada (worker tardó en conectarse): crear corrida si no existe
                    pass

                n = len(estado.workers)
                print(f"[COORD] Worker registrado: {nombre} ({data.get('os')}) — {n}/{MIN_WORKERS}")

                await websocket.send_text(json.dumps(
                    P.msg(P.REGISTERED,
                          objetivo=OBJETIVO_HASH,
                          chunks_totales=estado.total_chunks)
                ))

                if n >= MIN_WORKERS and not estado.arranque.is_set():
                    await estado.broadcast({
                        "type": "ready_to_configure",
                        "conectados": n,
                        "minimo": MIN_WORKERS,
                    })
                else:
                    await estado.broadcast({
                        "type": "waiting",
                        "conectados": n,
                        "minimo": MIN_WORKERS,
                        "faltantes": max(0, MIN_WORKERS - n),
                    })

                await estado.arranque.wait()
                await asignar(websocket, nombre)

            # ── METRICS ───────────────────────────────────────────────────
            elif tipo == P.METRICS:
                await estado.broadcast({
                    "type": "metrics",
                    "nombre": data.get("nombre"),
                    "chunk_id": data.get("chunk_id"),
                    "cpu_proc": data.get("cpu_proc"),
                    "cpu_sys":  data.get("cpu_sys"),
                    "ram_mb":   data.get("ram_mb"),
                })

            # ── RESULT ────────────────────────────────────────────────────
            elif tipo == P.RESULT:
                cid     = data.get("chunk_id")
                hallazgo = data.get("encontrado")
                tiempo  = data.get("tiempo", 0)

                estado.resultados.append(data)
                estado.workers[nombre]["chunks_hechos"] += 1
                if hallazgo is not None:
                    estado.encontrado = hallazgo

                so = estado.workers[nombre]["os"]
                stats.guardar_resultado(estado.corrida_id, nombre, so, data)

                print(f"[COORD] {nombre} terminó chunk #{cid} en {tiempo:.3f}s"
                      + ("  ← ¡ENCONTRADO!" if hallazgo else ""))

                await estado.broadcast({
                    "type": "chunk_done",
                    "nombre": nombre,
                    "os": so,
                    "chunk_id": cid,
                    "tiempo": tiempo,
                    "encontrado": hallazgo,
                    "chunks_hechos": estado.workers[nombre]["chunks_hechos"],
                    "completados": len(estado.resultados),
                    "total": estado.total_chunks,
                })
                await asignar(websocket, nombre)

    except WebSocketDisconnect:
        if nombre:
            estado.workers.pop(nombre, None)
            estado.workers_ws.pop(nombre, None)
            print(f"[COORD] Worker desconectado: {nombre}")


# ── Asignación de chunk ───────────────────────────────────────────────────────

async def asignar(websocket: WebSocket, nombre: str):
    chunk = await estado.siguiente_chunk()
    if chunk is None:
        await websocket.send_text(json.dumps(P.msg(P.NO_MORE)))
        if len(estado.resultados) == estado.total_chunks:
            resumen_consola()
            await broadcast_final()
        return

    print(f"[COORD] → {nombre}: chunk #{chunk['id']} "
          f"tipo={chunk['tipo_tarea']} [{chunk['inicio']},{chunk['fin']}) tam={chunk['tam']}")

    await websocket.send_text(json.dumps(P.msg(
        P.TASK_ASSIGN,
        chunk_id    = chunk["id"],
        tipo_tarea  = chunk["tipo_tarea"],
        inicio      = chunk["inicio"],
        fin         = chunk["fin"],
        tam         = chunk["tam"],
        objetivo    = OBJETIVO_HASH if chunk["tipo_tarea"] == "hash" else "",
    )))


# ── Final de misión ───────────────────────────────────────────────────────────

async def broadcast_final():
    filas    = stats.resumen_por_so(estado.corrida_id)
    por_so   = [{"so": so or "?", "chunks": chunks, "tiempo_prom": t,
                 "cpu_prom": cpu, "ram_prom": ram}
                for so, chunks, t, cpu, ram in filas]
    por_nodo = [{"nombre": n, "os": i["os"], "chunks": i["chunks_hechos"]}
                for n, i in estado.workers.items()]
    await estado.broadcast({
        "type": "mission_complete",
        "encontrado": estado.encontrado,
        "por_so": por_so,
        "por_nodo": por_nodo,
        "corrida_id": estado.corrida_id,
    })
    # Resetear INMEDIATAMENTE para que los workers que reconecten
    # en ~3s encuentren el arranque limpio (sin disparar).
    estado.reset()
    print("[COORD] Estado reseteado. Esperando workers para nueva corrida.")


def resumen_consola():
    print("\n" + "=" * 52)
    print(" MISIÓN COMPLETA")
    for n, i in estado.workers.items():
        print(f"  {n:<20} {i['chunks_hechos']} chunks")
    if estado.encontrado:
        print(f"\n  Número secreto: {estado.encontrado}")
    print("=" * 52 + "\n")


# ── WebSocket del dashboard ───────────────────────────────────────────────────

@app.websocket("/ws/dashboard")
async def ws_dashboard(websocket: WebSocket):
    await websocket.accept()
    estado.dashboards.append(websocket)
    print(f"[COORD] Dashboard conectado ({len(estado.dashboards)} activos)")

    # Snapshot inicial
    await websocket.send_text(json.dumps({
        "type": "snapshot",
        "tipo_tarea": estado.tipo_tarea,
        "intensidad": estado.intensidad,
        "chunks_totales": estado.total_chunks,
        "completados": len(estado.resultados),
        "min_workers": MIN_WORKERS,
        "arrancado": estado.arranque.is_set(),
        "workers": [
            {"nombre": n, "os": i["os"], "cpu": i["cpu"],
             "chunks_hechos": i["chunks_hechos"]}
            for n, i in estado.workers.items()
        ],
    }))

    try:
        while True:
            crudo = await websocket.receive_text()
            data  = json.loads(crudo)
            if data.get("type") == "start_mission":
                await iniciar_mision(
                    data.get("tipo", "hash"),
                    data.get("intensidad", "normal"),
                )
    except WebSocketDisconnect:
        if websocket in estado.dashboards:
            estado.dashboards.remove(websocket)
        print(f"[COORD] Dashboard desconectado ({len(estado.dashboards)} activos)")


async def iniciar_mision(tipo: str, intensidad: str):
    """Configura y dispara la misión cuando el dashboard lo ordena."""
    if estado.arranque.is_set():
        print("[COORD] Ya hay una misión en curso, ignorando start_mission.")
        return

    estado.tipo_tarea = tipo
    estado.intensidad = intensidad
    estado.cola       = construir_cola(tipo, intensidad)
    estado.total_chunks = len(estado.cola)
    estado.corrida_id = stats.nueva_corrida(OBJETIVO_HASH, estado.total_chunks)

    print(f"[COORD] Iniciando misión: tipo={tipo} intensidad={intensidad} "
          f"chunks={estado.total_chunks} corrida#{estado.corrida_id}")

    await estado.broadcast({
        "type": "mission_starting",
        "tipo": tipo,
        "intensidad": intensidad,
        "chunks_totales": estado.total_chunks,
    })
    estado.arranque.set()


# ── Archivos estáticos (dashboard Angular) ────────────────────────────────────

_static = Path(__file__).resolve().parent / "static"
if (_static / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(_static), html=True), name="static")
else:
    @app.get("/", response_class=HTMLResponse)
    def sin_dashboard():
        return ("<h2>OS Grid</h2>"
                "<p>Coordinador corriendo. Sin dashboard compilado.</p>"
                "<p><a href='/api/status'>/api/status</a></p>")
