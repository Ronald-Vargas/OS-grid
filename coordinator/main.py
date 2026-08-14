"""
Coordinador de OS Grid — versión estable (Fases 1-6 + configurador)
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

NUMERO_SECRETO = 8_450_137
OBJETIVO_HASH  = hash_de(NUMERO_SECRETO)

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


def construir_cola(tipo: str, intensidad: str) -> list:
    cfg = CONFIGURACIONES[tipo][intensidad]
    n = cfg["chunks"]
    cola = []
    if tipo in ("hash", "primos"):
        rango = cfg["rango"]
        tam_ch = rango // n
        for i in range(n):
            cola.append({"id": i, "tipo_tarea": tipo,
                         "inicio": i * tam_ch, "fin": (i + 1) * tam_ch, "tam": 0})
    elif tipo == "sort":
        tam = cfg["tam"]
        for i in range(n):
            cola.append({"id": i, "tipo_tarea": tipo,
                         "inicio": 0, "fin": 0, "tam": tam})
    return cola


class Estado:
    def __init__(self):
        self.tipo_tarea   = "hash"
        self.intensidad   = "normal"
        self.cola         = []
        self.lock         = asyncio.Lock()
        self.workers      = {}     # nombre -> {os, cpu, chunks_hechos, ws}
        self.resultados   = []
        self.encontrado   = None
        self.total_chunks = 0
        self.corrida_id   = None
        self.dashboards   = []
        # Estado global de la misión: "esperando" | "listo" | "corriendo" | "completa"
        self.fase         = "esperando"

    async def broadcast(self, evento: dict):
        muertos = []
        for d in self.dashboards:
            try:
                await d.send_text(json.dumps(evento))
            except Exception:
                muertos.append(d)
        for d in muertos:
            if d in self.dashboards:
                self.dashboards.remove(d)

    async def siguiente_chunk(self):
        async with self.lock:
            return self.cola.pop(0) if self.cola else None

    def reset(self):
        """Deja el estado listo para una nueva corrida (mantiene workers)."""
        self.cola         = []
        self.resultados   = []
        self.encontrado   = None
        self.total_chunks = 0
        self.corrida_id   = None
        self.fase         = "listo" if len(self.workers) >= MIN_WORKERS else "esperando"
        for w in self.workers.values():
            w["chunks_hechos"] = 0


estado = Estado()

app = FastAPI(title="OS Grid Coordinator")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


@app.get("/api/status")
def api_status():
    return {
        "fase": estado.fase,
        "tipo_tarea": estado.tipo_tarea,
        "intensidad": estado.intensidad,
        "chunks_totales": estado.total_chunks,
        "chunks_restantes": len(estado.cola),
        "workers": list(estado.workers.keys()),
    }


# ── WebSocket de workers ─────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_worker(websocket: WebSocket):
    await websocket.accept()
    nombre = None
    try:
        while True:
            crudo = await websocket.receive_text()
            data  = json.loads(crudo)
            tipo  = data.get("type")

            if tipo == P.REGISTER:
                nombre = data.get("nombre", "desconocido")
                estado.workers[nombre] = {
                    "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "chunks_hechos": 0,
                    "ws": websocket,
                }
                n = len(estado.workers)
                print(f"[COORD] Worker registrado: {nombre} ({data.get('os')}) — {n}/{MIN_WORKERS}")

                await websocket.send_text(json.dumps(
                    P.msg(P.REGISTERED, objetivo=OBJETIVO_HASH,
                          chunks_totales=estado.total_chunks)
                ))

                # Avisar al dashboard del nuevo worker
                await estado.broadcast({
                    "type": "worker_join",
                    "nombre": nombre, "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "chunks_totales": estado.total_chunks,
                })

                # Si estamos en fase "corriendo", darle chunks al vuelo
                if estado.fase == "corriendo":
                    await asignar(websocket, nombre)
                else:
                    # Actualizar la fase global según cuántos workers hay
                    if n >= MIN_WORKERS and estado.fase == "esperando":
                        estado.fase = "listo"
                    await avisar_fase()

            elif tipo == P.METRICS:
                await estado.broadcast({
                    "type": "metrics",
                    "nombre": data.get("nombre"),
                    "chunk_id": data.get("chunk_id"),
                    "cpu_proc": data.get("cpu_proc"),
                    "cpu_sys":  data.get("cpu_sys"),
                    "ram_mb":   data.get("ram_mb"),
                })

            elif tipo == P.RESULT:
                cid = data.get("chunk_id")
                hallazgo = data.get("encontrado")
                tiempo = data.get("tiempo", 0)

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
                    "nombre": nombre, "os": so, "chunk_id": cid,
                    "tiempo": tiempo, "encontrado": hallazgo,
                    "chunks_hechos": estado.workers[nombre]["chunks_hechos"],
                    "completados": len(estado.resultados),
                    "total": estado.total_chunks,
                })
                await asignar(websocket, nombre)

    except WebSocketDisconnect:
        if nombre and nombre in estado.workers:
            estado.workers.pop(nombre, None)
            print(f"[COORD] Worker desconectado: {nombre}")
            # Si estábamos listos pero se cayó uno, volver a esperando
            if estado.fase == "listo" and len(estado.workers) < MIN_WORKERS:
                estado.fase = "esperando"
            await avisar_fase()


# ── Asignación y flujo ───────────────────────────────────────────────────────

async def asignar(ws: WebSocket, nombre: str):
    chunk = await estado.siguiente_chunk()
    if chunk is None:
        try:
            await ws.send_text(json.dumps(P.msg(P.NO_MORE)))
        except Exception:
            pass
        # ¿Terminó todo?
        if estado.fase == "corriendo" and len(estado.resultados) == estado.total_chunks:
            estado.fase = "completa"
            await broadcast_final()
            # Reset inmediato para próxima corrida
            estado.reset()
            await avisar_fase()
        return

    print(f"[COORD] → {nombre}: chunk #{chunk['id']} tipo={chunk['tipo_tarea']}")
    await ws.send_text(json.dumps(P.msg(
        P.TASK_ASSIGN,
        chunk_id=chunk["id"], tipo_tarea=chunk["tipo_tarea"],
        inicio=chunk["inicio"], fin=chunk["fin"], tam=chunk["tam"],
        objetivo=OBJETIVO_HASH if chunk["tipo_tarea"] == "hash" else "",
    )))


async def avisar_fase():
    """Envía a los dashboards el estado actual (esperando / listo / corriendo)."""
    n = len(estado.workers)
    if estado.fase == "esperando":
        await estado.broadcast({
            "type": "waiting",
            "conectados": n, "minimo": MIN_WORKERS,
            "faltantes": max(0, MIN_WORKERS - n),
        })
    elif estado.fase == "listo":
        await estado.broadcast({
            "type": "ready_to_configure",
            "conectados": n, "minimo": MIN_WORKERS,
        })


async def broadcast_final():
    filas = stats.resumen_por_so(estado.corrida_id)
    por_so = [{"so": so or "?", "chunks": c, "tiempo_prom": t,
               "cpu_prom": cpu, "ram_prom": ram}
              for so, c, t, cpu, ram in filas]
    await estado.broadcast({
        "type": "mission_complete",
        "encontrado": estado.encontrado,
        "por_so": por_so,
        "corrida_id": estado.corrida_id,
    })
    print(f"[COORD] Misión #{estado.corrida_id} completa. Reset para siguiente corrida.")


# ── WebSocket del dashboard ──────────────────────────────────────────────────

@app.websocket("/ws/dashboard")
async def ws_dashboard(websocket: WebSocket):
    await websocket.accept()
    estado.dashboards.append(websocket)
    print(f"[COORD] Dashboard conectado ({len(estado.dashboards)} activos)")

    # Snapshot inicial
    await websocket.send_text(json.dumps({
        "type": "snapshot",
        "fase": estado.fase,
        "tipo_tarea": estado.tipo_tarea,
        "intensidad": estado.intensidad,
        "chunks_totales": estado.total_chunks,
        "completados": len(estado.resultados),
        "min_workers": MIN_WORKERS,
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
    """Arranca una misión: construye cola y asigna a todos los workers."""
    if estado.fase != "listo":
        print(f"[COORD] No se puede iniciar (fase={estado.fase})")
        return

    estado.tipo_tarea = tipo
    estado.intensidad = intensidad
    estado.cola = construir_cola(tipo, intensidad)
    estado.total_chunks = len(estado.cola)
    estado.corrida_id = stats.nueva_corrida(OBJETIVO_HASH, estado.total_chunks)
    estado.fase = "corriendo"

    print(f"[COORD] ▶ Misión iniciada: {tipo}/{intensidad} — {estado.total_chunks} chunks — corrida #{estado.corrida_id}")

    await estado.broadcast({
        "type": "mission_starting",
        "tipo": tipo, "intensidad": intensidad,
        "chunks_totales": estado.total_chunks,
    })

    # Repartir el primer chunk a cada worker
    for nombre, info in list(estado.workers.items()):
        try:
            await asignar(info["ws"], nombre)
        except Exception as e:
            print(f"[COORD] Error al asignar a {nombre}: {e}")


# ── Estáticos ────────────────────────────────────────────────────────────────

_static = Path(__file__).resolve().parent / "static"
if (_static / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(_static), html=True), name="static")
else:
    @app.get("/", response_class=HTMLResponse)
    def sin_dashboard():
        return "<h2>OS Grid</h2><p>Sin dashboard compilado.</p>"
