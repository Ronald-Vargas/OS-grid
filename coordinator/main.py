"""
Coordinador de OS Grid — versión estable (Fases 1-6 + configurador)

Reparto dinámico de chunks (work-stealing): cada worker arranca con un chunk y
pide el siguiente apenas termina, así el nodo más rápido procesa más y se ve
quién "gana".

Para que la misión SIEMPRE termine, el coordinador lleva contabilidad de los
chunks en vuelo (`asignados`) y los reencola si el worker se cae, si falla el
envío o si se cuelga (vigilante con timeout). Antes no existía esa contabilidad:
un solo chunk perdido dejaba la fase en "corriendo" para siempre y el sistema
quedaba trabado hasta reiniciar el coordinador.
"""

import asyncio
import json
import os
import sys
import time
import traceback
from contextlib import asynccontextmanager
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

# Configurables por variable de entorno (útil para pruebas locales).
MIN_WORKERS     = int(os.getenv("OSGRID_MIN_WORKERS", "3"))
TIMEOUT_CHUNK   = float(os.getenv("OSGRID_TIMEOUT_CHUNK", "120"))  # s sin respuesta => chunk perdido
INTERVALO_VIGIA = float(os.getenv("OSGRID_INTERVALO_VIGIA", "5"))  # s entre revisiones del vigilante
MAX_INTENTOS    = int(os.getenv("OSGRID_MAX_INTENTOS", "3"))       # reintentos antes de darlo por fallido


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
        self.workers      = {}     # nombre -> {os, cpu, chunks_hechos, sano, ws}
        self.resultados   = []
        self.encontrado   = None
        self.total_chunks = 0
        self.corrida_id   = None
        self.dashboards   = []
        # Chunks en vuelo: chunk_id -> {"chunk":…, "nombre":…, "desde": monotonic}
        self.asignados    = {}
        self.completados  = set()  # chunk_ids resueltos (evita contar duplicados)
        self.fallidos     = set()  # chunk_ids que agotaron los reintentos
        self.intentos     = {}     # chunk_id -> veces que se repartió
        self.reasignados  = 0      # para el informe: cuántas veces hubo que rescatar
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

    # ── contabilidad de chunks ───────────────────────────────────────────────

    async def tomar_chunk(self, nombre: str):
        """Saca el siguiente chunk pendiente y lo marca como en vuelo."""
        async with self.lock:
            while self.cola:
                ch = self.cola.pop(0)
                cid = ch["id"]
                if cid in self.completados or cid in self.asignados:
                    continue          # ya lo resolvió o lo está haciendo alguien
                self.intentos[cid] = self.intentos.get(cid, 0) + 1
                self.asignados[cid] = {"chunk": ch, "nombre": nombre,
                                       "desde": time.monotonic()}
                return ch
            return None

    async def reencolar(self, chunk_id: int, motivo: str) -> bool:
        """
        Devuelve un chunk perdido a la cola. Si ya agotó los reintentos lo marca
        como fallido para que la misión pueda cerrar igual en vez de trabarse.
        """
        async with self.lock:
            info = self.asignados.pop(chunk_id, None)
            if info is None or chunk_id in self.completados:
                return False
            if self.intentos.get(chunk_id, 0) >= MAX_INTENTOS:
                self.fallidos.add(chunk_id)
                print(f"[COORD] ✗ chunk #{chunk_id} descartado tras "
                      f"{MAX_INTENTOS} intentos ({motivo})")
                return False
            self.cola.insert(0, info["chunk"])   # prioridad: que salga ya
            self.reasignados += 1
            print(f"[COORD] ↻ chunk #{chunk_id} reencolado ({motivo}, "
                  f"estaba en {info['nombre']})")
            return True

    async def liberar_worker(self, nombre: str, motivo: str) -> list:
        """Reencola todos los chunks que tenía en vuelo un worker."""
        pendientes = [cid for cid, i in list(self.asignados.items())
                      if i["nombre"] == nombre]
        for cid in pendientes:
            await self.reencolar(cid, motivo)
        return pendientes

    def ocupados(self) -> set:
        return {i["nombre"] for i in self.asignados.values()}

    def mision_terminada(self) -> bool:
        return len(self.completados) + len(self.fallidos) >= self.total_chunks

    def reset(self):
        """Deja el estado listo para una nueva corrida (mantiene workers)."""
        self.cola         = []
        self.resultados   = []
        self.encontrado   = None
        self.total_chunks = 0
        self.corrida_id   = None
        self.asignados    = {}
        self.completados  = set()
        self.fallidos     = set()
        self.intentos     = {}
        self.reasignados  = 0
        self.fase         = "listo" if len(self.workers) >= MIN_WORKERS else "esperando"
        for w in self.workers.values():
            w["chunks_hechos"] = 0
            w["sano"] = True


estado = Estado()


@asynccontextmanager
async def lifespan(app: FastAPI):
    vigia = asyncio.create_task(vigilante())
    try:
        yield
    finally:
        vigia.cancel()


app = FastAPI(title="OS Grid Coordinator", lifespan=lifespan)
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
        "chunks_en_vuelo": {cid: i["nombre"] for cid, i in estado.asignados.items()},
        "completados": sorted(estado.completados),
        "fallidos": sorted(estado.fallidos),
        "workers": {n: {"os": w["os"], "sano": w["sano"],
                        "chunks_hechos": w["chunks_hechos"]}
                    for n, w in estado.workers.items()},
    }


@app.post("/api/abortar")
async def api_abortar():
    """Escotilla de emergencia: cierra la misión actual y vuelve a 'listo'."""
    if estado.fase != "corriendo":
        return {"ok": False, "fase": estado.fase}
    print("[COORD] Misión abortada manualmente.")
    await cerrar_mision()
    return {"ok": True, "fase": estado.fase}


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

                # Si ese nombre ya estaba con OTRA conexión, la vieja quedó
                # zombi: liberamos sus chunks antes de pisar el registro.
                previo = estado.workers.get(nombre)
                if previo is not None and previo["ws"] is not websocket:
                    print(f"[COORD] {nombre} se reconectó — descartando sesión anterior")
                    await estado.liberar_worker(nombre, "reconexión")

                estado.workers[nombre] = {
                    "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "chunks_hechos": previo["chunks_hechos"] if previo else 0,
                    "sano": True,
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
                await recibir_resultado(websocket, nombre, data)

    except WebSocketDisconnect:
        pass
    except Exception:
        # Una excepción suelta acá dejaba al worker como fantasma en el registro,
        # con un WebSocket muerto al que después se le asignaban chunks.
        print(f"[COORD] Error en la sesión de {nombre}:")
        traceback.print_exc()
    finally:
        await desconectar_worker(nombre, websocket)


async def recibir_resultado(websocket: WebSocket, nombre: str, data: dict):
    cid      = data.get("chunk_id")
    hallazgo = data.get("encontrado")
    tiempo   = data.get("tiempo", 0) or 0
    error    = data.get("error")

    worker = estado.workers.get(nombre)
    if worker is None:
        print(f"[COORD] RESULT de un worker no registrado ({nombre}) — ignorado")
        return

    async with estado.lock:
        estado.asignados.pop(cid, None)
        duplicado = cid in estado.completados
        if not duplicado:
            estado.completados.add(cid)
            estado.fallidos.discard(cid)

    if duplicado:
        # Llegó tarde un chunk que ya habíamos reasignado y resuelto.
        print(f"[COORD] chunk #{cid} duplicado (de {nombre}) — ignorado")
        await asignar(websocket, nombre)
        return

    estado.resultados.append(data)
    worker["chunks_hechos"] += 1
    worker["sano"] = True          # respondió: sale de cuarentena
    if hallazgo is not None:
        estado.encontrado = hallazgo

    so = worker["os"]
    stats.guardar_resultado(estado.corrida_id, nombre, so, data)

    marca = "  ← ¡ENCONTRADO!" if hallazgo else (f"  ← ERROR: {error}" if error else "")
    print(f"[COORD] {nombre} terminó chunk #{cid} en {tiempo:.3f}s{marca}")

    await estado.broadcast({
        "type": "chunk_done",
        "nombre": nombre, "os": so, "chunk_id": cid,
        "tiempo": tiempo, "encontrado": hallazgo, "error": error,
        "chunks_hechos": worker["chunks_hechos"],
        "completados": len(estado.completados),
        "total": estado.total_chunks,
    })
    await asignar(websocket, nombre)


async def desconectar_worker(nombre: str, websocket: WebSocket):
    """Limpia el registro solo si el ws guardado es realmente el que se cayó."""
    if not nombre:
        return
    actual = estado.workers.get(nombre)
    if actual is None or actual["ws"] is not websocket:
        return   # ya se reconectó con otra sesión: no tocar el registro nuevo

    estado.workers.pop(nombre, None)
    print(f"[COORD] Worker desconectado: {nombre}")

    # Sus chunks en vuelo vuelven a la cola y se reparten entre los que quedan.
    perdidos = await estado.liberar_worker(nombre, "desconexión")
    await estado.broadcast({"type": "worker_leave", "nombre": nombre,
                            "chunks_devueltos": perdidos})

    if estado.fase == "corriendo":
        await repartir_pendientes()
        await revisar_fin_de_mision()
    elif estado.fase == "listo" and len(estado.workers) < MIN_WORKERS:
        estado.fase = "esperando"
    await avisar_fase()


# ── Asignación y flujo ───────────────────────────────────────────────────────

async def asignar(ws: WebSocket, nombre: str):
    chunk = await estado.tomar_chunk(nombre)
    if chunk is None:
        try:
            await ws.send_text(json.dumps(P.msg(P.NO_MORE)))
        except Exception:
            pass
        await revisar_fin_de_mision()
        return

    try:
        await ws.send_text(json.dumps(P.msg(
            P.TASK_ASSIGN,
            chunk_id=chunk["id"], tipo_tarea=chunk["tipo_tarea"],
            inicio=chunk["inicio"], fin=chunk["fin"], tam=chunk["tam"],
            objetivo=OBJETIVO_HASH if chunk["tipo_tarea"] == "hash" else "",
        )))
    except Exception as e:
        # Antes el chunk ya estaba fuera de la cola y se perdía en silencio.
        print(f"[COORD] Falló el envío del chunk #{chunk['id']} a {nombre}: {e}")
        await estado.reencolar(chunk["id"], "envío fallido")
        return

    print(f"[COORD] → {nombre}: chunk #{chunk['id']} tipo={chunk['tipo_tarea']}")


async def repartir_pendientes():
    """Le da trabajo a todo worker sano que esté libre y quede cola."""
    if estado.fase != "corriendo":
        return
    ocupados = estado.ocupados()
    for nombre, info in list(estado.workers.items()):
        if not estado.cola:
            break
        if nombre in ocupados or not info["sano"]:
            continue
        await asignar(info["ws"], nombre)


async def revisar_fin_de_mision():
    if estado.fase == "corriendo" and estado.mision_terminada():
        await cerrar_mision()


async def cerrar_mision():
    estado.fase = "completa"
    await broadcast_final()
    estado.reset()          # listo para la siguiente corrida
    await avisar_fase()


# ── Vigilante: rescata chunks de workers colgados ────────────────────────────

async def vigilante():
    """
    Revisa periódicamente los chunks en vuelo. Si un worker no responde en
    TIMEOUT_CHUNK segundos se asume colgado: su chunk vuelve a la cola y el
    worker queda en cuarentena hasta que dé señales de vida.
    """
    while True:
        try:
            await asyncio.sleep(INTERVALO_VIGIA)
            if estado.fase != "corriendo":
                continue

            ahora = time.monotonic()
            vencidos = [(cid, i["nombre"]) for cid, i in list(estado.asignados.items())
                        if ahora - i["desde"] > TIMEOUT_CHUNK]

            for cid, quien in vencidos:
                print(f"[COORD] ⏱ chunk #{cid} sin respuesta de {quien} "
                      f"tras {TIMEOUT_CHUNK}s")
                if quien in estado.workers:
                    estado.workers[quien]["sano"] = False
                await estado.reencolar(cid, "timeout")
                await estado.broadcast({"type": "chunk_timeout",
                                        "chunk_id": cid, "nombre": quien})

            # Antitrabas: hay cola, nadie procesando y ningún worker sano libre.
            if estado.cola and not estado.asignados:
                if not any(w["sano"] for w in estado.workers.values()):
                    print("[COORD] Ningún worker sano — se levanta la cuarentena")
                    for w in estado.workers.values():
                        w["sano"] = True

            if vencidos or estado.cola:
                await repartir_pendientes()
            await revisar_fin_de_mision()

        except asyncio.CancelledError:
            raise
        except Exception:
            print("[COORD] Error en el vigilante:")
            traceback.print_exc()


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
        "completados": len(estado.completados),
        "total": estado.total_chunks,
        "fallidos": sorted(estado.fallidos),
        "reasignados": estado.reasignados,
    })
    extra = ""
    if estado.fallidos:
        extra += f" — chunks sin resolver: {sorted(estado.fallidos)}"
    if estado.reasignados:
        extra += f" — {estado.reasignados} reasignación(es)"
    print(f"[COORD] Misión #{estado.corrida_id} completa "
          f"({len(estado.completados)}/{estado.total_chunks}){extra}. "
          f"Reset para siguiente corrida.")


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
        "completados": len(estado.completados),
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
            elif data.get("type") == "abort_mission":
                if estado.fase == "corriendo":
                    print("[COORD] Misión abortada desde el dashboard.")
                    await cerrar_mision()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in estado.dashboards:
            estado.dashboards.remove(websocket)
        print(f"[COORD] Dashboard desconectado ({len(estado.dashboards)} activos)")


async def iniciar_mision(tipo: str, intensidad: str):
    """Arranca una misión: construye cola y asigna a todos los workers."""
    if estado.fase != "listo":
        print(f"[COORD] No se puede iniciar (fase={estado.fase})")
        return

    estado.reset()
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
    await repartir_pendientes()


# ── Estáticos ────────────────────────────────────────────────────────────────

_static = Path(__file__).resolve().parent / "static"
if (_static / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(_static), html=True), name="static")
else:
    @app.get("/", response_class=HTMLResponse)
    def sin_dashboard():
        return "<h2>OS Grid</h2><p>Sin dashboard compilado.</p>"
