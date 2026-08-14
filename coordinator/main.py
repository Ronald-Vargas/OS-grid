"""
Coordinador de OS Grid  (Fases 1-2)

Servidor central del clúster. Sus responsabilidades:

  1. Aceptar conexiones WebSocket de los workers.
  2. Registrarlos cuando se presentan (REGISTER).
  3. Repartir chunks de la cola bajo demanda: cada vez que un worker queda
     libre, pide y recibe el siguiente chunk disponible (asignación dinámica,
     no repartida de antemano; así el nodo más rápido procesa más chunks).
  4. Recibir los resultados (RESULT) y, cuando la cola se vacía, avisar el
     final (NO_MORE) y mostrar el resumen.

Cómo correrlo (desde la carpeta os-grid, con el venv activo):

    uvicorn coordinator.main:app --host 0.0.0.0 --port 8000

El host 0.0.0.0 hace que escuche en todas las interfaces: en Fase 2 te
conectás por localhost, y más adelante los workers remotos usarán la IP
del equipo (o la de Tailscale) sin cambiar nada del servidor.
"""

import asyncio
import json
import sys
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Permite importar protocol.py y worker/tasks.py desde la raíz del proyecto.
RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

import protocol as P  # noqa: E402
from coordinator import stats  # noqa: E402

# ------------------------------------------------------------------
# Configuración de la tarea global a distribuir.
# ------------------------------------------------------------------
# Buscamos el número cuyo SHA-256 coincide con OBJETIVO. Lo fijamos a un
# número conocido para que la corrida sea reproducible en la demo.
NUMERO_SECRETO = 8_450_137
from worker.tasks import hash_de  # noqa: E402

OBJETIVO = hash_de(NUMERO_SECRETO)

RANGO_TOTAL = 12_000_000  # revisamos del 0 al 12.000.000
NUM_CHUNKS = 12           # partido en 12 pedazos (cada uno tarda ~1-3s,
                          # suficiente para capturar varias lecturas de métricas)
TAM_CHUNK = RANGO_TOTAL // NUM_CHUNKS

# Cuántos workers deben conectarse antes de que empiece la misión.
# Cambialo a 1 si querés probar en solitario.
MIN_WORKERS = 3


def construir_cola():
    """Genera la lista de chunks como rangos (inicio, fin)."""
    cola = []
    for i in range(NUM_CHUNKS):
        inicio = i * TAM_CHUNK
        fin = inicio + TAM_CHUNK
        cola.append({"id": i, "inicio": inicio, "fin": fin})
    return cola


app = FastAPI(title="OS Grid Coordinator")

# Permitir que el dashboard Angular (servido en otro puerto durante el
# desarrollo, p. ej. localhost:4200) se conecte al coordinador sin bloqueos.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class Estado:
    """Estado compartido del clúster durante una corrida."""
    def __init__(self):
        self.cola = construir_cola()
        self.lock = asyncio.Lock()      # protege la cola entre workers
        self.workers = {}               # nombre -> info
        self.resultados = []            # RESULTs recibidos
        self.encontrado = None          # número hallado, si aparece
        self.total_chunks = len(self.cola)
        self.corrida_id = None          # id de la corrida en la base de datos
        self.dashboards = []            # WebSockets de dashboards conectados
        # Evento que se dispara cuando llegaron suficientes workers.
        self.arranque = asyncio.Event()

    async def broadcast(self, evento: dict):
        """
        Reenvía un evento a todos los dashboards conectados. Si alguno se
        desconectó, lo quitamos de la lista silenciosamente.
        """
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
        """Entrega el próximo chunk de la cola, o None si ya no hay."""
        async with self.lock:
            if self.cola:
                return self.cola.pop(0)
            return None


estado = Estado()


@app.get("/api/status")
def estado_api():
    return {
        "servicio": "OS Grid Coordinator",
        "objetivo": OBJETIVO,
        "chunks_totales": estado.total_chunks,
        "chunks_restantes": len(estado.cola),
        "workers": list(estado.workers.keys()),
    }


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    nombre = None
    try:
        while True:
            crudo = await websocket.receive_text()
            data = json.loads(crudo)
            tipo = data.get("type")

            # --- REGISTER: el worker se presenta ---
            if tipo == P.REGISTER:
                nombre = data.get("nombre", "desconocido")
                estado.workers[nombre] = {
                    "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "chunks_hechos": 0,
                }
                # La primera vez que se conecta un worker, abrimos la corrida
                # en la base de datos.
                if estado.corrida_id is None:
                    estado.corrida_id = stats.nueva_corrida(OBJETIVO, estado.total_chunks)
                    print(f"[COORD] Corrida #{estado.corrida_id} registrada en la base de datos")
                print(f"[COORD] Worker registrado: {nombre} "
                      f"({data.get('os')}, {data.get('cpu')} núcleos)")
                await websocket.send_text(json.dumps(
                    P.msg(P.REGISTERED,
                          objetivo=OBJETIVO,
                          chunks_totales=estado.total_chunks)
                ))
                # Avisar a los dashboards que se sumó un worker.
                await estado.broadcast({
                    "type": "worker_join",
                    "nombre": nombre,
                    "os": data.get("os"),
                    "cpu": data.get("cpu"),
                    "ram_total_mb": data.get("ram_total_mb"),
                    "chunks_totales": estado.total_chunks,
                })
                # Esperar a que se conecten todos los workers antes de arrancar.
                n = len(estado.workers)
                faltantes = MIN_WORKERS - n
                print(f"[COORD] Workers conectados: {n}/{MIN_WORKERS}")
                await estado.broadcast({
                    "type": "waiting",
                    "conectados": n,
                    "minimo": MIN_WORKERS,
                    "faltantes": faltantes,
                })
                if n >= MIN_WORKERS and not estado.arranque.is_set():
                    print("[COORD] ¡Todos los workers listos! Arrancando misión...")
                    estado.arranque.set()
                # Esperar el disparo (si ya está seteado, pasa inmediatamente)
                await estado.arranque.wait()
                await asignar(websocket, nombre)

            # --- METRICS: lectura de CPU/RAM en vivo mientras procesa ---
            elif tipo == P.METRICS:
                print(f"[METRICS] {data.get('nombre')} chunk#{data.get('chunk_id')} "
                      f"| CPU proc {data.get('cpu_proc')}% "
                      f"| CPU sys {data.get('cpu_sys')}% "
                      f"| RAM {data.get('ram_mb')} MB")
                # Reenviar la métrica en vivo a los dashboards.
                await estado.broadcast({
                    "type": "metrics",
                    "nombre": data.get("nombre"),
                    "chunk_id": data.get("chunk_id"),
                    "cpu_proc": data.get("cpu_proc"),
                    "cpu_sys": data.get("cpu_sys"),
                    "ram_mb": data.get("ram_mb"),
                })

            # --- RESULT: el worker terminó un chunk ---
            elif tipo == P.RESULT:
                cid = data.get("chunk_id")
                hallazgo = data.get("encontrado")
                tiempo = data.get("tiempo", 0)
                estado.resultados.append(data)
                estado.workers[nombre]["chunks_hechos"] += 1
                marca = "  <-- ¡NÚMERO ENCONTRADO!" if hallazgo is not None else ""
                print(f"[COORD] {nombre} terminó chunk #{cid} "
                      f"en {tiempo:.3f}s{marca}")
                if hallazgo is not None:
                    estado.encontrado = hallazgo
                # Guardamos el resultado de este chunk en la base de datos.
                so_worker = estado.workers[nombre]["os"]
                stats.guardar_resultado(estado.corrida_id, nombre, so_worker, data)
                # Avisar a los dashboards del chunk completado y el progreso.
                await estado.broadcast({
                    "type": "chunk_done",
                    "nombre": nombre,
                    "os": so_worker,
                    "chunk_id": cid,
                    "tiempo": tiempo,
                    "encontrado": hallazgo,
                    "chunks_hechos": estado.workers[nombre]["chunks_hechos"],
                    "completados": len(estado.resultados),
                    "total": estado.total_chunks,
                })
                # Le damos el siguiente chunk (o le avisamos que no hay más).
                await asignar(websocket, nombre)

    except WebSocketDisconnect:
        if nombre:
            print(f"[COORD] Worker desconectado: {nombre}")


async def asignar(websocket: WebSocket, nombre: str):
    """Entrega el siguiente chunk al worker, o le avisa que la cola terminó."""
    chunk = await estado.siguiente_chunk()
    if chunk is None:
        await websocket.send_text(json.dumps(P.msg(P.NO_MORE)))
        # ¿Terminaron todos los chunks? Mostramos el resumen una sola vez.
        if len(estado.resultados) == estado.total_chunks:
            resumen()
            await broadcast_final()
        return
    print(f"[COORD] -> {nombre}: asignando chunk #{chunk['id']} "
          f"[{chunk['inicio']}, {chunk['fin']})")
    await websocket.send_text(json.dumps(
        P.msg(P.TASK_ASSIGN,
              chunk_id=chunk["id"],
              inicio=chunk["inicio"],
              fin=chunk["fin"],
              objetivo=OBJETIVO)
    ))


async def broadcast_final():
    """Envía a los dashboards el leaderboard final por SO y por nodo."""
    filas = stats.resumen_por_so(estado.corrida_id)
    por_so = [
        {"so": so or "?", "chunks": chunks, "tiempo_prom": t_prom,
         "cpu_prom": cpu_prom, "ram_prom": ram_prom}
        for so, chunks, t_prom, cpu_prom, ram_prom in filas
    ]
    por_nodo = [
        {"nombre": n, "os": info["os"], "chunks": info["chunks_hechos"]}
        for n, info in estado.workers.items()
    ]
    await estado.broadcast({
        "type": "mission_complete",
        "encontrado": estado.encontrado,
        "por_so": por_so,
        "por_nodo": por_nodo,
        "corrida_id": estado.corrida_id,
    })


def resumen():
    print("\n" + "=" * 52)
    print(" MISIÓN COMPLETA")
    print("=" * 52)
    for nombre, info in estado.workers.items():
        print(f"  {nombre:<20} {info['chunks_hechos']} chunks")
    if estado.encontrado is not None:
        print(f"\n  Número secreto encontrado: {estado.encontrado}")

    # Resumen agregado por sistema operativo, leído desde la base de datos.
    filas = stats.resumen_por_so(estado.corrida_id)
    if filas:
        print("\n  Comparativa por sistema operativo:")
        print(f"  {'SO':<10}{'chunks':>8}{'t.prom(s)':>12}{'CPU proc%':>12}{'RAM MB':>10}")
        print("  " + "-" * 50)
        for so, chunks, t_prom, cpu_prom, ram_prom in filas:
            so_txt = so or "?"
            print(f"  {so_txt:<10}{chunks:>8}{t_prom:>12}{cpu_prom:>12}{ram_prom:>10}")
    print(f"\n  Datos guardados en: os_grid.db (corrida #{estado.corrida_id})")
    print("=" * 52 + "\n")


@app.websocket("/ws/dashboard")
async def ws_dashboard(websocket: WebSocket):
    """
    Canal para los dashboards. No manda trabajo: solo recibe los eventos
    que el coordinador retransmite (worker_join, metrics, chunk_done,
    mission_complete). Al conectarse, le enviamos el estado actual para que
    un dashboard que llega tarde vea los workers ya presentes.
    """
    await websocket.accept()
    estado.dashboards.append(websocket)
    print(f"[COORD] Dashboard conectado ({len(estado.dashboards)} activos)")

    # Enviar snapshot inicial del estado.
    try:
        await websocket.send_text(json.dumps({
            "type": "snapshot",
            "objetivo": OBJETIVO,
            "chunks_totales": estado.total_chunks,
            "completados": len(estado.resultados),
            "min_workers": MIN_WORKERS,
            "arrancado": estado.arranque.is_set(),
            "workers": [
                {"nombre": n, "os": info["os"], "cpu": info["cpu"],
                 "chunks_hechos": info["chunks_hechos"]}
                for n, info in estado.workers.items()
            ],
        }))
        # Mantener la conexión viva. El dashboard no envía nada relevante,
        # pero leemos para detectar cuando se desconecta.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in estado.dashboards:
            estado.dashboards.remove(websocket)
        print(f"[COORD] Dashboard desconectado ({len(estado.dashboards)} activos)")


# Servir el dashboard Angular ya compilado (cuando exista la carpeta static).
# Durante el desarrollo se usa `ng serve` aparte; en producción se copia el
# build de Angular a coordinator/static/ y se sirve desde aquí.
_static = Path(__file__).resolve().parent / "static"
if (_static / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(_static), html=True), name="static")
else:
    # Sin dashboard compilado: la raíz muestra un aviso simple.
    from fastapi.responses import HTMLResponse

    @app.get("/", response_class=HTMLResponse)
    def sin_dashboard():
        return (
            "<h2>OS Grid Coordinator</h2>"
            "<p>El coordinador está corriendo. El dashboard aún no está "
            "compilado en <code>coordinator/static/</code>.</p>"
            "<p>Estado del clúster: <a href='/api/status'>/api/status</a></p>"
        )
