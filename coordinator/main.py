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

# Permite importar protocol.py y worker/tasks.py desde la raíz del proyecto.
RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

import protocol as P  # noqa: E402

# ------------------------------------------------------------------
# Configuración de la tarea global a distribuir.
# ------------------------------------------------------------------
# Buscamos el número cuyo SHA-256 coincide con OBJETIVO. Lo fijamos a un
# número conocido para que la corrida sea reproducible en la demo.
NUMERO_SECRETO = 733_219
from worker.tasks import hash_de  # noqa: E402

OBJETIVO = hash_de(NUMERO_SECRETO)

RANGO_TOTAL = 1_000_000   # revisamos del 0 al 1.000.000
NUM_CHUNKS = 20           # partido en 20 pedazos
TAM_CHUNK = RANGO_TOTAL // NUM_CHUNKS


def construir_cola():
    """Genera la lista de chunks como rangos (inicio, fin)."""
    cola = []
    for i in range(NUM_CHUNKS):
        inicio = i * TAM_CHUNK
        fin = inicio + TAM_CHUNK
        cola.append({"id": i, "inicio": inicio, "fin": fin})
    return cola


app = FastAPI(title="OS Grid Coordinator")


class Estado:
    """Estado compartido del clúster durante una corrida."""
    def __init__(self):
        self.cola = construir_cola()
        self.lock = asyncio.Lock()      # protege la cola entre workers
        self.workers = {}               # nombre -> info
        self.resultados = []            # RESULTs recibidos
        self.encontrado = None          # número hallado, si aparece
        self.total_chunks = len(self.cola)

    async def siguiente_chunk(self):
        """Entrega el próximo chunk de la cola, o None si ya no hay."""
        async with self.lock:
            if self.cola:
                return self.cola.pop(0)
            return None


estado = Estado()


@app.get("/")
def raiz():
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
                print(f"[COORD] Worker registrado: {nombre} "
                      f"({data.get('os')}, {data.get('cpu')} núcleos)")
                await websocket.send_text(json.dumps(
                    P.msg(P.REGISTERED,
                          objetivo=OBJETIVO,
                          chunks_totales=estado.total_chunks)
                ))
                # Le mandamos su primer chunk de inmediato.
                await asignar(websocket, nombre)

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


def resumen():
    print("\n" + "=" * 52)
    print(" MISIÓN COMPLETA")
    print("=" * 52)
    for nombre, info in estado.workers.items():
        print(f"  {nombre:<20} {info['chunks_hechos']} chunks")
    if estado.encontrado is not None:
        print(f"\n  Número secreto encontrado: {estado.encontrado}")
    print("=" * 52 + "\n")
