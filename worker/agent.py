"""Worker de OS Grid — versión estable."""
import asyncio
import json
import os
import platform
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import psutil
import websockets

import protocol as P
from worker.metrics import Muestreador
from worker.tasks import procesar_chunk

HOST   = sys.argv[1] if len(sys.argv) > 1 else "localhost"
NOMBRE = sys.argv[2] if len(sys.argv) > 2 else platform.node()
PUERTO = 8000


def detectar_os() -> str:
    s = platform.system()
    return {"Darwin": "macOS", "Windows": "Windows"}.get(s, "Linux")


async def procesar_con_metricas(ws, tipo_tarea, chunk_id, inicio, fin, objetivo, tam):
    muestreador = Muestreador(ws, NOMBRE, chunk_id)

    async def computar_y_detener():
        loop = asyncio.get_event_loop()
        resultado = await loop.run_in_executor(
            None, procesar_chunk,
            tipo_tarea, chunk_id, inicio, fin, objetivo, tam
        )
        muestreador.detener()
        return resultado

    resultados = await asyncio.gather(
        computar_y_detener(),
        muestreador.iniciar(),
        return_exceptions=True
    )
    return resultados[0]


async def sesion():
    """Una conexión: se mantiene abierta durante MÚLTIPLES misiones."""
    uri = f"ws://{HOST}:{PUERTO}/ws"
    async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
        mem = psutil.virtual_memory()
        await ws.send(json.dumps(P.msg(
            P.REGISTER,
            nombre=NOMBRE, os=detectar_os(),
            cpu=psutil.cpu_count(logical=True) or 1,
            ram_total_mb=round(mem.total / 1024 / 1024),
        )))
        print(f"[{NOMBRE}] Registrado en {HOST}:{PUERTO}")

        async for crudo in ws:
            data = json.loads(crudo)
            tipo = data.get("type")

            if tipo == P.REGISTERED:
                print(f"[{NOMBRE}] En espera de tarea...")

            elif tipo == P.TASK_ASSIGN:
                chunk_id   = data["chunk_id"]
                tipo_tarea = data.get("tipo_tarea", "hash")
                inicio     = data.get("inicio", 0)
                fin        = data.get("fin", 0)
                objetivo   = data.get("objetivo", "")
                tam        = data.get("tam", 0)
                print(f"[{NOMBRE}] Chunk #{chunk_id} | {tipo_tarea}")

                resultado = await procesar_con_metricas(
                    ws, tipo_tarea, chunk_id, inicio, fin, objetivo, tam
                )
                hallazgo = resultado.get("encontrado")
                tiempo   = resultado.get("tiempo", 0)
                extras   = resultado.get("extras", {})

                if hallazgo is not None:
                    print(f"[{NOMBRE}] ¡ENCONTRADO! {hallazgo}")

                await ws.send(json.dumps(P.msg(
                    P.RESULT, nombre=NOMBRE, chunk_id=chunk_id,
                    encontrado=hallazgo, tiempo=tiempo, **extras,
                )))

            elif tipo == P.NO_MORE:
                # Misión terminada. NO cerramos la conexión — esperamos
                # que llegue el próximo TASK_ASSIGN cuando se inicie otra corrida.
                print(f"[{NOMBRE}] Misión completa. Listo para la próxima.")


async def main():
    """Reconecta solo si la conexión TCP se cae."""
    while True:
        try:
            await sesion()
        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            print(f"[{NOMBRE}] Conexión perdida ({type(e).__name__}). Reintentando en 3s...")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[{NOMBRE}] Error inesperado: {e}. Reintentando en 3s...")
        await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{NOMBRE}] Detenido por el usuario.")
