"""Worker de OS Grid — versión estable.

El MISMO archivo corre en Fedora, macOS y Windows. Mantiene una sola conexión
WebSocket abierta durante varias misiones y reconecta solo si el TCP se cae.

Uso:
    python worker/agent.py                       # localhost
    python worker/agent.py 100.x.x.x             # IP del coordinador
    python worker/agent.py 100.x.x.x NodoWindows # además fija el nombre del nodo
"""
import asyncio
import json
import os
import platform
import sys
import time
import traceback

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

ESPERA_RECONEXION = 3      # segundos entre reintentos
TIMEOUT_METRICAS  = 5      # margen para que el muestreador cierre solo


def detectar_os() -> str:
    s = platform.system()
    return {"Darwin": "macOS", "Windows": "Windows"}.get(s, "Linux")


async def procesar_con_metricas(ws, tipo_tarea, chunk_id, inicio, fin, objetivo, tam):
    """
    Corre el chunk en un hilo (para no bloquear el event loop) mientras el
    Muestreador manda métricas en vivo.

    CLAVE: el muestreador se detiene en un `finally`. Si el cómputo revienta,
    antes el muestreador quedaba girando para siempre y el worker se colgaba
    silenciosamente: seguía conectado, mandando métricas con CPU 0%, sin
    devolver nunca el RESULT. El chunk se perdía y la misión no terminaba.
    """
    muestreador = Muestreador(ws, NOMBRE, chunk_id)
    tarea_metricas = asyncio.create_task(muestreador.iniciar())

    t0 = time.perf_counter()
    try:
        loop = asyncio.get_running_loop()
        resultado = await loop.run_in_executor(
            None, procesar_chunk,
            tipo_tarea, chunk_id, inicio, fin, objetivo, tam
        )
    except Exception as e:
        # No dejamos morir al worker: reportamos el chunk como fallido y seguimos.
        print(f"[{NOMBRE}] ERROR procesando chunk #{chunk_id}: {type(e).__name__}: {e}")
        traceback.print_exc()
        resultado = {
            "chunk_id": chunk_id,
            "tiempo": time.perf_counter() - t0,
            "encontrado": None,
            "extras": {"error": f"{type(e).__name__}: {e}"},
        }
    finally:
        muestreador.detener()
        try:
            await asyncio.wait_for(tarea_metricas, timeout=TIMEOUT_METRICAS)
        except Exception:
            tarea_metricas.cancel()

    # Garantizar al menos una muestra: los chunks que duran menos que el
    # intervalo de muestreo reportaban CPU/RAM en 0 y ensuciaban la comparación.
    if muestreador.muestras == 0:
        await muestreador.enviar_muestra()

    extras = dict(resultado.get("extras") or {})
    extras.update(muestreador.picos())
    resultado["extras"] = extras
    return resultado


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
            try:
                data = json.loads(crudo)
            except json.JSONDecodeError:
                print(f"[{NOMBRE}] Mensaje ilegible, ignorado.")
                continue
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

                # Pase lo que pase, el coordinador recibe una respuesta por chunk.
                await ws.send(json.dumps(P.msg(
                    P.RESULT, nombre=NOMBRE, chunk_id=chunk_id,
                    encontrado=hallazgo, tiempo=tiempo, **extras,
                )))
                print(f"[{NOMBRE}] Chunk #{chunk_id} listo en {tiempo:.3f}s")

            elif tipo == P.NO_MORE:
                # Misión terminada. NO cerramos la conexión — esperamos
                # que llegue el próximo TASK_ASSIGN cuando se inicie otra corrida.
                print(f"[{NOMBRE}] Misión completa. Listo para la próxima.")


async def main():
    """Reconecta solo si la conexión TCP se cae."""
    while True:
        try:
            await sesion()
            print(f"[{NOMBRE}] El coordinador cerró la conexión. "
                  f"Reintentando en {ESPERA_RECONEXION}s...")
        except asyncio.CancelledError:
            raise
        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            print(f"[{NOMBRE}] Conexión perdida ({type(e).__name__}). "
                  f"Reintentando en {ESPERA_RECONEXION}s...")
        except Exception as e:
            print(f"[{NOMBRE}] Error inesperado: {type(e).__name__}: {e}. "
                  f"Reintentando en {ESPERA_RECONEXION}s...")
            traceback.print_exc()
        await asyncio.sleep(ESPERA_RECONEXION)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{NOMBRE}] Detenido por el usuario.")
