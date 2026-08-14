"""
Worker de OS Grid.

Se conecta al coordinador, se registra y procesa chunks hasta que no haya más.
Cuando la misión termina (NO_MORE), espera 3 segundos y reconecta automáticamente
para participar en la siguiente corrida sin que el usuario tenga que hacer nada.

Uso:
    python agent.py <host> <nombre_nodo>

Ejemplos:
    python agent.py localhost     NodoFedora
    python agent.py 100.70.74.9  NodoMac
"""

import asyncio
import json
import platform
import sys
import time

import psutil
import websockets

RAIZ = __file__.rsplit("/", 2)[0]
sys.path.insert(0, RAIZ)

import protocol as P
from worker.metrics import Muestreador
from worker.tasks import procesar_chunk

HOST   = sys.argv[1] if len(sys.argv) > 1 else "localhost"
NOMBRE = sys.argv[2] if len(sys.argv) > 2 else platform.node()
PUERTO = 8000


def detectar_os() -> str:
    s = platform.system()
    return {"Darwin": "macOS", "Windows": "Windows"}.get(s, "Linux")


async def correr_una_vez():
    """Ejecuta una misión completa. Devuelve cuando recibe NO_MORE."""
    uri = f"ws://{HOST}:{PUERTO}/ws"
    async with websockets.connect(uri, ping_interval=20) as ws:
        # ── Registro ──────────────────────────────────────────────────────
        mem = psutil.virtual_memory()
        cpu_count = psutil.cpu_count(logical=True) or 1
        await ws.send(json.dumps(P.msg(
            P.REGISTER,
            nombre=NOMBRE,
            os=detectar_os(),
            cpu=cpu_count,
            ram_total_mb=round(mem.total / 1024 / 1024),
        )))
        print(f"[{NOMBRE}] Registrado en {HOST}:{PUERTO}. Esperando inicio de misión...")

        # ── Bucle principal ───────────────────────────────────────────────
        async for crudo in ws:
            data   = json.loads(crudo)
            tipo   = data.get("type")

            if tipo == P.REGISTERED:
                print(f"[{NOMBRE}] Confirmado. En espera de tarea...")

            elif tipo == P.TASK_ASSIGN:
                chunk_id    = data["chunk_id"]
                tipo_tarea  = data.get("tipo_tarea", "hash")
                inicio      = data.get("inicio", 0)
                fin         = data.get("fin", 0)
                objetivo    = data.get("objetivo", "")
                tam         = data.get("tam", 0)

                print(f"[{NOMBRE}] Chunk #{chunk_id} | tipo={tipo_tarea} | "
                      f"rango=[{inicio},{fin}) | tam={tam}")

                # Muestrear métricas mientras se procesa el chunk
                muestreador = Muestreador(ws, NOMBRE, chunk_id)
                tarea = asyncio.get_event_loop().run_in_executor(
                    None,
                    procesar_chunk,
                    tipo_tarea, chunk_id, inicio, fin, objetivo, tam,
                )
                await asyncio.gather(
                    tarea,
                    muestreador.iniciar(),
                )
                resultado = tarea.result()
                muestreador.detener()

                hallazgo = resultado.get("encontrado")
                tiempo   = resultado.get("tiempo", 0)
                extras   = resultado.get("extras", {})

                if hallazgo is not None:
                    print(f"[{NOMBRE}] ¡ENCONTRADO! {hallazgo} (chunk #{chunk_id})")

                await ws.send(json.dumps(P.msg(
                    P.RESULT,
                    nombre=NOMBRE,
                    chunk_id=chunk_id,
                    encontrado=hallazgo,
                    tiempo=tiempo,
                    **extras,
                )))

            elif tipo == P.NO_MORE:
                print(f"[{NOMBRE}] Misión completa. Esperando próxima corrida...")
                return  # Salir del bucle → el outer loop reconecta


async def main():
    """Bucle externo: reconecta automáticamente después de cada misión."""
    while True:
        try:
            await correr_una_vez()
        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionRefusedError, OSError) as e:
            print(f"[{NOMBRE}] Conexión cerrada ({e}). Reintentando en 3s...")
        except Exception as e:
            print(f"[{NOMBRE}] Error inesperado: {e}. Reintentando en 3s...")
        await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
