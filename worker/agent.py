"""
Agente worker de OS Grid  (Fases 1-2)

Este MISMO archivo corre sin cambios en Fedora, macOS y Windows. Es la pieza
que demuestra la portabilidad del sistema: solo necesita Python y las
librerías 'websockets' y 'psutil'.

Ciclo de vida:
  1. Se conecta al coordinador por WebSocket.
  2. Se presenta (REGISTER) diciendo su SO y número de núcleos.
  3. Recibe chunks (TASK_ASSIGN), los procesa y reporta (RESULT).
  4. Repite hasta que el coordinador avisa NO_MORE.

Uso:
    python agent.py                        # se conecta a localhost (Fase 2)
    python agent.py 100.x.x.x              # IP remota (Fase 5, Tailscale)
    python agent.py 100.x.x.x MiNombre     # además fija un nombre de nodo

El nombre por defecto sale del hostname del equipo, así en la demo cada
nodo aparece con su propio nombre sin configurar nada.
"""

import asyncio
import json
import platform
import socket
import sys
import time

import psutil
import websockets

# --- protocolo compartido: agrega la raíz del proyecto al path ---
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import protocol as P  # noqa: E402

from tasks import procesar_chunk  # noqa: E402


def nombre_so() -> str:
    """Nombre legible del sistema operativo: Linux / Darwin(macOS) / Windows."""
    s = platform.system()
    return {"Darwin": "macOS"}.get(s, s)


async def correr(host: str, puerto: int, nombre: str):
    uri = f"ws://{host}:{puerto}/ws"
    print(f"[{nombre}] Conectando a {uri} ...")

    async with websockets.connect(uri) as ws:
        # 1. Registro
        await ws.send(json.dumps(P.msg(
            P.REGISTER,
            nombre=nombre,
            os=nombre_so(),
            cpu=psutil.cpu_count(logical=True),
        )))
        print(f"[{nombre}] Registrado como {nombre_so()} "
              f"({psutil.cpu_count(logical=True)} núcleos lógicos)")

        # 2. Bucle principal: recibir y procesar chunks
        while True:
            data = json.loads(await ws.recv())
            tipo = data.get("type")

            if tipo == P.REGISTERED:
                print(f"[{nombre}] Confirmado. Chunks totales en la misión: "
                      f"{data.get('chunks_totales')}")

            elif tipo == P.TASK_ASSIGN:
                cid = data["chunk_id"]
                inicio, fin = data["inicio"], data["fin"]
                print(f"[{nombre}] Procesando chunk #{cid} [{inicio}, {fin}) ...")

                t0 = time.perf_counter()
                encontrado = procesar_chunk(inicio, fin, data["objetivo"])
                elapsed = time.perf_counter() - t0

                await ws.send(json.dumps(P.msg(
                    P.RESULT,
                    chunk_id=cid,
                    encontrado=encontrado,
                    tiempo=elapsed,
                )))
                extra = f" -> ¡encontrado {encontrado}!" if encontrado is not None else ""
                print(f"[{nombre}] Chunk #{cid} listo en {elapsed:.3f}s{extra}")

            elif tipo == P.NO_MORE:
                print(f"[{nombre}] No quedan más chunks. Trabajo terminado.")
                break


def main():
    # Argumentos: [host] [nombre]
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    nombre = sys.argv[2] if len(sys.argv) > 2 else socket.gethostname()
    puerto = 8000
    try:
        asyncio.run(correr(host, puerto, nombre))
    except ConnectionRefusedError:
        print(f"[{nombre}] No se pudo conectar a {host}:{puerto}. "
              f"¿Está corriendo el coordinador?")
    except KeyboardInterrupt:
        print(f"\n[{nombre}] Interrumpido por el usuario.")


if __name__ == "__main__":
    main()
