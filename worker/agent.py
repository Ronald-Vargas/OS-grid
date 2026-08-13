"""
Agente worker de OS Grid  (Fase 3)

Este MISMO archivo corre sin cambios en Fedora, macOS y Windows. Es la pieza
que demuestra la portabilidad del sistema: solo necesita Python y las
librerías 'websockets' y 'psutil'.

Ciclo de vida:
  1. Se conecta al coordinador por WebSocket.
  2. Se presenta (REGISTER) con su SO, núcleos y RAM total.
  3. Recibe chunks (TASK_ASSIGN). Mientras procesa cada chunk, en paralelo
     muestrea CPU y RAM cada 500 ms y las envía como mensajes METRICS.
  4. Al terminar el chunk reporta el resultado (RESULT).
  5. Repite hasta que el coordinador avisa NO_MORE.

La clave técnica de esta fase: el cómputo de un chunk es bloqueante (un bucle
que consume CPU). Para poder muestrear métricas EN PARALELO sin frenar ni el
cómputo ni el envío de mensajes, corremos el cómputo en un executor (otro hilo)
y dejamos el event loop de asyncio libre para muestrear y mandar METRICS.

Uso:
    python agent.py                        # se conecta a localhost
    python agent.py 100.x.x.x              # IP remota (Fase 5, Tailscale)
    python agent.py 100.x.x.x MiNombre     # además fija un nombre de nodo
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
from metrics import Muestreador, info_estatica  # noqa: E402

INTERVALO_METRICS = 0.5  # segundos entre lecturas de métricas


def nombre_so() -> str:
    """Nombre legible del sistema operativo: Linux / Darwin(macOS) / Windows."""
    s = platform.system()
    return {"Darwin": "macOS"}.get(s, s)


async def procesar_con_metricas(ws, nombre, cid, inicio, fin, objetivo):
    """
    Procesa un chunk en un hilo aparte y, mientras tanto, muestrea métricas
    cada INTERVALO_METRICS segundos enviándolas al coordinador.

    Devuelve (encontrado, tiempo, pico_cpu_proc, pico_cpu_sys, pico_ram).
    """
    loop = asyncio.get_running_loop()
    muestreador = Muestreador()

    # Lanzamos el cómputo pesado en un executor (otro hilo). Esto libera el
    # event loop para poder muestrear y mandar mensajes mientras se procesa.
    t0 = time.perf_counter()
    tarea_computo = loop.run_in_executor(
        None, procesar_chunk, inicio, fin, objetivo
    )

    pico_cpu_proc = pico_cpu_sys = pico_ram = 0.0

    # Bucle de muestreo: sigue leyendo hasta que el cómputo termina.
    while not tarea_computo.done():
        await asyncio.sleep(INTERVALO_METRICS)
        m = muestreador.leer()
        pico_cpu_proc = max(pico_cpu_proc, m["cpu_proc"])
        pico_cpu_sys = max(pico_cpu_sys, m["cpu_sys"])
        pico_ram = max(pico_ram, m["ram_mb"])
        # Enviamos la lectura en vivo al coordinador.
        await ws.send(json.dumps(P.msg(
            P.METRICS,
            nombre=nombre,
            chunk_id=cid,
            cpu_proc=m["cpu_proc"],
            cpu_sys=m["cpu_sys"],
            ram_mb=m["ram_mb"],
        )))
        print(f"[{nombre}]   . CPU proc {m['cpu_proc']:5.1f}% | "
              f"CPU sys {m['cpu_sys']:5.1f}% | RAM {m['ram_mb']:6.1f} MB")

    encontrado = await tarea_computo
    elapsed = time.perf_counter() - t0
    return encontrado, elapsed, pico_cpu_proc, pico_cpu_sys, pico_ram


async def correr(host: str, puerto: int, nombre: str):
    uri = f"ws://{host}:{puerto}/ws"
    print(f"[{nombre}] Conectando a {uri} ...")

    async with websockets.connect(uri) as ws:
        est = info_estatica()
        # 1. Registro (ahora incluye RAM total del equipo)
        await ws.send(json.dumps(P.msg(
            P.REGISTER,
            nombre=nombre,
            os=nombre_so(),
            cpu=est["nucleos"],
            ram_total_mb=est["ram_total_mb"],
        )))
        print(f"[{nombre}] Registrado como {nombre_so()} "
              f"({est['nucleos']} nucleos, {est['ram_total_mb']:.0f} MB RAM)")

        # 2. Bucle principal: recibir y procesar chunks
        while True:
            data = json.loads(await ws.recv())
            tipo = data.get("type")

            if tipo == P.REGISTERED:
                print(f"[{nombre}] Confirmado. Chunks totales en la mision: "
                      f"{data.get('chunks_totales')}")

            elif tipo == P.TASK_ASSIGN:
                cid = data["chunk_id"]
                inicio, fin = data["inicio"], data["fin"]
                print(f"[{nombre}] Procesando chunk #{cid} [{inicio}, {fin}) ...")

                (encontrado, elapsed, pico_cpu_proc,
                 pico_cpu_sys, pico_ram) = await procesar_con_metricas(
                    ws, nombre, cid, inicio, fin, data["objetivo"]
                )

                # Reportamos el resultado, incluyendo los picos de esta corrida.
                await ws.send(json.dumps(P.msg(
                    P.RESULT,
                    chunk_id=cid,
                    encontrado=encontrado,
                    tiempo=elapsed,
                    pico_cpu_proc=round(pico_cpu_proc, 1),
                    pico_cpu_sys=round(pico_cpu_sys, 1),
                    pico_ram_mb=round(pico_ram, 1),
                )))
                extra = f" -> encontrado {encontrado}!" if encontrado is not None else ""
                print(f"[{nombre}] Chunk #{cid} listo en {elapsed:.3f}s "
                      f"(pico CPU proc {pico_cpu_proc:.0f}%){extra}")

            elif tipo == P.NO_MORE:
                print(f"[{nombre}] No quedan mas chunks. Trabajo terminado.")
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
              f"Esta corriendo el coordinador?")
    except KeyboardInterrupt:
        print(f"\n[{nombre}] Interrumpido por el usuario.")


if __name__ == "__main__":
    main()
