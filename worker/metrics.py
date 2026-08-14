"""
Muestreador de métricas del sistema con psutil.
Corre en paralelo al cómputo y envía métricas por WebSocket cada 0.5s.
"""
import asyncio
import json
import os
import sys

import psutil

# Asegurar que protocol.py sea importable
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)
import protocol as P

NUCLEOS = psutil.cpu_count(logical=True) or 1


class Muestreador:
    """
    Toma lecturas periódicas de CPU y RAM del proceso worker y las envía
    al coordinador vía WebSocket mientras se procesa un chunk.
    """

    def __init__(self, ws, nombre: str, chunk_id: int):
        self.ws       = ws
        self.nombre   = nombre
        self.chunk_id = chunk_id
        self.activo   = False
        self.proc     = psutil.Process(os.getpid())
        # Cebar las lecturas para que el primer .cpu_percent() no dé 0.
        self.proc.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None)

    async def iniciar(self):
        """Loop que muestrea cada 0.5s y envía por WebSocket."""
        self.activo = True
        while self.activo:
            await asyncio.sleep(0.5)
            if not self.activo:
                break
            try:
                cpu_proc = self.proc.cpu_percent(interval=None) / NUCLEOS
                cpu_sys  = psutil.cpu_percent(interval=None)
                ram_mb   = self.proc.memory_info().rss / (1024 * 1024)
                await self.ws.send(json.dumps(P.msg(
                    P.METRICS,
                    nombre   = self.nombre,
                    chunk_id = self.chunk_id,
                    cpu_proc = round(cpu_proc, 1),
                    cpu_sys  = round(cpu_sys, 1),
                    ram_mb   = round(ram_mb, 1),
                )))
            except Exception:
                # Si la conexión se cae o el proceso desaparece, salimos silenciosamente.
                break

    def detener(self):
        self.activo = False


def info_estatica() -> dict:
    return {
        "nucleos": NUCLEOS,
        "ram_total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 0),
    }
