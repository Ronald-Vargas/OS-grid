"""
Muestreador de métricas del sistema con psutil.
Corre en paralelo al cómputo y envía métricas por WebSocket mientras se
procesa un chunk.

Dos garantías importantes:
  1. Nunca gira infinito: el loop se despierta con un Event, así que detener()
     lo corta al instante en vez de esperar el sleep completo.
  2. Siempre hay al menos una muestra, aunque el chunk dure menos que el
     intervalo de muestreo (si no, los nodos rápidos reportaban CPU 0%).
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

    Además acumula los picos, que se mandan junto al RESULT para que el
    coordinador los guarde en SQLite (tabla `resultados`).
    """

    PRIMER_INTERVALO = 0.15   # primera lectura rápida: chunks cortos también cuentan
    INTERVALO        = 0.40

    def __init__(self, ws, nombre: str, chunk_id: int):
        self.ws       = ws
        self.nombre   = nombre
        self.chunk_id = chunk_id
        self.proc     = psutil.Process(os.getpid())
        # Cebar las lecturas para que el primer .cpu_percent() no dé 0.
        self.proc.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None)

        self._parar        = asyncio.Event()
        self.muestras      = 0
        self.pico_cpu_proc = 0.0
        self.pico_cpu_sys  = 0.0
        self.pico_ram_mb   = 0.0

    # ── lectura ──────────────────────────────────────────────────────────────

    def leer(self) -> dict:
        """Lee CPU/RAM actuales y actualiza los picos."""
        cpu_proc = self.proc.cpu_percent(interval=None) / NUCLEOS
        cpu_sys  = psutil.cpu_percent(interval=None)
        ram_mb   = self.proc.memory_info().rss / (1024 * 1024)

        self.pico_cpu_proc = max(self.pico_cpu_proc, cpu_proc)
        self.pico_cpu_sys  = max(self.pico_cpu_sys, cpu_sys)
        self.pico_ram_mb   = max(self.pico_ram_mb, ram_mb)
        self.muestras     += 1

        return {"cpu_proc": round(cpu_proc, 1),
                "cpu_sys":  round(cpu_sys, 1),
                "ram_mb":   round(ram_mb, 1)}

    def picos(self) -> dict:
        """Picos de la corrida, con los nombres que espera stats.guardar_resultado."""
        return {"pico_cpu_proc": round(self.pico_cpu_proc, 1),
                "pico_cpu_sys":  round(self.pico_cpu_sys, 1),
                "pico_ram_mb":   round(self.pico_ram_mb, 1)}

    # ── envío ────────────────────────────────────────────────────────────────

    async def enviar_muestra(self) -> bool:
        """Toma una lectura y la manda. Devuelve False si el envío falló."""
        try:
            m = self.leer()
            await self.ws.send(json.dumps(P.msg(
                P.METRICS, nombre=self.nombre, chunk_id=self.chunk_id, **m
            )))
            return True
        except Exception:
            # Si la conexión se cae o el proceso desaparece, salimos silenciosamente.
            return False

    async def iniciar(self):
        """Loop de muestreo. Termina en cuanto detener() o si falla el envío."""
        espera = self.PRIMER_INTERVALO
        while True:
            try:
                await asyncio.wait_for(self._parar.wait(), timeout=espera)
                return                      # nos pidieron parar
            except (asyncio.TimeoutError, TimeoutError):
                pass                        # tocaba muestrear
            espera = self.INTERVALO
            if not await self.enviar_muestra():
                return

    def detener(self):
        self._parar.set()


def info_estatica() -> dict:
    return {
        "nucleos": NUCLEOS,
        "ram_total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 0),
    }
