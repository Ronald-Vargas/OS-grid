import asyncio
import os
import psutil

NUCLEOS = psutil.cpu_count(logical=True) or 1

class Muestreador:
    def __init__(self, ws, nombre: str, chunk_id: int):
        self.ws       = ws
        self.nombre   = nombre
        self.chunk_id = chunk_id
        self.activo   = False
        self.proc     = psutil.Process(os.getpid())
        self.proc.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None)

    async def iniciar(self):
        self.activo = True
        while self.activo:
            await asyncio.sleep(0.5)
            if not self.activo:
                break
            cpu_proc = self.proc.cpu_percent(interval=None) / NUCLEOS
            cpu_sys  = psutil.cpu_percent(interval=None)
            ram_mb   = self.proc.memory_info().rss / (1024 * 1024)
            try:
                import json, protocol as P
                await self.ws.send(json.dumps(P.msg(
                    P.METRICS,
                    nombre   = self.nombre,
                    chunk_id = self.chunk_id,
                    cpu_proc = round(cpu_proc, 1),
                    cpu_sys  = round(cpu_sys, 1),
                    ram_mb   = round(ram_mb, 1),
                )))
            except Exception:
                break

    def detener(self):
        self.activo = False

def info_estatica() -> dict:
    return {
        "nucleos": NUCLEOS,
        "ram_total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 0),
    }
