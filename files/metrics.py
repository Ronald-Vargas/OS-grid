"""
Recolección de métricas del sistema con psutil  (Fase 3)

Este módulo lee el estado real del sistema operativo mientras el worker
procesa un chunk. Es el corazón "de Sistemas Operativos" del proyecto: los
números que salen de aquí son los que después se comparan entre Linux, macOS
y Windows.

Medimos dos cosas distintas a propósito:

  CPU del PROCESO  -> cuánto CPU consume nuestra tarea (el proceso worker).
                      Se normaliza entre 0 y 100% sin importar cuántos núcleos
                      tenga el equipo (dividimos por el número de núcleos), así
                      la comparación entre equipos con distinto hardware es justa.

  CPU GLOBAL       -> cuánto CPU usa TODO el sistema. La diferencia con la del
                      proceso muestra cuánto se lleva el resto del sistema y
                      cómo el scheduler del SO reparte la atención.

  RAM del PROCESO  -> memoria física (RSS) que ocupa el worker, en MB.

Uso: se crea un Muestreador y se llama a .leer() cada cierto intervalo.
psutil funciona igual en los tres SO, por eso el mismo código sirve para todos.
"""

import os
import psutil

# Número de núcleos lógicos del equipo. Se usa para normalizar el CPU del
# proceso: psutil puede reportar hasta 100% * n_núcleos para un proceso
# multihilo, y queremos un valor de 0 a 100 comparable entre equipos.
NUCLEOS = psutil.cpu_count(logical=True) or 1


class Muestreador:
    """Toma lecturas periódicas de CPU y RAM del worker y del sistema."""

    def __init__(self):
        self.proc = psutil.Process(os.getpid())
        # La primera llamada a cpu_percent() siempre devuelve 0.0 porque
        # necesita un intervalo de referencia. La "cebamos" aquí para que
        # las lecturas siguientes ya sean válidas.
        self.proc.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None)

    def leer(self) -> dict:
        """
        Devuelve una lectura instantánea. Pensado para llamarse en bucle
        con una pequeña pausa entre llamadas (p. ej. 0.5 s).
        """
        # CPU del proceso, normalizado a 0-100 sobre el total de núcleos.
        cpu_proc = self.proc.cpu_percent(interval=None) / NUCLEOS
        # CPU global del sistema (ya viene en 0-100).
        cpu_sys = psutil.cpu_percent(interval=None)
        # RAM física del proceso en MB.
        ram_mb = self.proc.memory_info().rss / (1024 * 1024)

        return {
            "cpu_proc": round(cpu_proc, 1),
            "cpu_sys": round(cpu_sys, 1),
            "ram_mb": round(ram_mb, 1),
        }


def info_estatica() -> dict:
    """
    Datos que no cambian durante la corrida, útiles para el informe:
    núcleos y RAM total del equipo.
    """
    return {
        "nucleos": NUCLEOS,
        "ram_total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 0),
    }
