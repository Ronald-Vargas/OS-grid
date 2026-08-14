"""
Tarea de cómputo distribuida: fuerza bruta de un hash.

La "tarea global" es encontrar qué número entre 0 y N produce un hash SHA-256
dado. Ese rango se parte en CHUNKS, y cada worker recibe un sub-rango
[inicio, fin) para revisar. Es ideal para el proyecto porque:

  - Se divide de forma trivial y pareja (cada chunk es un rango de números).
  - Es CPU-bound puro: mantiene el procesador ocupado, que es justo lo que
    queremos para observar cómo cada SO planifica la carga.
  - El resultado es fácil de verificar.

En Fase 2 corre en el worker. Más adelante se puede reemplazar la tarea por
multiplicación de matrices, etc., sin tocar el resto del sistema.
"""

import hashlib


def hash_de(numero: int) -> str:
    """SHA-256 de un número, como texto hexadecimal."""
    return hashlib.sha256(str(numero).encode()).hexdigest()


def procesar_chunk(inicio: int, fin: int, objetivo: str):
    """
    Revisa los números en el rango [inicio, fin) buscando el que genera
    el hash 'objetivo'. Devuelve el número si lo encuentra, o None.

    Este es el trabajo pesado: es lo que consume CPU y lo que el
    scheduler de cada SO tiene que planificar.
    """
    for n in range(inicio, fin):
        if hash_de(n) == objetivo:
            return n
    return None
