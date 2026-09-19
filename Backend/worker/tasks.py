"""
Tareas de cómputo que ejecutan los workers.

Cada función recibe los parámetros del chunk y devuelve un dict con:
  - tiempo   (float, segundos)
  - encontrado (int o None, solo para hash)
  - extras   (dict opcional, datos adicionales por tipo de tarea)
"""
import hashlib
import random
import time


# ── SHA-256 (búsqueda por fuerza bruta) ──────────────────────────────────────

def hash_de(n: int) -> str:
    return hashlib.sha256(str(n).encode()).hexdigest()


def chunk_hash(inicio: int, fin: int, objetivo: str) -> dict:
    t0 = time.perf_counter()
    encontrado = None
    for n in range(inicio, fin):
        if hash_de(n) == objetivo:
            encontrado = n
            break
    return {"tiempo": time.perf_counter() - t0, "encontrado": encontrado}


# ── Ordenamiento (lista aleatoria de floats) ─────────────────────────────────

def chunk_sort(tam: int) -> dict:
    t0 = time.perf_counter()
    arr = [random.random() for _ in range(tam)]
    arr.sort()
    return {"tiempo": time.perf_counter() - t0, "encontrado": None,
            "extras": {"elementos": tam, "min": arr[0], "max": arr[-1]}}


# ── Números primos (criba segmentada) ────────────────────────────────────────

def chunk_primos(inicio: int, fin: int) -> dict:
    t0 = time.perf_counter()
    bajo = max(inicio, 2)
    if bajo >= fin:
        return {"tiempo": 0.0, "encontrado": None, "extras": {"primos": 0}}
    n = fin - bajo
    es_p = bytearray(b'\x01') * n
    for p in range(2, int(fin ** 0.5) + 1):
        start = ((bajo + p - 1) // p) * p
        if start == p:
            start += p
        for j in range(start - bajo, n, p):
            es_p[j] = 0
    count = sum(es_p)
    return {"tiempo": time.perf_counter() - t0, "encontrado": None,
            "extras": {"primos": count, "rango": f"{bajo}-{fin}"}}


# ── Despachador principal ─────────────────────────────────────────────────────

def procesar_chunk(tipo: str, chunk_id: int,
                   inicio: int = 0, fin: int = 0,
                   objetivo: str = "", tam: int = 0) -> dict:
    """
    Despacha la tarea según `tipo` y devuelve el resultado estandarizado.
    Siempre incluye: chunk_id, tiempo, encontrado.
    """
    if tipo == "hash":
        res = chunk_hash(inicio, fin, objetivo)
    elif tipo == "sort":
        res = chunk_sort(tam)
    elif tipo == "primos":
        res = chunk_primos(inicio, fin)
    else:
        res = {"tiempo": 0.0, "encontrado": None}

    return {"chunk_id": chunk_id, **res}
