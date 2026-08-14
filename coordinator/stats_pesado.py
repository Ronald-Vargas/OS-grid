"""
Persistencia de métricas en SQLite — Prueba pesada (3 chunks)

Copia de stats.py que guarda en un archivo de base de datos separado
(os_grid_pesado.db) para no mezclar los resultados de la prueba pesada con
los de la corrida original de 12 chunks (os_grid.db). El esquema de tablas
es idéntico; solo cambia la ruta del archivo.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

# La base vive en la raíz del proyecto, junto al código, con nombre propio.
RUTA_DB = Path(__file__).resolve().parent.parent / "os_grid_pesado.db"


def conectar():
    """Abre (o crea) la base de datos y asegura que las tablas existen."""
    con = sqlite3.connect(RUTA_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS corridas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha       TEXT NOT NULL,
            objetivo    TEXT NOT NULL,
            num_chunks  INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS resultados (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            corrida_id     INTEGER NOT NULL,
            nodo           TEXT NOT NULL,
            so             TEXT,
            chunk_id       INTEGER NOT NULL,
            tiempo         REAL,
            pico_cpu_proc  REAL,
            pico_cpu_sys   REAL,
            pico_ram_mb    REAL,
            encontrado     INTEGER,
            FOREIGN KEY (corrida_id) REFERENCES corridas (id)
        )
    """)
    con.commit()
    return con


def nueva_corrida(objetivo: str, num_chunks: int) -> int:
    """Registra el inicio de una corrida y devuelve su id."""
    con = conectar()
    cur = con.execute(
        "INSERT INTO corridas (fecha, objetivo, num_chunks) VALUES (?, ?, ?)",
        (datetime.now().isoformat(timespec="seconds"), objetivo, num_chunks),
    )
    con.commit()
    corrida_id = cur.lastrowid
    con.close()
    return corrida_id


def guardar_resultado(corrida_id: int, nodo: str, so: str, data: dict):
    """
    Guarda el resultado de un chunk. 'data' es el mensaje RESULT que llega
    del worker, con tiempo y picos.
    """
    con = conectar()
    con.execute("""
        INSERT INTO resultados
            (corrida_id, nodo, so, chunk_id, tiempo,
             pico_cpu_proc, pico_cpu_sys, pico_ram_mb, encontrado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        corrida_id,
        nodo,
        so,
        data.get("chunk_id"),
        data.get("tiempo"),
        data.get("pico_cpu_proc"),
        data.get("pico_cpu_sys"),
        data.get("pico_ram_mb"),
        data.get("encontrado"),
    ))
    con.commit()
    con.close()


def resumen_por_so(corrida_id: int) -> list:
    """
    Devuelve estadísticas agregadas por sistema operativo para una corrida:
    cuántos chunks hizo cada SO, tiempo promedio y CPU promedio.
    Útil para el análisis del informe.
    """
    con = conectar()
    filas = con.execute("""
        SELECT so,
               COUNT(*)                 AS chunks,
               ROUND(AVG(tiempo), 3)    AS tiempo_prom,
               ROUND(AVG(pico_cpu_proc), 1) AS cpu_proc_prom,
               ROUND(AVG(pico_ram_mb), 1)   AS ram_prom
        FROM resultados
        WHERE corrida_id = ?
        GROUP BY so
        ORDER BY tiempo_prom ASC
    """, (corrida_id,)).fetchall()
    con.close()
    return filas
