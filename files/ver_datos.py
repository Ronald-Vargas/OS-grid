"""
Utilidad para ver y exportar los datos de OS Grid  (Fase 4)

Herramienta de línea de comandos para revisar las corridas guardadas y
exportarlas a CSV, listas para abrir en Excel/LibreOffice y hacer los
gráficos del informe.

Uso (desde la carpeta os-grid, con el venv activo):

    python -m coordinator.ver_datos              # lista todas las corridas
    python -m coordinator.ver_datos 3            # detalle de la corrida #3
    python -m coordinator.ver_datos 3 --csv      # exporta la corrida #3 a CSV

El CSV se guarda como  corrida_<id>.csv  en la carpeta del proyecto.
"""

import csv
import sqlite3
import sys
from pathlib import Path

RUTA_DB = Path(__file__).resolve().parent.parent / "os_grid.db"


def _con():
    if not RUTA_DB.exists():
        print("Todavía no hay base de datos. Corré una misión primero.")
        sys.exit(1)
    return sqlite3.connect(RUTA_DB)


def listar_corridas():
    con = _con()
    filas = con.execute("""
        SELECT c.id, c.fecha, c.num_chunks, COUNT(r.id) AS resultados
        FROM corridas c
        LEFT JOIN resultados r ON r.corrida_id = c.id
        GROUP BY c.id
        ORDER BY c.id DESC
    """).fetchall()
    con.close()
    if not filas:
        print("No hay corridas guardadas todavía.")
        return
    print(f"\n{'ID':>3}  {'Fecha':<20} {'Chunks':>7} {'Resultados':>11}")
    print("-" * 46)
    for cid, fecha, nchunks, nres in filas:
        print(f"{cid:>3}  {fecha:<20} {nchunks:>7} {nres:>11}")
    print("\nUsá  python -m coordinator.ver_datos <id>  para ver el detalle.\n")


def detalle_corrida(cid: int):
    con = _con()
    # Resumen por SO
    porso = con.execute("""
        SELECT so,
               COUNT(*),
               ROUND(AVG(tiempo), 3),
               ROUND(MIN(tiempo), 3),
               ROUND(MAX(tiempo), 3),
               ROUND(AVG(pico_cpu_proc), 1),
               ROUND(AVG(pico_cpu_sys), 1),
               ROUND(AVG(pico_ram_mb), 1)
        FROM resultados WHERE corrida_id = ?
        GROUP BY so ORDER BY AVG(tiempo)
    """, (cid,)).fetchall()
    con.close()

    if not porso:
        print(f"No hay resultados para la corrida #{cid}.")
        return

    print(f"\n=== Corrida #{cid} — comparativa por sistema operativo ===\n")
    print(f"{'SO':<10}{'chunks':>7}{'t.prom':>9}{'t.min':>8}{'t.max':>8}"
          f"{'CPUp%':>8}{'CPUs%':>8}{'RAM MB':>9}")
    print("-" * 67)
    for so, n, tp, tmin, tmax, cp, cs, ram in porso:
        print(f"{(so or '?'):<10}{n:>7}{tp:>9}{tmin:>8}{tmax:>8}"
              f"{cp:>8}{cs:>8}{ram:>9}")
    print()


def exportar_csv(cid: int):
    con = _con()
    filas = con.execute("""
        SELECT nodo, so, chunk_id, tiempo,
               pico_cpu_proc, pico_cpu_sys, pico_ram_mb, encontrado
        FROM resultados WHERE corrida_id = ?
        ORDER BY chunk_id
    """, (cid,)).fetchall()
    con.close()

    if not filas:
        print(f"No hay resultados para la corrida #{cid}.")
        return

    salida = RUTA_DB.parent / f"corrida_{cid}.csv"
    with open(salida, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["nodo", "so", "chunk_id", "tiempo_s",
                    "pico_cpu_proc", "pico_cpu_sys", "pico_ram_mb", "encontrado"])
        w.writerows(filas)
    print(f"Exportado: {salida}  ({len(filas)} filas)")


def main():
    args = sys.argv[1:]
    if not args:
        listar_corridas()
        return
    cid = int(args[0])
    if "--csv" in args:
        exportar_csv(cid)
    else:
        detalle_corrida(cid)


if __name__ == "__main__":
    main()
