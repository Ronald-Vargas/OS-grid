# OS Grid — Clúster de cómputo distribuido heterogéneo

Proyecto de Sistemas Operativos. Un coordinador reparte una tarea de cómputo
(fuerza bruta de un hash) en *chunks* entre varios *workers* que corren en
distintos sistemas operativos (Linux, macOS, Windows). Cada worker procesa sus
chunks y reporta resultados y métricas al coordinador.

Este repositorio contiene las **Fases 1-6**: registro de workers, reparto
dinámico de chunks, métricas de CPU/RAM en vivo con psutil, persistencia en
SQLite, y un dashboard Angular en tiempo real.

## Estructura

```
os-grid/
├── protocol.py            # Contrato de mensajes (compartido)
├── coordinator/
│   ├── __init__.py
│   ├── main.py            # Servidor FastAPI + WebSocket + canal dashboard
│   ├── stats.py           # Persistencia en SQLite (Fase 4)
│   ├── ver_datos.py       # Consultar y exportar datos a CSV (Fase 4)
│   └── static/            # Dashboard Angular compilado (se sirve aquí)
├── worker/
│   ├── agent.py           # Agente worker (mismo código para los 3 SO)
│   ├── tasks.py           # Tarea de cómputo (fuerza bruta de hash)
│   └── metrics.py         # Métricas de CPU/RAM con psutil (Fase 3)
├── dashboard/             # Proyecto Angular 17 (código fuente)
├── requirements.txt
├── run_local.sh           # Prueba local con 2 workers
├── build_dashboard.sh     # Compila el dashboard y lo copia a static/
└── os_grid.db             # Base de datos (se crea al correr; no se versiona)
```

## Qué mide la Fase 3

Mientras cada worker procesa un chunk, muestrea cada 500 ms y reporta:

- **CPU del proceso** (0-100%, normalizado por núcleos): cuánto consume la
  tarea del worker.
- **CPU global del sistema**: cuánto usa todo el equipo. La diferencia con la
  del proceso muestra cómo el scheduler del SO reparte la atención.
- **RAM del proceso** (MB): memoria física que ocupa el worker.

Además, cada RESULT incluye los **picos** de CPU y RAM de ese chunk.

## Instalación (Fedora)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

### Opción rápida: prueba local automática

```bash
./run_local.sh
```

Levanta el coordinador y dos workers en `localhost`, procesa toda la cola y
muestra el resumen. Sirve para verificar que todo funciona en tu equipo.

### Opción manual (dos o tres terminales)

**Terminal 1 — coordinador:**
```bash
source .venv/bin/activate
uvicorn coordinator.main:app --host 0.0.0.0 --port 8000
```

**Terminal 2 — worker Linux (tu Fedora):**
```bash
source .venv/bin/activate
cd worker
python agent.py localhost
```

Podés abrir más terminales con más workers. Cada uno pide chunks hasta que la
cola se vacía.

### Ver el estado del coordinador

Con el coordinador corriendo, abrí en el navegador o con curl:

```bash
curl http://localhost:8000/
```

## Cómo conectar los otros equipos (más adelante)

El mismo `agent.py` corre en macOS y Windows. Solo necesitan:

1. Python instalado (en Windows, marcar "Add Python to PATH").
2. `pip install websockets psutil`
3. Copiar la carpeta `worker/` y `protocol.py`.
4. Correr `python agent.py <IP-del-coordinador>`.

Para la exposición virtual (equipos en redes distintas), la IP del coordinador
será su IP de Tailscale. El desarrollo local no cambia.

## Ver y exportar los datos (Fase 4)

Cada corrida se guarda en `os_grid.db`. Para revisarla:

```bash
python -m coordinator.ver_datos           # lista todas las corridas
python -m coordinator.ver_datos 1         # detalle comparativo de la corrida #1
python -m coordinator.ver_datos 1 --csv   # exporta la corrida #1 a corrida_1.csv
```

El CSV se abre directo en Excel o LibreOffice para hacer los gráficos del
informe. La comparativa por SO (tiempo promedio, CPU, RAM) es la base del
análisis de resultados.

## Dashboard en vivo (Fase 6)

El dashboard es un proyecto Angular 17 que muestra en tiempo real las tarjetas
por nodo (CPU/RAM), gráficas de líneas, progreso global y el leaderboard final.

**Compilarlo** (requiere Node.js):

```bash
./build_dashboard.sh
```

Esto compila el proyecto de `dashboard/` y copia el resultado a
`coordinator/static/`. A partir de ahí, con el coordinador corriendo, el
dashboard se abre en el navegador en:

```
http://localhost:8000/
```

Al abrirlo, se conecta solo al coordinador. Para la exposición con equipos
remotos, en el campo "host" del dashboard se pone la IP de Tailscale del
coordinador.

**Desarrollo del dashboard** (opcional, con recarga automática):

```bash
cd dashboard
npm install
npm start          # ng serve en http://localhost:4200
```

## Próximas fases

- **Fase 5:** conexión de los tres equipos por Tailscale.
- **Fase 7:** experimentos, gráficos y redacción del informe.
