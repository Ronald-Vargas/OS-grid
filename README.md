# OS Grid — Clúster de cómputo distribuido heterogéneo

Proyecto de Sistemas Operativos. Un coordinador reparte una tarea de cómputo
(fuerza bruta de un hash) en *chunks* entre varios *workers* que corren en
distintos sistemas operativos (Linux, macOS, Windows). Cada worker procesa sus
chunks y reporta resultados y métricas al coordinador.

Este repositorio contiene las **Fases 1-2**: registro de workers y reparto
dinámico de chunks, con todo verificable por consola.

## Estructura

```
os-grid/
├── protocol.py            # Contrato de mensajes (compartido)
├── coordinator/
│   └── main.py            # Servidor FastAPI + WebSocket
├── worker/
│   ├── agent.py           # Agente worker (mismo código para los 3 SO)
│   └── tasks.py           # Tarea de cómputo (fuerza bruta de hash)
├── requirements.txt
└── run_local.sh           # Prueba local con 2 workers
```

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

## Prueba pesada (3 chunks)

Además de la corrida original (12 chunks livianos, ~1-3s cada uno), hay una
segunda configuración independiente con solo 3 chunks mucho más pesados
(~15.000.000 de números cada uno, ~24s de cómputo), para que la carga
sostenida por máquina sea mayor y la diferencia de rendimiento entre
Linux/macOS/Windows se note más.

Vive en archivos separados (`coordinator/main_pesado.py`,
`coordinator/stats_pesado.py`, `coordinator/ver_datos_pesado.py`) que no
tocan ni afectan la prueba original, y guarda sus resultados en una base de
datos aparte (`os_grid_pesado.db`).

**Lo único que cambia es el comando del coordinador** (mismo host, mismo
puerto 8000):

```bash
uvicorn coordinator.main_pesado:app --host 0.0.0.0 --port 8000
```

Los workers en las otras máquinas se conectan exactamente igual que siempre
(`python agent.py <IP-del-coordinador> <Nombre>`), sin ningún cambio,
porque hablan el mismo protocolo por el mismo puerto.

Como ambas versiones usan el puerto 8000, no corren al mismo tiempo en la
misma máquina: hay que parar una (Ctrl+C) antes de levantar la otra.

Para probar en localhost antes de la corrida real: `./run_local_pesado.sh`.
Para ver los resultados guardados: `python -m coordinator.ver_datos_pesado`.

## Cómo conectar los otros equipos (más adelante)

El mismo `agent.py` corre en macOS y Windows. Solo necesitan:

1. Python instalado (en Windows, marcar "Add Python to PATH").
2. `pip install websockets psutil`
3. Copiar la carpeta `worker/` y `protocol.py`.
4. Correr `python agent.py <IP-del-coordinador>`.

Para la exposición virtual (equipos en redes distintas), la IP del coordinador
será su IP de Tailscale. El desarrollo local no cambia.

## Próximas fases

- **Fase 3:** métricas de CPU/RAM en vivo con `psutil`.
- **Fase 4:** persistencia de métricas en SQLite.
- **Fase 5:** conexión de los tres equipos por Tailscale.
- **Fase 6:** dashboard Angular con gráficas en vivo.
