# Prompt para generar el informe escrito de OS Grid

Copiá y pegá TODO lo que está debajo de la línea en tu LLM de preferencia.
Antes de pegarlo, rellená los campos marcados con `<<< >>>` y regenerá la tabla
de datos con `python -m coordinator.ver_datos` (los números crecen con cada corrida).

---

Actuá como estudiante avanzado de Ingeniería en Sistemas redactando el informe
final de un proyecto del curso de **Sistemas Operativos**. Escribí en español
neutro-latinoamericano, en registro técnico-académico, en tercera persona
impersonal ("se implementó", "se midió"). El lector es un profesor universitario
que conoce teoría de SO pero no conoce este proyecto.

## REGLA CRÍTICA — no inventar

Usá **únicamente** los datos, cifras, nombres de archivo y comportamientos que te
doy en este prompt. Está prohibido inventar métricas, benchmarks, citas
bibliográficas, versiones de software o resultados que no aparezcan acá. Si para
alguna sección te falta información, escribí literalmente
`[PENDIENTE: <qué dato falta>]` y seguí. Es mucho mejor un hueco marcado que un
dato falso.

## Datos del trabajo (rellenar)

- Universidad / curso: `<<<UNIVERSIDAD Y CURSO>>>`
- Profesor: `<<<NOMBRE DEL PROFESOR>>>`
- Integrantes: `<<<NOMBRES>>>`
- Fecha de entrega: `<<<FECHA>>>`
- Extensión objetivo: `<<<20-25 páginas>>>`
- Formato de citas: `<<<APA 7 / IEEE / ninguno>>>`

---

# DOSSIER TÉCNICO DEL SISTEMA

## 1. Qué es

**OS Grid** es un clúster de cómputo distribuido heterogéneo. Un **coordinador**
parte una tarea pesada en *chunks* y los reparte entre **tres workers** que
corren en tres sistemas operativos distintos (Linux, macOS y Windows). El
objetivo del proyecto no es solo distribuir trabajo: es **comparar el rendimiento
de los tres SO ejecutando exactamente el mismo código Python**, en un formato de
competencia ("battle royale") donde gana el SO con mejor tiempo promedio por chunk.

## 2. Arquitectura

```
                    ┌──────────────────────────┐
                    │   COORDINADOR (Fedora)   │
                    │   FastAPI + Uvicorn      │
                    │   WebSocket /ws          │  ← workers
                    │   WebSocket /ws/dashboard│  ← navegador
                    │   SQLite: os_grid.db     │
                    │   Sirve el dashboard     │
                    └───────────┬──────────────┘
                                │ Tailscale (VPN malla, WebSocket sobre TCP)
              ┌─────────────────┼─────────────────┐
              │                 │                 │
      ┌───────▼──────┐  ┌───────▼──────┐  ┌───────▼──────┐
      │  NodoFedora  │  │   NodoMac    │  │ NodoWindows  │
      │    Linux     │  │    macOS     │  │   Windows    │
      │  worker/     │  │  worker/     │  │  worker/     │
      │  agent.py    │  │  agent.py    │  │  agent.py    │
      └──────────────┘  └──────────────┘  └──────────────┘
```

Punto clave para el informe: **el mismo archivo `worker/agent.py`, sin ninguna
rama condicional por SO, corre en las tres máquinas.** La única función que
detecta el SO es `detectar_os()`, y se usa solo para etiquetar las métricas.

## 3. Stack

| Componente | Tecnología |
|---|---|
| Coordinador | Python 3.14.6, FastAPI 0.141.1, Uvicorn 0.52.3 |
| Workers | Python 3.14, websockets 17.0.1, psutil 7.2.2 |
| Dashboard | Angular 17.3, TypeScript 5.4, signals |
| Persistencia | SQLite (módulo `sqlite3` de la stdlib) |
| Red | Tailscale (las 3 máquinas están en redes físicas distintas) |

## 4. Estructura del repositorio

```
os-grid/
├── protocol.py              # Contrato de mensajes compartido
├── coordinator/
│   ├── main.py              # Servidor, máquina de estados, reparto, vigilante
│   ├── stats.py             # Persistencia SQLite y agregaciones
│   ├── ver_datos.py         # CLI para listar/exportar corridas a CSV
│   └── static/              # Dashboard Angular compilado
├── worker/
│   ├── agent.py             # Agente (idéntico en los 3 SO)
│   ├── tasks.py             # Las tres tareas de cómputo
│   └── metrics.py           # Muestreador de CPU/RAM con psutil
├── dashboard/               # Código fuente Angular
└── os_grid.db               # Base de datos de resultados
```

## 5. Protocolo (`protocol.py`)

Mensajes JSON sobre WebSocket.

**Worker → Coordinador:**
- `REGISTER` — se presenta: nombre, SO, núcleos lógicos, RAM total
- `RESULT` — terminó un chunk: chunk_id, tiempo, encontrado, picos de CPU/RAM
- `METRICS` — telemetría en vivo mientras procesa (cada ~0.4 s)

**Coordinador → Worker:**
- `REGISTERED` — confirma el registro
- `TASK_ASSIGN` — le entrega un chunk (id, tipo, inicio, fin, tam, objetivo)
- `NO_MORE` — no quedan chunks; el worker NO cierra, queda esperando la próxima misión

## 6. Máquina de estados del coordinador

```
esperando ──(se conectan 3 workers)──> listo ──(el usuario arranca)──> corriendo
    ▲                                    ▲                                 │
    └──(baja de 3 workers)───────────────┴──────(todos los chunks listos)──┘
```

- `MIN_WORKERS = 3`: la misión no puede arrancar sin los tres SO conectados.
- Al terminar, el coordinador vuelve solo a `listo`: se pueden encadenar misiones
  sin reiniciar nada.
- Las conexiones de los workers son **persistentes**: sobreviven a varias misiones.

## 7. Reparto de carga: work-stealing dinámico

Este es el corazón del sistema y merece una sección propia en el informe.

1. Al arrancar la misión, el coordinador le manda **un** chunk a cada worker.
2. Apenas un worker devuelve su `RESULT`, el coordinador le manda el siguiente.
3. Se repite hasta vaciar la cola.

**Consecuencia teórica:** el reparto es proporcional a la velocidad real de cada
máquina, sin que el coordinador sepa nada del hardware. El nodo más rápido
procesa más chunks. Contrastalo explícitamente con un **reparto estático**
(dividir 18 chunks en 6+6+6 de antemano), donde el tiempo total quedaría atado
al nodo más lento — es el argumento central a favor del diseño elegido.

Este mecanismo también explica por qué la comparación entre SO se hace por
**tiempo promedio por chunk** y no por cantidad de chunks: la cantidad depende
del reparto, el tiempo por chunk mide la máquina.

## 8. Las tres pruebas (`worker/tasks.py`)

Explicá cada una: qué computa, qué subsistema del SO estresa y por qué se eligió.

### 8.1 `hash` — fuerza bruta de SHA-256

Busca el número secreto **8 450 137** probando `sha256(str(n))` para cada `n` del
rango asignado, hasta igualar el hash objetivo. Es **CPU pura**: sin E/S, con
memoria constante. Mide la velocidad bruta de cómputo entero y el planificador.
Es la única prueba que puede terminar antes de tiempo (cuando el chunk contiene
el número, sale del bucle con `break`).

### 8.2 `sort` — ordenamiento en memoria

Genera una lista de `tam` floats aleatorios (`random.random()`) y la ordena con
`list.sort()` (Timsort). Estresa **asignación de memoria, caché y el gestor de
memoria del SO**. Es la prueba con mayor huella de RAM.

### 8.3 `primos` — criba de Eratóstenes segmentada

Cuenta los primos del rango con una criba sobre un `bytearray`. Mezcla cómputo
con **acceso intensivo a memoria por patrones de salto** (escribe cada `p`
posiciones), lo que castiga la localidad de caché.

## 9. Las tres intensidades (`CONFIGURACIONES` en `coordinator/main.py`)

Reproducí esta tabla en el informe y explicá la asimetría que viene después.

| Prueba | Intensidad | Parámetro total | Chunks | Trabajo por chunk |
|---|---|---|---|---|
| hash | ligero | rango 3 000 000 | 6 | 500 000 hashes |
| hash | normal | rango 12 000 000 | 12 | 1 000 000 hashes |
| hash | pesado | rango 40 000 000 | 18 | 2 222 222 hashes |
| sort | ligero | tam 200 000 | 6 | 200 000 elementos |
| sort | normal | tam 1 000 000 | 12 | 1 000 000 elementos |
| sort | pesado | tam 4 000 000 | 18 | 4 000 000 elementos |
| primos | ligero | rango 1 000 000 | 6 | 166 666 números |
| primos | normal | rango 5 000 000 | 12 | 416 666 números |
| primos | pesado | rango 20 000 000 | 18 | 1 111 111 números |

**Asimetría importante que hay que señalar en el informe:** en `hash` y `primos`
el rango total se *divide* entre los chunks (`tam_ch = rango // n`), de modo que
el trabajo total es el rango. En `sort`, en cambio, **cada chunk ordena una lista
completa de `tam` elementos**, así que el trabajo total es `n × tam`. Por eso la
intensidad "pesado" de `sort` (18 × 4 000 000 = 72 millones de elementos
ordenados) es desproporcionadamente más costosa que la de las otras dos. Es una
decisión de diseño, no un error, pero hay que explicitarla al comparar tiempos
entre tipos de prueba.

## 10. Medición de métricas (`worker/metrics.py`)

- Con `psutil` se mide, del **proceso worker**: `cpu_percent` normalizado por
  núcleos lógicos, y `memory_info().rss`. Del sistema completo: `cpu_percent`.
- Primera muestra a los **0.15 s**, luego cada **0.4 s**. El primer intervalo es
  corto a propósito: los chunks del nodo más rápido duran ~0.12 s y con el
  muestreo original (0.5 s) no generaban ni una sola lectura, reportando 0 %.
- Se garantiza **al menos una muestra por chunk**.
- Los **picos** de CPU y RAM viajan dentro del `RESULT` y se persisten en SQLite.

**Punto de teoría de SO obligatorio:** el cómputo es bloqueante y sostiene el
GIL. Para poder muestrear y enviar telemetría *mientras* se computa, el chunk se
ejecuta con `loop.run_in_executor(None, ...)` en un hilo del ThreadPoolExecutor,
dejando libre el event loop de asyncio. Discutí concurrencia vs paralelismo, el
rol del GIL y por qué acá los hilos sirven aunque no den paralelismo real de CPU.

## 11. Persistencia (`coordinator/stats.py`)

Dos tablas:

- `corridas(id, fecha, objetivo, num_chunks)`
- `resultados(id, corrida_id, nodo, so, chunk_id, tiempo, pico_cpu_proc,
  pico_cpu_sys, pico_ram_mb, encontrado)`

Agregaciones:
- `resumen_por_so(corrida_id)` — comparativa de una misión
- `resumen_global()` — marcador acumulado histórico
- `corridas_ganadas()` — victorias por SO. **Gana la misión el SO con menor
  tiempo promedio por chunk**; a igualdad, el que hizo más chunks. Solo cuentan
  las corridas en las que compitió más de un SO.

Herramienta CLI: `python -m coordinator.ver_datos [id] [--csv]`.

## 12. Tolerancia a fallos

Sección importante: al principio el sistema **se trababa por completo** si un
worker fallaba. Contá el problema y la solución.

- **Contabilidad de chunks en vuelo** (`asignados`): el coordinador sabe qué
  chunk tiene cada worker y desde cuándo.
- **Reencolado** ante: desconexión del worker, fallo al enviar el `TASK_ASSIGN`,
  o vencimiento del timeout.
- **Vigilante (watchdog)**: revisa cada 5 s; si un chunk lleva más de 120 s sin
  respuesta, lo devuelve a la cola y pone al worker en cuarentena.
- **Máximo 3 intentos por chunk**: garantiza que la misión termina aunque un
  chunk sea irrecuperable, en vez de colgarse para siempre.
- **Deduplicación**: si un chunk reasignado vuelve dos veces, el segundo se ignora.
- **Reconexión con el mismo nombre**: se descarta la sesión zombi y se liberan
  sus chunks, sin borrar del registro al worker recién reconectado.

Conceptos de SO a conectar: detección de fallos por timeout, idempotencia,
recuperación sin estado compartido, el problema de los "procesos zombi".

## 13. Dashboard — DOS PESTAÑAS

El dashboard Angular se sirve desde el propio coordinador en
`http://<IP-del-coordinador>:8000/` y tiene dos pestañas. **Describí cada una y
dejá el hueco para la captura correspondiente.**

### Pestaña 1 — "Panel principal"
Vista operativa y de datos:
- Barra de conexión y progreso de la misión (`N / M chunks`)
- Pantalla de espera con contador `X / 3 equipos conectados`
- **Panel de configuración**: elegir tipo de prueba (SHA-256 / Ordenamiento /
  Núm. primos) e intensidad (Ligero / Normal / Pesado), y botón "Arrancar misión"
- Una **tarjeta por nodo** con sparkline de CPU en vivo, CPU proceso, CPU sistema,
  RAM y chunks completados
- **Tabla de resultado de la misión** por SO, con medallas y el ganador resaltado
- **Tabla de reparto por nodo** (chunks y % del total)
- **Marcador acumulado** de todas las corridas guardadas

### Pestaña 2 — "⚡ Interacción SO" (vista Battle Royale)
Vista de visualización y competencia:
- Nodo coordinador central con barra de progreso, y líneas SVG hacia cada worker
  que se iluminan cuando ese nodo está procesando
- Tarjeta por worker con el **planificador de su SO** rotulado
  (Linux: CFS/EEVDF · macOS: XNU Mach+BSD · Windows: híbrido NT kernel) y barras
  animadas de CPU, RAM y chunks
- **Cartel de ganador** con corona y el color del SO
- Leaderboard de la misión y **tabla de posiciones acumulada** con victorias
- **Consola de eventos** en vivo (registro cronológico de todo lo que pasa)

## 14. RESULTADOS EXPERIMENTALES REALES

Estos son datos medidos, no simulados. Usalos tal cual.

### Marcador acumulado (14 misiones con los 3 SO, código ya corregido)

| SO | Chunks | T. promedio | T. mínimo | T. máximo | CPU proc | CPU sist | RAM |
|---|---|---|---|---|---|---|---|
| macOS | 159 | 0.504 s | 0.122 s | 0.802 s | 10.0 % | 21.2 % | 144.2 MB |
| Windows | 70 | 1.386 s | 0.228 s | 3.781 s | 8.4 % | 18.6 % | 100.7 MB |
| Linux | 53 | 1.680 s | 0.364 s | 3.055 s | 6.6 % | 21.5 % | 92.1 MB |

**Victorias: macOS 14 — Windows 0 — Linux 0.**

### Lecturas que el informe debe desarrollar

1. **macOS domina**: ~2.7× más rápido que Windows y ~3.3× más rápido que Linux
   por chunk. Procesó 159 de 282 chunks (56 %) sin ninguna configuración manual:
   es el work-stealing actuando solo.
2. **Windows le gana a Linux** en esta prueba (1.386 s vs 1.680 s). Discutí que
   el resultado mide *esa máquina con ese SO*, no el SO en abstracto: son
   equipos con hardware distinto. Es la principal amenaza a la validez interna
   del experimento y hay que decirlo explícitamente.
3. **Varianza**: Windows tiene el rango más amplio (0.228 – 3.781 s). Relacionalo
   con la política del planificador, procesos en segundo plano y gestión térmica.
4. **RAM**: macOS usa la huella más alta (144 MB) y Linux la más baja (92 MB)
   para el mismo programa Python. Discutí asignadores de memoria y cómo cada SO
   contabiliza el RSS.
5. **CPU del proceso baja (6–10 %)**: es esperable, porque el valor está
   normalizado por núcleos lógicos y la tarea es monohilo. Explicá el cálculo.

> Antes de escribir, regenerá esta tabla con `python -m coordinator.ver_datos` y
> reemplazá los números si hubo corridas nuevas.

## 15. INFORME DE PRUEBAS Y DEPURACIÓN

Esta sección es la más valiosa del trabajo: documenta ocho defectos reales,
encontrados y corregidos con evidencia. Presentala como tabla y después
desarrollá los tres primeros en profundidad.

| # | Defecto | Causa raíz | Efecto observado | Corrección |
|---|---|---|---|---|
| 1 | Worker colgado en silencio | `muestreador.detener()` estaba después del `await` y no en un `finally`; si el cómputo lanzaba excepción nunca se llamaba, el muestreador giraba infinito y `asyncio.gather` no retornaba nunca | El nodo seguía conectado y enviando métricas con CPU 0 %, sin devolver jamás el `RESULT` | `try/finally` alrededor del cómputo; ante excepción se envía un `RESULT` con el error |
| 2 | Sistema trabado por completo | El coordinador no llevaba registro de los chunks en vuelo: `cola.pop(0)` y se perdía el rastro | Con un chunk perdido, `len(resultados) == total_chunks` nunca se cumplía; la fase quedaba en `corriendo` para siempre y el botón de arrancar dejaba de funcionar. Solo se salía reiniciando el coordinador | Diccionario `asignados`, reencolado, vigilante con timeout y límite de reintentos |
| 3 | Chunk perdido al fallar el envío | `asignar()` sacaba el chunk de la cola y después enviaba; si el envío fallaba, el chunk desaparecía | Pérdida silenciosa de trabajo | Reencolado en el `except` del envío |
| 4 | Worker fantasma | Una excepción distinta de `WebSocketDisconnect` mataba el handler sin sacar al worker del registro | Se le asignaban chunks a un WebSocket muerto | `try/except/finally` con limpieza garantizada |
| 5 | Reconexión destructiva | Al reconectarse con el mismo nombre, la sesión vieja al morir borraba del registro a la nueva | El worker quedaba conectado pero invisible | Se compara la identidad del WebSocket antes de borrar |
| 6 | Métricas nulas en SQLite | El worker nunca enviaba `pico_cpu_proc` / `pico_cpu_sys` / `pico_ram_mb` | Todas las columnas de CPU y RAM en `NULL`; el comparativo por SO salía vacío | El muestreador acumula picos y los adjunta al `RESULT` |
| 7 | Nodos rápidos con 0 % de CPU | El muestreo empezaba a los 0.5 s y los chunks del nodo rápido duraban 0.33 s | El SO más rápido aparecía con CPU y RAM en cero | Primera muestra a los 0.15 s y una muestra final garantizada |
| 8 | Tabla de resultados invisible | `mission_complete` y `ready_to_configure` se emiten con microsegundos de diferencia, y el handler del segundo limpiaba el estado de la misión | La tabla final aparecía y desaparecía al instante | Los resultados se limpian al **arrancar** la misión siguiente, no al terminar la anterior |

### Casos de prueba ejecutados y su resultado

| Escenario | Comportamiento anterior | Comportamiento corregido |
|---|---|---|
| Worker colgado indefinidamente | Misión trabada en 11/12 chunks | Timeout → reencolado → **12/12 completados** |
| Worker terminado con `kill -9` en pleno chunk | Chunk perdido | Reencolado inmediato → **18/18 completados** |
| Tres misiones consecutivas con un nodo colgado | Imposible: la fase quedaba trabada | **3/3 misiones completas** |
| Tres nodos sanos, tres misiones | Correcto | **30 chunks reparados 10/10/10**, cero reencolados |
| Excepción dentro del executor | Cuelgue infinito (>10 s sin responder) | Responde en **0.01 s** con el error reportado |

**Evidencia adicional para el informe:** en la base de datos, las corridas 6, 8,
9 y 10 quedaron registradas con **cero resultados**. Son misiones que se trabaron
con el defecto #2 antes de la corrección. Todas las corridas posteriores a la
corrección completaron el 100 % de sus chunks.

---

# ESTRUCTURA EXIGIDA DEL INFORME

1. **Portada** — título, curso, integrantes, profesor, fecha
2. **Resumen ejecutivo** (máx. 250 palabras)
3. **Introducción** — problema, objetivo general y específicos, alcance
4. **Marco teórico** — cómputo distribuido, planificación de procesos en los tres
   SO, concurrencia vs paralelismo, GIL, balanceo estático vs dinámico, IPC y sockets
5. **Arquitectura del sistema** — diagrama, componentes, decisiones de diseño y
   sus alternativas descartadas
6. **Protocolo de comunicación** — tabla de mensajes y diagrama de secuencia de
   una misión completa
7. **Diseño experimental** — las tres pruebas, las tres intensidades, la tabla de
   parámetros, la asimetría de `sort`, y las variables medidas
8. **Implementación** — coordinador, worker, muestreador, persistencia y dashboard
9. **Tolerancia a fallos** — el problema, el rediseño y los mecanismos
10. **Informe de pruebas y depuración** — los 8 defectos y los 5 escenarios
11. **Resultados** — tablas, y descripción textual de las gráficas que irán en las capturas
12. **Análisis y discusión** — las cinco lecturas de la sección 14, con las
    limitaciones metodológicas
13. **Conclusiones** — respuesta directa a cada objetivo específico
14. **Trabajo futuro** — mínimo cinco propuestas concretas
15. **Anexos** — comandos de despliegue, esquema SQL, guía de reproducción

# HUECOS PARA CAPTURAS DE PANTALLA

Insertá estos marcadores en su lugar exacto dentro del texto. Cada uno debe
llevar número de figura, un pie de figura descriptivo y, debajo, **un párrafo que
explique qué se está viendo** (el párrafo lo escribís vos; la imagen la pego yo).

```
[FIGURA 1 — CAPTURA: Panel principal, pantalla de espera con 0/3 y 2/3 equipos conectados]
[FIGURA 2 — CAPTURA: Panel principal, panel de configuración con los 3 tipos de prueba y las 3 intensidades]
[FIGURA 3 — CAPTURA: Panel principal, misión en curso — tarjetas de los 3 nodos con sparklines de CPU]
[FIGURA 4 — CAPTURA: Panel principal, tabla de resultado de la misión con medallas y ganador]
[FIGURA 5 — CAPTURA: Panel principal, tabla de reparto por nodo]
[FIGURA 6 — CAPTURA: Panel principal, marcador acumulado histórico]
[FIGURA 7 — CAPTURA: Interacción SO, topología coordinador→workers con las líneas activas]
[FIGURA 8 — CAPTURA: Interacción SO, barras de CPU/RAM/chunks de los 3 nodos en pleno cómputo]
[FIGURA 9 — CAPTURA: Interacción SO, cartel de ganador con la corona]
[FIGURA 10 — CAPTURA: Interacción SO, tabla de posiciones acumulada con victorias]
[FIGURA 11 — CAPTURA: Interacción SO, consola de eventos]
[FIGURA 12 — CAPTURA: Terminal del coordinador durante una misión, con el reparto de chunks]
[FIGURA 13 — CAPTURA: Terminal del coordinador mostrando un reencolado por timeout]
[FIGURA 14 — CAPTURA: Terminales de los 3 workers en paralelo]
[FIGURA 15 — CAPTURA: Salida de python -m coordinator.ver_datos]
```

Además, describí en el texto (sin generarlas) tres gráficas que conviene armar
desde el CSV exportado: barras de tiempo promedio por SO, líneas de CPU en el
tiempo por nodo, y torta del reparto de chunks.

# REGLAS DE ESTILO

- Español técnico, claro, sin relleno ni frases de manual.
- Nada de "en el mundo actual de la tecnología" ni introducciones genéricas.
- Cada afirmación cuantitativa debe venir de los datos de este prompt.
- Los fragmentos de código, cortos (máx. 15 líneas) y solo si aportan.
- Tablas en Markdown.
- Los nombres de archivo y funciones, en `monoespaciado`.
- Al discutir resultados, distinguí siempre entre lo que se midió y lo que se
  infiere.
- Reconocé explícitamente las limitaciones: hardware distinto entre nodos, una
  sola ejecución por configuración, red compartida, y el sesgo de que Python
  monohilo no aprovecha los múltiples núcleos.

Empezá por la portada y seguí en orden. No resumas secciones por falta de espacio:
si el informe queda largo, está bien.
