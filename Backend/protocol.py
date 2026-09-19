"""
Protocolo de mensajes de OS Grid.

Tipos de mensaje que viajan por el WebSocket, en formato JSON.
Este archivo es el "contrato" compartido entre el coordinador y los workers,
para que no haya ambigüedad sobre qué campos lleva cada mensaje.

    WORKER  ->  COORDINADOR
        REGISTER    el worker se presenta al conectarse
        RESULT      el worker terminó un chunk y reporta el resultado
        METRICS     (Fase 3) métricas de CPU/RAM mientras procesa

    COORDINADOR  ->  WORKER
        REGISTERED  confirmación de registro
        TASK_ASSIGN el coordinador le da un chunk para procesar
        NO_MORE     ya no quedan chunks, el worker puede cerrar
"""

# Worker -> Coordinador
REGISTER = "REGISTER"
RESULT = "RESULT"
METRICS = "METRICS"

# Coordinador -> Worker
REGISTERED = "REGISTERED"
TASK_ASSIGN = "TASK_ASSIGN"
NO_MORE = "NO_MORE"


def msg(type_, **fields):
    """Construye un mensaje como diccionario listo para serializar a JSON."""
    return {"type": type_, **fields}
