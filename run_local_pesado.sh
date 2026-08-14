#!/bin/bash
# Prueba local de OS Grid — variante PESADA (3 chunks grandes): levanta el
# coordinador pesado y dos workers en localhost, procesa toda la cola y
# muestra el resumen. Sirve para validar la config antes de correrla entre
# las 3 máquinas reales por Tailscale.
#
# Uso:  ./run_local_pesado.sh
#
# Requiere el entorno virtual activo (o las dependencias instaladas):
#     source .venv/bin/activate

set -e
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$AQUI"

echo ">> Limpiando procesos previos..."
pkill -f "uvicorn coordinator.main" 2>/dev/null || true
pkill -f "agent.py" 2>/dev/null || true
sleep 1

echo ">> Levantando coordinador (prueba pesada)..."
uvicorn coordinator.main_pesado:app --host 127.0.0.1 --port 8000 --log-level warning &
COORD=$!

# Esperar a que el coordinador responda
echo ">> Esperando al coordinador..."
for i in $(seq 1 15); do
  if curl -s http://127.0.0.1:8000/ >/dev/null 2>&1; then
    echo ">> Coordinador listo."
    break
  fi
  sleep 1
done

echo ">> Lanzando tres workers (MIN_WORKERS=3 en coordinator/main_pesado.py)..."
( cd worker && python agent.py localhost NodoA ) &
W1=$!
( cd worker && python agent.py localhost NodoB ) &
W2=$!
( cd worker && python agent.py localhost NodoC ) &
W3=$!

# Esperar a que los tres workers terminen
wait $W1 $W2 $W3

echo ">> Workers terminados. Cerrando coordinador..."
sleep 1
kill $COORD 2>/dev/null || true
echo ">> Listo."
