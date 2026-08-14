#!/bin/bash
# Compila el dashboard Angular y copia el resultado a coordinator/static/,
# donde el coordinador lo sirve automáticamente en la raíz (http://IP:8000/).
#
# Uso:  ./build_dashboard.sh
#
# Requiere Node.js instalado. La primera vez instala las dependencias de
# Angular (npm install), lo cual puede tardar unos minutos.

set -e
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$AQUI/dashboard"

if [ ! -d node_modules ]; then
  echo ">> Instalando dependencias de Angular (solo la primera vez)..."
  npm install
fi

echo ">> Compilando el dashboard..."
npx ng build

echo ">> Copiando el build a coordinator/static/..."
rm -f "$AQUI/coordinator/static/"*.js "$AQUI/coordinator/static/index.html"
cp -r dist/browser/* "$AQUI/coordinator/static/"

echo ">> Listo. El dashboard se servirá en http://<IP-del-coordinador>:8000/"
