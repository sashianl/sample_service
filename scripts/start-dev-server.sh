
# See if port is in use.
USING_PORT=$(lsof -i ":${DC_PORT:-5000}")
if [ -n "$USING_PORT" ]; then
  echo "Another application is using port '${DC_PORT:-5000}' (see below)"
  echo ""
  echo "${USING_PORT}"
  echo ""
  echo "Please set PORT to another value and try again."
  echo ""
  exit 1
fi

if [ "$DC_DETACH" == "yes" ]; then
  docker compose -f dev/docker-compose.yml up --detach
else
  docker compose -f dev/docker-compose.yml up
fi
