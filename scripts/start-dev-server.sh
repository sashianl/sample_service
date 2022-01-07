
# See if port is in use.
USING_PORT=$(lsof -i ":${DS_PORT:-5000}")
if [ -n "$USING_PORT" ]; then
  echo "Another application is using port '${DS_PORT}' (see below)"
  echo ""
  echo "${USING_PORT}"
  echo ""
  echo "Please set PORT to another value and try again."
  echo ""
  exit 1
fi

docker compose -f dev/docker-compose.yml up
