#!/bin/bash
function validate_opensearch {
  export ES_HOST=${ES_HOST:-localhost}
  export ES_PORT=${ES_PORT:-9202}
  health=$(curl -s -o /dev/null -w '%{http_code}' "http://${ES_HOST}:${ES_PORT}/_cluster/health")
  if [ "$health" -eq 200 ]; then
    return 0
  else
    return 1
  fi
}

echo "start opensearch"
/usr/share/opensearch/bin/opensearch &

if [ "${RUN_LOCAL_OS}" = "1" ]; then
  echo "starting opensearch"
  /usr/share/opensearch/bin/opensearch &

  echo "wait for os to be ready"
  until validate_opensearch; do
    echo -n "."
    sleep 5
  done
  echo "opensearch is up"
fi

echo "start stac-fastapi-os"
exec uvicorn stac_fastapi.opensearch.app:app \
  --host "${APP_HOST}" \
  --port "${APP_PORT}" \
  --workers "${WEB_CONCURRENCY}" \
  --reload
