#!/bin/bash
function validate_elasticsearch {
  export ES_HOST=${ES_HOST:-localhost}
  export ES_PORT=${ES_PORT:-9200}
  health=$(curl -s -o /dev/null -w '%{http_code}' "http://${ES_HOST}:${ES_PORT}/_cluster/health")
  if [ "$health" -eq 200 ]; then
    return 0
  else
    return 1
  fi
}

if [ "${RUN_LOCAL_ES}" = "1" ]; then
  echo "starting elasticsearch"
  /usr/share/elasticsearch/bin/elasticsearch &

  echo "wait for es to be ready"
  until validate_elasticsearch; do
    echo -n "."
    sleep 5
  done
  echo "Elasticsearch is up"
fi

echo "start stac-fastapi-es"
exec uvicorn stac_fastapi.elasticsearch.app:app \
  --host "${APP_HOST}" \
  --port "${APP_PORT}" \
  --workers "${WEB_CONCURRENCY}" \
  --reload