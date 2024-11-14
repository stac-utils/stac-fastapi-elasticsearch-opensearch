#!/bin/bash

function validate_elasticsearch {
  health=$(curl -s -o /dev/null -w '%{http_code}' "http://${ES_HOST}:${ES_PORT}/_cluster/health")
  if [ "$health" -eq 200 ]; then
    return 0
  else
    return 1
  fi
}

echo "start es"
/usr/share/elasticsearch/bin/elasticsearch &

echo "wait for es to be ready"
until validate_elasticsearch; do
  echo -n "."
  sleep 5
done
echo "Elasticsearch is up"

echo "start stac-fastapi-es"
exec uvicorn stac_fastapi.elasticsearch.app:app --host "${APP_HOST}" --port "${APP_PORT}" --reload
