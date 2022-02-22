#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}

run_es = docker-compose -f docker-compose.elasticsearch.yml \
				run \
				-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
				-e PY_IGNORE_IMPORTMISMATCH=1 \
				-e APP_HOST=${APP_HOST} \
				-e APP_PORT=${APP_PORT} \
				app-elasticsearch

.PHONY: es-image
es-image:
	docker-compose -f docker-compose.elasticsearch.yml build

.PHONY: docker-run-es
docker-run-es: es-image
	$(run_es)

.PHONY: docker-shell-es
docker-shell-es:
	$(run_es) /bin/bash

.PHONY: test-es
test-es:
	$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it.sh elasticsearch:9200 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'

.PHONY: run-es-database
run-es-database:
	docker-compose -f docker-compose.elasticsearch.yml run --rm elasticsearch

.PHONY: test
test: test-elasticsearch

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev]

.PHONY: es-install
es-install: pybase-install
	pip install -e ./stac_fastapi/elasticsearch[dev,server]

.PHONY: docs-image
docs-image:
	docker-compose -f docker-compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker-compose -f docker-compose.docs.yml \
		run docs
