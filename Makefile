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

run_mongo = docker-compose -f docker-compose.mongo.yml \
				run \
				-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
				-e PY_IGNORE_IMPORTMISMATCH=1 \
				-e APP_HOST=${APP_HOST} \
				-e APP_PORT=${APP_PORT} \
				app-mongo

.PHONY: es-image
es-image:
	docker-compose -f docker-compose.elasticsearch.yml build

.PHONY: mongo-image
mongo-image:
	docker-compose -f docker-compose.mongo.yml build

.PHONY: docker-run-es
docker-run-es: es-image
	$(run_es)

.PHONY: docker-run-mongo
docker-run-mongo: mongo-image
	$(run_mongo)

.PHONY: docker-shell-es
docker-shell-es:
	$(run_es) /bin/bash

.PHONY: docker-shell-mongo
docker-shell-mongo:
	$(run_mongo) /bin/bash

.PHONY: test-es
test-es:
	$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it.sh elasticsearch:9200 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'

.PHONY: test-mongo
test-mongo:
	$(run_mongo) /bin/bash -c 'export && cd /app/stac_fastapi/mongo/tests/ && pytest'

.PHONY: run-es-database
run-es-database:
	docker-compose -f docker-compose.elasticsearch.yml run --rm elasticsearch

.PHONY: run-mongo-database
run-mongo-database:
	docker-compose -f docker-compose.mongo.yml run --rm mongo_db

.PHONY: test
test: test-elasticsearch test-mongo

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev]

.PHONY: es-install
es-install: pybase-install
	pip install -e ./stac_fastapi/elasticsearch[dev,server]

.PHONY: mongo-install
mongo-install: pybase-install
	pip install -e ./stac_fastapi/mongo[dev,server]

.PHONY: docs-image
docs-image:
	docker-compose -f docker-compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker-compose -f docker-compose.docs.yml \
		run docs
