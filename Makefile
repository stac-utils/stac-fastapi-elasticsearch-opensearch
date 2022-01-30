#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}
run_mongo = docker-compose -f docker-compose.mongo.yml \
				run \
				-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
				-e PY_IGNORE_IMPORTMISMATCH=1 \
				-e APP_HOST=${APP_HOST} \
				-e APP_PORT=${APP_PORT} \
				app-mongo

.PHONY: mongo-image
mongo-image:
	docker-compose -f docker-compose.mongo.yml build

.PHONY: docker-run-mongo
docker-run-mongo: mongo-image
	$(run_mongo)

.PHONY: docker-shell-mongo
docker-shell-mongo:
	$(run_mongo) /bin/bash

.PHONY: test-mongo
test-mongo:
	$(run_mongo) /bin/bash -c 'export && cd /app/stac_fastapi/mongo/tests/ && pytest'

.PHONY: run-mongo-database
run-mongo-database:
	docker-compose -f docker-compose.mongo.yml run --rm mongo_db

.PHONY: test
test: test-sqlalchemy test-pgstac test-mongo

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev]

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
