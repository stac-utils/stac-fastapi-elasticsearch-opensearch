#!make
APP_HOST ?= 0.0.0.0
EXTERNAL_APP_PORT ?= ${APP_PORT}

MONGO_APP_PORT ?= 8084
MONGO_HOST ?= docker.for.mac.localhost
MONGO_PORT ?= 27017

run_mongo = docker-compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${MONGO_APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${MONGO_APP_PORT} \
	app-mongo

.PHONY: image-deploy-mongo
image-deploy-mongo:
	docker build -f dockerfiles/Dockerfile.dev.mongo -t stac-fastapi-mongo:latest .


.PHONY: run-deploy-locally
run-deploy-locally:
	 docker run -it -p 8084:8084 \
		-e ES_HOST=${MONGO_HOST} \
		-e ES_PORT=${MONGO_PORT} \
		-e ES_USER=${MONGO_USER} \
		-e ES_PASS=${MONGO_PASS} \
		stac-fastapi-mongo:latest

.PHONY: image-dev
image-dev:
	docker-compose build

.PHONY: docker-run-mongo
docker-run-mongo: image-dev
	$(run_mongo)

.PHONY: docker-shell-mongo
docker-shell-mongo:
	$(run_mongo) /bin/bash


.PHONY: test-mongo
test-mongo:
	-$(run_mongo) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh mongo:27017 && cd stac_fastapi/tests/ && pytest'
	docker-compose down

.PHONY: test
test:
	-$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh mongo:27017 && cd stac_fastapi/tests/ && pytest'
	docker-compose down

.PHONY: run-database-mongo
run-database-mongo:
	docker-compose run --rm mongo

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev] && \
	pip install -e ./stac_fastapi/core

.PHONY: install-mongo
install-es: pybase-install
	pip install -e ./stac_fastapi/mongo[dev,server]

.PHONY: ingest
ingest:
	python3 data_loader/data_loader.py
