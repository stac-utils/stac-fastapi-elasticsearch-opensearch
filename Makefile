#!make
APP_HOST ?= 0.0.0.0
ES_APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}

ES_APP_PORT ?= 8080
ES_HOST ?= docker.for.mac.localhost
ES_PORT ?= 9200

OS_APP_PORT ?= 8082
ES_HOST ?= docker.for.mac.localhost
OS_PORT ?= 9202

run_es = docker-compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${ES_APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${ES_APP_PORT} \
	app-elasticsearch

run_os = docker-compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${OS_APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${OS_APP_PORT} \
	app-opensearch

.PHONY: image-deploy
image-deploy:
	docker build -f Dockerfile.deploy -t stac-fastapi-elasticsearch:latest .

.PHONY: run-deploy-locally
run-deploy-locally:
	 docker run -it -p 8080:8080 \
		-e ES_HOST=${ES_HOST} \
		-e ES_PORT=${ES_PORT} \
		-e ES_USER=${ES_USER} \
		-e ES_PASS=${ES_PASS} \
		stac-fastapi-elasticsearch:latest

.PHONY: image-dev
image-dev:
	docker-compose build

.PHONY: docker-run
docker-run: image-dev
	$(run_es)

.PHONY: docker-shell
docker-shell:
	$(run_es) /bin/bash

.PHONY: test-elasticsearch
test:
	-$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh elasticsearch:9200 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'
	docker-compose down

.PHONY: test-opensearch
test-opensearch:
	-$(run_os) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh opensearch:9202 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'
	docker-compose down

.PHONY: test
test:
	-$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh elasticsearch:9200 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'
	docker-compose down

	-$(run_os) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh opensearch:9202 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'
	docker-compose down

.PHONY: run-database-es
run-database-es:
	docker-compose run --rm elasticsearch

.PHONY: run-database-os
run-database-os:
	docker-compose run --rm opensearch

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev]

.PHONY: install
install: pybase-install
	pip install -e ./stac_fastapi/elasticsearch[dev,server]

.PHONY: ingest
ingest:
	python3 data_loader/data_loader.py
