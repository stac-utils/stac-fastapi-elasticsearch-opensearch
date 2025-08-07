#!make
APP_HOST ?= 0.0.0.0
EXTERNAL_APP_PORT ?= 8080

ES_APP_PORT ?= 8080
OS_APP_PORT ?= 8082

ES_HOST ?= docker.for.mac.localhost
ES_PORT ?= 9200

run_es = docker compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${ES_APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${ES_APP_PORT} \
	app-elasticsearch

run_os = docker compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${OS_APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${OS_APP_PORT} \
	app-opensearch

.PHONY: image-deploy-es
image-deploy-es:
	docker build -f dockerfiles/Dockerfile.dev.es -t stac-fastapi-elasticsearch:latest .

.PHONY: image-deploy-os
image-deploy-os:
	docker build -f dockerfiles/Dockerfile.dev.os -t stac-fastapi-opensearch:latest .

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
	docker compose build

.PHONY: docker-run-es
docker-run-es: image-dev
	$(run_es)

.PHONY: docker-run-os
docker-run-os: image-dev
	$(run_os)

.PHONY: docker-shell-es
docker-shell-es:
	$(run_es) /bin/bash

.PHONY: docker-shell-os
docker-shell-os:
	$(run_os) /bin/bash

.PHONY: test-elasticsearch
test-elasticsearch:
	-$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh elasticsearch:9200 && cd stac_fastapi/tests/ && pytest'
	docker compose down

.PHONY: test-opensearch
test-opensearch:
	-$(run_os) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh opensearch:9202 && cd stac_fastapi/tests/ && pytest'
	docker compose down

.PHONY: test-datetime-filtering-es
test-datetime-filtering-es:
	-$(run_es) /bin/bash -c 'export ENABLE_DATETIME_INDEX_FILTERING=true && ./scripts/wait-for-it-es.sh elasticsearch:9200 && cd stac_fastapi/tests/ && pytest -s --cov=stac_fastapi --cov-report=term-missing -m datetime_filtering'
	docker compose down

.PHONY: test-datetime-filtering-os
test-datetime-filtering-os:
	-$(run_os) /bin/bash -c 'export ENABLE_DATETIME_INDEX_FILTERING=true && ./scripts/wait-for-it-es.sh opensearch:9202 && cd stac_fastapi/tests/ && pytest -s --cov=stac_fastapi --cov-report=term-missing -m datetime_filtering'
	docker compose down

.PHONY: test
test: test-elasticsearch test-datetime-filtering-es test-opensearch test-datetime-filtering-os

.PHONY: run-database-es
run-database-es:
	docker compose run --rm elasticsearch

.PHONY: run-database-os
run-database-os:
	docker compose run --rm opensearch

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev] && \
	pip install -e ./stac_fastapi/core && \
	pip install -e ./stac_fastapi/sfeos_helpers

.PHONY: install-es
install-es: pybase-install
	pip install -e ./stac_fastapi/elasticsearch[dev,server]

.PHONY: install-os
install-os: pybase-install
	pip install -e ./stac_fastapi/opensearch[dev,server]

.PHONY: docs-image
docs-image:
	docker compose -f compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker compose -f compose.docs.yml \
		run docs