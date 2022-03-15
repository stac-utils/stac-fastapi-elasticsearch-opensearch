#!make
APP_HOST ?= 0.0.0.0
APP_PORT ?= 8080
EXTERNAL_APP_PORT ?= ${APP_PORT}

run_es = docker-compose \
	run \
	-p ${EXTERNAL_APP_PORT}:${APP_PORT} \
	-e PY_IGNORE_IMPORTMISMATCH=1 \
	-e APP_HOST=${APP_HOST} \
	-e APP_PORT=${APP_PORT} \
	app-elasticsearch

.PHONY: image
image:
	docker-compose build

.PHONY: docker-run
docker-run: image
	$(run_es)

.PHONY: docker-shell
docker-shell:
	$(run_es) /bin/bash

.PHONY: test
test:
	$(run_es) /bin/bash -c 'export && ./scripts/wait-for-it-es.sh elasticsearch:9200 && cd /app/stac_fastapi/elasticsearch/tests/ && pytest'
	#docker-compose down

.PHONY: run-database
run-database:
	docker-compose run --rm elasticsearch

.PHONY: pybase-install
pybase-install:
	pip install wheel && \
	pip install -e ./stac_fastapi/api[dev] && \
	pip install -e ./stac_fastapi/types[dev] && \
	pip install -e ./stac_fastapi/extensions[dev]

.PHONY: install
install: pybase-install
	pip install -e ./stac_fastapi/elasticsearch[dev,server]

.PHONY: docs-image
docs-image:
	docker-compose -f docker-compose.docs.yml \
		build

.PHONY: docs
docs: docs-image
	docker-compose -f docker-compose.docs.yml \
		run docs

.PHONY: ingest
ingest:
	python3 data_loader/data_loader.py
