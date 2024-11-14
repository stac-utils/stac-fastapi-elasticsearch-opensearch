FROM debian:bookworm-slim AS base

ARG STAC_FASTAPI_TITLE
ARG STAC_FASTAPI_DESCRIPTION
ARG STAC_FASTAPI_VERSION
ARG APP_HOST
ARG APP_PORT
ARG RELOAD
ARG ENVIRONMENT
ARG WEB_CONCURRENCY
ARG ES_HOST
ARG ES_PORT
ARG ES_USE_SSL
ARG ES_VERIFY_CERTS
ARG BACKEND

ENV STAC_FASTAPI_TITLE=${STAC_FASTAPI_TITLE}
ENV STAC_FASTAPI_DESCRIPTION=${STAC_FASTAPI_DESCRIPTION}
ENV STAC_FASTAPI_VERSION=${STAC_FASTAPI_VERSION}
ENV APP_HOST=${APP_HOST}
ENV APP_PORT=${APP_PORT}
ENV RELOAD=${RELOAD}
ENV ENVIRONMENT=${ENVIRONMENT}
ENV WEB_CONCURRENCY=${WEB_CONCURRENCY}
ENV ES_HOST=${ES_HOST}
ENV ES_PORT=${ES_PORT}
ENV ES_USE_SSL=${ES_USE_SSL}
ENV ES_VERIFY_CERTS=${ES_VERIFY_CERTS}
ENV BACKEND=${BACKEND}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    curl \
    python3 \
    python3-pip \
    python3-venv \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# set non-root user
RUN groupadd -g 1000 elasticsearch && \
    useradd -u 1000 -g elasticsearch -s /bin/bash -m elasticsearch

# elasticsearch binaries and libraries
COPY --from=docker.elastic.co/elasticsearch/elasticsearch:8.11.0 /usr/share/elasticsearch /usr/share/elasticsearch

# ser ownership
RUN chown -R elasticsearch:elasticsearch /usr/share/elasticsearch

WORKDIR /app
COPY . /app

# stac-fastapi-es installation
RUN pip3 install --no-cache-dir  --break-system-packages -e ./stac_fastapi/core && \
    pip3 install --no-cache-dir  --break-system-packages ./stac_fastapi/elasticsearch[server]

COPY elasticsearch/config/elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml

COPY dockerfiles/entrypoint-es.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


ENV ES_JAVA_OPTS="-Xms512m -Xmx1g" \
    PATH="/usr/share/elasticsearch/bin:${PATH}"


HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD \
    curl --silent --fail http://${ES_HOST}:${ES_PORT}/_cluster/health || exit 1 && \
    curl --silent --fail http://${APP_HOST}:${APP_PORT}/api.html || exit 1

EXPOSE $APP_PORT $ES_PORT

USER elasticsearch
ENTRYPOINT ["/entrypoint.sh"]
