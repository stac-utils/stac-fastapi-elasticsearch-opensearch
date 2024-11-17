FROM debian:bookworm-slim AS base

ENV ENVIRONMENT="local"
ENV APP_HOST="0.0.0.0"
ENV APP_PORT="8080"
ENV WEB_CONCURRENCY=10
ENV ES_USE_SSL=false
ENV ES_VERIFY_CERTS=false
ENV STAC_FASTAPI_RATE_LIMIT="200/minute"
ENV RUN_LOCAL_ES=0

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
    curl --silent --fail http://${APP_HOST}:${APP_PORT}/api.html || exit 1

EXPOSE $APP_PORT

USER elasticsearch
ENTRYPOINT ["/entrypoint.sh"]
