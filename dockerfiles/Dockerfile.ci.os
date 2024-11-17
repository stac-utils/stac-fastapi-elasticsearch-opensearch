FROM debian:bookworm-slim AS base

ENV ENVIRONMENT="local"
ENV APP_HOST="0.0.0.0"
ENV APP_PORT="8080"
ENV WEB_CONCURRENCY=10
ENV ES_USE_SSL=false
ENV ES_VERIFY_CERTS=false
ENV STAC_FASTAPI_RATE_LIMIT="200/minute"
ENV RUN_LOCAL_OS=0

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
RUN groupadd -g 1000 opensearch && \
    useradd -u 1000 -g opensearch -s /bin/bash -m opensearch

# opensearch binaries and libraries
COPY --from=opensearchproject/opensearch:2.11.1 /usr/share/opensearch /usr/share/opensearch

# ser ownership
RUN chown -R opensearch:opensearch /usr/share/opensearch

WORKDIR /app
COPY . /app

# stac-fastapi-os installation
RUN pip3 install --no-cache-dir  --break-system-packages -e ./stac_fastapi/core && \
    pip3 install --no-cache-dir  --break-system-packages ./stac_fastapi/opensearch[server]

COPY opensearch/config/opensearch.yml /usr/share/opensearch/config/opensearch.yml

COPY dockerfiles/entrypoint-os.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx1g" \
    PATH="/usr/share/opensearch/bin:${PATH}"

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD \
    curl --silent --fail http://${APP_HOST}:${APP_PORT}/api.html || exit 1

EXPOSE $APP_PORT

USER opensearch
ENTRYPOINT ["/entrypoint.sh"]
