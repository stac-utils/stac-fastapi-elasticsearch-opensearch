FROM python:3.12-slim

ENV STAC_FASTAPI_TITLE="stac-fastapi-opensearch"
ENV STAC_FASTAPI_DESCRIPTION="A STAC FastAPI with an Opensearch backend"
ENV STAC_FASTAPI_VERSION="3.0.0a2"
ENV APP_HOST="0.0.0.0"
ENV APP_PORT="8082"
ENV RELOAD="true"
ENV ENVIRONMENT="local"
ENV WEB_CONCURRENCY="10"
ENV ES_HOST="localhost"
ENV ES_PORT="9202"
ENV ES_USE_SSL="false"
ENV ES_VERIFY_CERTS="false"
ENV BACKEND="opensearch"
ENV STAC_FASTAPI_RATE_LIMIT="200/minute"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . /app/

RUN pip3 install --no-cache-dir -e ./stac_fastapi/core && \
    pip3 install --no-cache-dir ./stac_fastapi/opensearch[server]

USER root

CMD ["python", "-m", "stac_fastapi.opensearch.app"]