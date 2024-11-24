FROM python:3.12-slim

ENV APP_HOST="0.0.0.0"
ENV APP_PORT="8080"
ENV WEB_CONCURRENCY="10"
ENV RELOAD="true"
ENV ES_HOST="localhost"
ENV ES_PORT="9200"
ENV ES_USE_SSL="false"
ENV ES_VERIFY_CERTS="false"
ENV STAC_FASTAPI_TITLE="stac-fastapi-elasticsearch"
ENV STAC_FASTAPI_DESCRIPTION="A STAC FastAPI with an Elasticsearch backend"
ENV STAC_FASTAPI_VERSION="2.1"
ENV ENVIRONMENT="local"
ENV BACKEND="elasticsearch"
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
    pip3 install --no-cache-dir ./stac_fastapi/elasticsearch[server]

USER root

CMD ["python", "-m", "stac_fastapi.elasticsearch.app"]