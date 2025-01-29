FROM python:3.12-slim

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