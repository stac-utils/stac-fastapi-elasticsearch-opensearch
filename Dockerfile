FROM python:3.14-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY README.md .
COPY stac_fastapi/core/pyproject.toml stac_fastapi/core/
COPY stac_fastapi/sfeos_helpers/pyproject.toml stac_fastapi/sfeos_helpers/
COPY stac_fastapi/opensearch/pyproject.toml stac_fastapi/opensearch/
COPY stac_fastapi/scripts/

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY stac_fastapi/ stac_fastapi/

RUN pip install --no-cache-dir ./stac_fastapi/core[redis]
RUN pip install --no-cache-dir ./stac_fastapi/sfeos_helpers
RUN pip install --no-cache-dir ./stac_fastapi/opensearch[server,redis]

EXPOSE 8080

CMD ["uvicorn", "stac_fastapi.opensearch.app:app", "--host", "0.0.0.0", "--port", "8080"]