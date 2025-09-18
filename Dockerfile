FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY README.md .
COPY stac_fastapi/opensearch/setup.py stac_fastapi/opensearch/
COPY stac_fastapi/core/setup.py stac_fastapi/core/
COPY stac_fastapi/sfeos_helpers/setup.py stac_fastapi/sfeos_helpers/


RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY stac_fastapi/ stac_fastapi/

RUN pip install --no-cache-dir ./stac_fastapi/core
RUN pip install --no-cache-dir ./stac_fastapi/sfeos_helpers
RUN pip install --no-cache-dir ./stac_fastapi/opensearch[server]

EXPOSE 8080

CMD ["uvicorn", "stac_fastapi.opensearch.app:app", "--host", "0.0.0.0", "--port", "8080"]
