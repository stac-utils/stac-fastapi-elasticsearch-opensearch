FROM python:3.11-slim

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install gcc && \
    apt-get -y install build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -e ./stac_fastapi/core
RUN pip install --no-cache-dir -e ./stac_fastapi/sfeos_helpers
RUN pip install --no-cache-dir ./stac_fastapi/elasticsearch[server]

EXPOSE 8080

CMD ["uvicorn", "stac_fastapi.elasticsearch.app:app", "--host", "0.0.0.0", "--port", "8080"]
