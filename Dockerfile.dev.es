FROM python:3.10-slim

# update apt pkgs, and install build-essential for ciso8601
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# update certs used by Requests
ENV CURL_CA BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

# Copy the contents of common and elastic_search directories directly into /app
COPY ./stac_fastapi/common /app/stac_fastapi/common
COPY ./stac_fastapi/elastic_search /app/stac_fastapi/elastic_search

# Install dependencies
RUN pip install --no-cache-dir -e ./stac_fastapi/elastic_search[dev,server]
