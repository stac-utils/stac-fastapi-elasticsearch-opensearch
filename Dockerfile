FROM python:3.8-slim as base

FROM base as builder
# Any python libraries that require system libraries to be installed will likely
# need the following packages in order to build
RUN apt-get update && \
    apt-get install -y build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

ARG install_dev_dependencies=true

WORKDIR /app

COPY . /app

ENV PATH=$PATH:/install/bin

RUN mkdir -p /install
RUN pip install --no-cache-dir -e ./stac_fastapi/elasticsearch[dev,server]
