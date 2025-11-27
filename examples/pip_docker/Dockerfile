FROM python:3.11-slim


# update apt pkgs, and install build-essential for ciso8601
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# update certs used by Requests
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

COPY . /app

RUN pip install stac-fastapi.elasticsearch==2.2.0