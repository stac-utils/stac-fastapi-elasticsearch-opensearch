# stac-fastapi-elasticsearch

Elasticsearch backend for stac-fastapi. 

**WIP** This backend is not yet stable (notice no releases yet), so use the pgstac backend instead.

## Development Environment Setup

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit run --all-files`
```

```shell
cd stac_fastapi/elasticsearch
pip install .[dev]
```

## Building

```shell
docker-compose build
```

## Running API on localhost:8083

```shell
docker-compose up
```

## Testing

```shell
make test
```

## Ingest sample data

```shell
make ingest
```

## Elasticsearch Mappings

Mappings apply to search index, not source. 