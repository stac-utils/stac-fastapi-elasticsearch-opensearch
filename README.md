# stac-fastapi-elasticsearch

Elasticsearch backend for stac-fastapi. 

**WIP** This backend is not yet stable (notice no releases yet), so use the pgstac backend instead.

## Development Environment Setup

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```
pre-commit run --all-files`
```

## Building

```
docker-compose build
```

## Running API on localhost:8083

```
docker-compose up
```

## Testing

```
make test
```
