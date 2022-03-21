# stac-fastapi-elasticsearch

Elasticsearch backend for stac-fastapi. 

**WIP** This backend is not yet stable (notice no releases yet), so use the pgstac backend instead.

For changes, see the [Changelog](CHANGELOG.md).

## Development Environment Setup

To install the classes in your local Python env, run:

```shell
cd stac_fastapi/elasticsearch
pip install -e .[dev]
```

### Pre-commit

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit run --all-files`
```


## Building

```shell
docker-compose build
```

## Running API on localhost:8083

```shell
docker-compose up
```

By default, docker-compose uses Elasticsearch 7.x. If you wish to use a different version, put the following in a 
file named `.env` in the same directory you run docker-compose from:

```shell
ELASTICSEARCH_VERSION=7.12.0
```

TBD: how to run this with 8.x with a password enabled and TLS.

To create a new Collection:

```shell
curl -X "POST" "http://localhost:8083/collections" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
  "id": "my_collection"
}'
```

Note: this "Collections Transaction" behavior is not part of the STAC API, but may be soon.

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