# stac-fastapi-mongo

## Mongo backend for the stac-fastapi project built on top of the [sfeos](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch) core api library. 

- Note: This is presently in development and, for now, is a proof of concept project showing that other databases can be plugged into the [sfeos](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch) core api library, besides for elasticsearch and opensearch. 
   

To install from PyPI:

```shell
pip install stac_fastapi.mongo
```

#### For changes, see the [Changelog](CHANGELOG.md)


## Development Environment Setup

To install the classes in your local Python env, run:

```shell
pip install -e 'stac_fastapi/mongo[dev]'
```


### Pre-commit

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit run --all-files
```

## Build Elasticsearh API backend

```shell
docker-compose up mongo
docker-compose build app-mongo
```
  
## Running Mongo API on localhost:8084

```shell
docker-compose up app-mongo
```

To create a new Collection:

```shell
curl -X "POST" "http://localhost:8084/collections" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
  "id": "my_collection"
}'
```

Note: this "Collections Transaction" behavior is not part of the STAC API, but may be soon.  


## Collection pagination

The collections route handles optional `limit` and `token` parameters. The `links` field that is
returned from the `/collections` route contains a `next` link with the token that can be used to 
get the next page of results.
   
```shell
curl -X "GET" "http://localhost:8084/collections?limit=1&token=example_token"
```

## Testing

```shell
make test
```


## Ingest sample data

```shell
make ingest
```
