# STAC FastAPI Elasticsearch (sfes)

## Elasticsearch backend for stac-fastapi
   
#### Join our [Gitter](https://gitter.im/stac-fastapi-elasticsearch/community) page

#### Check out the public Postman documentation [Postman](https://documenter.getpostman.com/view/12888943/2s8ZDSdRHA)

#### Check out the examples folder for deployment options, ex. running sfes from pip in docker

#### For changes, see the [Changelog](CHANGELOG.md)


## Development Environment Setup

To install the classes in your local Python env, run:

```shell
pip install -e 'stac_fastapi/elasticsearch[dev]'
```

### Pre-commit

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit run --all-files
```


## Building

```shell
docker-compose build
```
  
## Running API on localhost:8080

```shell
docker-compose up
```

By default, docker-compose uses Elasticsearch 8.x and OpenSearch 2.11.1. 
If you wish to use a different version, put the following in a 
file named `.env` in the same directory you run docker-compose from:

```shell
ELASTICSEARCH_VERSION=7.17.1
OPENSEARCH_VERSION=2.11.0
```
The most recent 7.x versions should also work. See the [opensearch-py docs](https://github.com/opensearch-project/opensearch-py/blob/main/COMPATIBILITY.md) for compatibility information.

To create a new Collection:

```shell
curl -X "POST" "http://localhost:8080/collections" \
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
curl -X "GET" "http://localhost:8080/collections?limit=1&token=example_token"
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
    

## Managing Elasticsearch Indices

This section covers how to create a snapshot repository and then create and restore snapshots with this.

Create a snapshot repository. This puts the files in the `elasticsearch/snapshots` in this git repo clone, as
the elasticsearch.yml and docker-compose files create a mapping from that directory to 
`/usr/share/elasticsearch/snapshots` within the Elasticsearch container and grant permissions on using it.

```shell
curl -X "PUT" "http://localhost:9200/_snapshot/my_fs_backup" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
            "type": "fs",
            "settings": {
                "location": "/usr/share/elasticsearch/snapshots/my_fs_backup"
            }
}'
```

The next step is to create a snapshot of one or more indices into this snapshot repository.  This command creates
a snapshot named `my_snapshot_2` and waits for the action to be completed before returning. This can also be done
asynchronously, and queried for status. The `indices` parameter determines which indices are snapshotted, and
can include wildcards.

```shell
curl -X "PUT" "http://localhost:9200/_snapshot/my_fs_backup/my_snapshot_2?wait_for_completion=true" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
  "metadata": {
    "taken_because": "dump of all items",
    "taken_by": "pvarner"
  },
  "include_global_state": false,
  "ignore_unavailable": false,
  "indices": "items_my-collection"
}'
```

To see the status of this snapshot:

```shell
curl http://localhost:9200/_snapshot/my_fs_backup/my_snapshot_2
```

To see all the snapshots:

```shell
curl http://localhost:9200/_snapshot/my_fs_backup/_all
```

To restore a snapshot, run something similar to the following. This specific command will restore any indices that
match `items_*` and rename them so that the new index name will be suffixed with `-copy`.

```shell
curl -X "POST" "http://localhost:9200/_snapshot/my_fs_backup/my_snapshot_2/_restore?wait_for_completion=true" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
  "include_aliases": false,
  "include_global_state": false,
  "ignore_unavailable": true,
  "rename_replacement": "items_$1-copy",
  "indices": "items_*",
  "rename_pattern": "items_(.+)"
}'
```

Now the item documents have been restored in to the new index (e.g., `my-collection-copy`), but the value of the
`collection` field in those documents is still the original value of `my-collection`. To update these to match the
new collection name, run the following Elasticsearch Update By Query command, substituting the old collection name
into the term filter and the new collection name into the script parameter:

```shell
curl -X "POST" "http://localhost:9200/items_my-collection-copy/_update_by_query" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d $'{
    "query": {
        "match_all": {}
},
  "script": {
    "lang": "painless",
    "params": {
      "collection": "my-collection-copy"
    },
    "source": "ctx._source.collection = params.collection"
  }
}'
```

Then, create a new collection through the api with the new name for each of the restored indices:

```shell
curl -X "POST" "http://localhost:8080/collections" \
     -H 'Content-Type: application/json' \
     -d $'{
  "id": "my-collection-copy"
}'
```

Voila! You have a copy of the collection now that has a resource URI (`/collections/my-collection-copy`) and can be
correctly queried by collection name.
