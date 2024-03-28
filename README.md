# stac-fastapi-elasticsearch-opensearch (sfeos)

## Elasticsearch and Opensearch backends for the stac-fastapi project  
  
  [![PyPI version](https://badge.fury.io/py/stac-fastapi.elasticsearch.svg)](https://badge.fury.io/py/stac-fastapi.elasticsearch)  
  
- Our Api core library can be used to create custom backends. See [stac-fastapi-mongo](https://github.com/Healy-Hyperspatial/stac-fastapi-mongo) for a working example.  
- Reach out on our [Gitter](https://app.gitter.im/#/room/#stac-fastapi-elasticsearch_community:gitter.im) channel or feel free to add to our [Discussions](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/discussions) page here on github.
- There is [Postman](https://documenter.getpostman.com/view/12888943/2s8ZDSdRHA) documentation here for examples on how to run some of the API routes locally - after starting the elasticsearch backend via the docker-compose.yml file.
- The `/examples` folder shows an example of running stac-fastapi-elasticsearch from PyPI in docker without needing any code from the repository. There is also a Postman collection here that you can load into Postman for testing the API routes. 

### To install from PyPI:

```shell
pip install stac_fastapi.elasticsearch
```
or   
```
pip install stac_fastapi.opensearch
```

#### For changes, see the [Changelog](CHANGELOG.md)


## Development Environment Setup

To install the classes in your local Python env, run:

```shell
pip install -e 'stac_fastapi/elasticsearch[dev]'
```

or

```shell
pip install -e 'stac_fastapi/opensearch[dev]'
```


### Pre-commit

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit run --all-files
```

## Build Elasticsearh API backend

```shell
docker-compose up elasticsearch
docker-compose build app-elasticsearch
```
  
## Running Elasticsearh API on localhost:8080

```shell
docker-compose up app-elasticsearch
```

By default, docker-compose uses Elasticsearch 8.x and OpenSearch 2.11.1. 
If you wish to use a different version, put the following in a 
file named `.env` in the same directory you run docker-compose from:

```shell
ELASTICSEARCH_VERSION=7.17.1
OPENSEARCH_VERSION=2.11.0
```
The most recent Elasticsearch 7.x versions should also work. See the [opensearch-py docs](https://github.com/opensearch-project/opensearch-py/blob/main/COMPATIBILITY.md) for compatibility information.

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

## Ingesting Sample Data   

```shell
cd data_loader   
python3 data_loader.py --base-url http://localhost:8080
```  

## Testing

```shell
make test
```
Test against OpenSearch only

```shell
make test-opensearch
```

Test against Elasticsearch only

```shell
make test-elasticsearch
```  

## Elasticsearch Mappings

Mappings apply to search index, not source. The mappings are stored in index templates on application startup. 
These templates will be used implicitly when creating new Collection and Item indices.
    

## Managing Elasticsearch Indices
### Snapshots

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

### Reindexing
This section covers how to reindex documents stored in Elasticsearch/OpenSearch. 
A reindex operation might be useful to apply changes to documents or to correct dynamically generated mappings.

The index templates will make sure that manually created indices will also have the correct mappings and settings.

In this example, we will make a copy of an existing Item index `items_my-collection-000001` but change the Item identifier to be lowercase.

```shell
curl -X "POST" "http://localhost:9200/_reindex" \
  -H 'Content-Type: application/json' \
  -d $'{
    "source": {
      "index": "items_my-collection-000001"
    }, 
    "dest": {
      "index": "items_my-collection-000002"
    },
    "script": {
      "source": "ctx._source.id = ctx._source.id.toLowerCase()",
      "lang": "painless"
    }
  }'
```

If we are happy with the data in the newly created index, we can move the alias `items_my-collection` to the new index `items_my-collection-000002`.
```shell
curl -X "POST" "http://localhost:9200/_aliases" \
  -h 'Content-Type: application/json' \
  -d $'{
    "actions": [
      {
        "remove": {
          "index": "*",
          "alias": "items_my-collection"
        }
      },
      {
        "add": {
          "index": "items_my-collection-000002",
          "alias": "items_my-collection"
        }
      }
    ]
  }'
```

The modified Items with lowercase identifiers will now be visible to users accessing `my-collection` in the STAC API.