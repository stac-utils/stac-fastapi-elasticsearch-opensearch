# stac-fastapi-nosql

Elasticsearch and mongodb backends for stac-fastapi.

------
#### Running API on localhost:8083

```docker-compose -f docker-compose.mongo.yml up``` **or**

```docker-compose -f docker-compose.elasticsearch.yml up```

------
#### Testing


```make test-mongo``` **or**

```make test-es```
