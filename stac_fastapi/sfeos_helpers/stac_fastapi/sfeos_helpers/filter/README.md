# STAC FastAPI Filter Package

This package contains shared filter extension functionality used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI. It helps reduce code duplication and ensures consistent behavior
between the two implementations.

## Package Structure

The filter package is organized into three main modules:

- **cql2.py**: Contains functions for converting CQL2 patterns to Elasticsearch/OpenSearch compatible formats

  - [cql2_like_to_es](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/filter.py:59:0-75:5): Converts CQL2 "LIKE" characters to Elasticsearch "wildcard" characters
  - [\_replace_like_patterns](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/filter.py:51:0-56:71): Helper function for pattern replacement

- **transform.py**: Contains functions for transforming CQL2 queries to Elasticsearch/OpenSearch query DSL

  - [to_es_field](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/filter.py:83:0-93:47): Maps field names using queryables mapping
  - [to_es](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/filter.py:96:0-201:13): Transforms CQL2 query structures to Elasticsearch/OpenSearch query DSL

- **client.py**: Contains the base filter client implementation
  - [EsAsyncBaseFiltersClient](cci:2://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/filter.py:209:0-293:25): Base class for implementing the STAC filter extension

## Usage

Import the necessary components from the filter package:

```python
from stac_fastapi.sfeos_helpers.filter import cql2_like_to_es, to_es, EsAsyncBaseFiltersClient
```
