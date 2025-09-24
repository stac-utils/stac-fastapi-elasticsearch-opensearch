# stac-fastapi-elasticsearch-opensearch

<!-- markdownlint-disable MD033 MD041 -->


<p align="left">
  <img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/sfeos.png" width=1000>
</p>

**Jump to:** [Project Introduction](#project-introduction---what-is-sfeos) | [Quick Start](#quick-start) | [Table of Contents](#table-of-contents)

  [![Downloads](https://static.pepy.tech/badge/stac-fastapi-core?color=blue)](https://pepy.tech/project/stac-fastapi-core)
  [![GitHub contributors](https://img.shields.io/github/contributors/stac-utils/stac-fastapi-elasticsearch-opensearch?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/graphs/contributors)
  [![GitHub stars](https://img.shields.io/github/stars/stac-utils/stac-fastapi-elasticsearch-opensearch.svg?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/stargazers)
  [![GitHub forks](https://img.shields.io/github/forks/stac-utils/stac-fastapi-elasticsearch-opensearch.svg?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/network/members)
   [![PyPI version](https://img.shields.io/pypi/v/stac-fastapi-elasticsearch.svg?color=blue)](https://pypi.org/project/stac-fastapi-elasticsearch/)
  [![STAC](https://img.shields.io/badge/STAC-1.1.0-blue.svg)](https://github.com/radiantearth/stac-spec/tree/v1.1.0)
  [![stac-fastapi](https://img.shields.io/badge/stac--fastapi-6.0.0-blue.svg)](https://github.com/stac-utils/stac-fastapi)

## Sponsors & Supporters

The following organizations have contributed time and/or funding to support the development of this project:

<p align="left">
  <a href="https://healy-hyperspatial.github.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/hh-logo-blue.png" alt="Healy Hyperspatial" height="100" hspace="20"></a>
  <a href="https://atomicmaps.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/am-logo-black.png" alt="Atomic Maps" height="100" hspace="20"></a>
  <a href="https://remotesensing.vito.be/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/VITO.png" alt="VITO Remote Sensing" height="100" hspace="20"></a>
</p>

## Project Introduction - What is SFEOS?

SFEOS (stac-fastapi-elasticsearch-opensearch) is a high-performance, scalable API implementation for serving SpatioTemporal Asset Catalog (STAC) data - an enhanced GeoJSON format designed specifically for geospatial assets like satellite imagery, aerial photography, and other Earth observation data. This project enables organizations to:

- **Efficiently catalog and search geospatial data** such as satellite imagery, aerial photography, DEMs, and other geospatial assets using Elasticsearch or OpenSearch as the database backend
- **Implement standardized STAC APIs** that support complex spatial, temporal, and property-based queries across large collections of geospatial data
- **Scale to millions of geospatial assets** with fast search performance through optimized spatial indexing and query capabilities
- **Support OGC-compliant filtering** including spatial operations (intersects, contains, etc.) and temporal queries
- **Perform geospatial aggregations** to analyze data distribution across space and time
- **Enhanced collection search capabilities** with support for sorting and field selection

This implementation builds on the STAC-FastAPI framework, providing a production-ready solution specifically optimized for Elasticsearch and OpenSearch databases. It's ideal for organizations managing large geospatial data catalogs who need efficient discovery and access capabilities through standardized APIs.

## Common Deployment Patterns

stac-fastapi-elasticsearch-opensearch can be deployed in several ways depending on your needs:

- **Containerized Application**: Run as a Docker container with connections to Elasticsearch/OpenSearch databases
- **Serverless Function**: Deploy as AWS Lambda or similar serverless function with API Gateway
- **Traditional Server**: Run on virtual machines or bare metal servers in your infrastructure
- **Kubernetes**: Deploy as part of a larger microservices architecture with container orchestration

The implementation is flexible and can scale from small local deployments to large production environments serving millions of geospatial assets.

## Technologies

This project is built on the following technologies: STAC, stac-fastapi, FastAPI, Elasticsearch, Python, OpenSearch

<p align="left">
  <a href="https://stacspec.org/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/STAC-01.png" alt="STAC" height="100" hspace="10"></a>
  <a href="https://www.python.org/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/python.png" alt="Python" height="80" hspace="10"></a>
  <a href="https://fastapi.tiangolo.com/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/fastapi.svg" alt="FastAPI" height="80" hspace="10"></a>
  <a href="https://www.elastic.co/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/elasticsearch.png" alt="Elasticsearch" height="80" hspace="10"></a>
  <a href="https://opensearch.org/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/opensearch.svg" alt="OpenSearch" height="80" hspace="10"></a>
</p>

## Table of Contents

- [stac-fastapi-elasticsearch-opensearch](#stac-fastapi-elasticsearch-opensearch)
  - [Sponsors \& Supporters](#sponsors--supporters)
  - [Project Introduction - What is SFEOS?](#project-introduction---what-is-sfeos)
  - [Common Deployment Patterns](#common-deployment-patterns)
  - [Technologies](#technologies)
  - [Table of Contents](#table-of-contents)
  - [Collection Search Extensions](#collection-search-extensions)
  - [Documentation \& Resources](#documentation--resources)
  - [Package Structure](#package-structure)
  - [Examples](#examples)
  - [Performance](#performance)
    - [Direct Response Mode](#direct-response-mode)
  - [Quick Start](#quick-start)
    - [Installation](#installation)
    - [Running Locally](#running-locally)
      - [Using Pre-built Docker Images](#using-pre-built-docker-images)
      - [Using Docker Compose](#using-docker-compose)
  - [Configuration Reference](#configuration-reference)
  - [Datetime-Based Index Management](#datetime-based-index-management)
    - [Overview](#overview)
    - [When to Use](#when-to-use)
    - [Configuration](#configuration)
      - [Enabling Datetime-Based Indexing](#enabling-datetime-based-indexing)
    - [Related Configuration Variables](#related-configuration-variables)
  - [How Datetime-Based Indexing Works](#how-datetime-based-indexing-works)
    - [Index and Alias Naming Convention](#index-and-alias-naming-convention)
    - [Index Size Management](#index-size-management)
  - [Interacting with the API](#interacting-with-the-api)
  - [Configure the API](#configure-the-api)
  - [Collection Pagination](#collection-pagination)
  - [Ingesting Sample Data CLI Tool](#ingesting-sample-data-cli-tool)
  - [Elasticsearch Mappings](#elasticsearch-mappings)
  - [Managing Elasticsearch Indices](#managing-elasticsearch-indices)
    - [Snapshots](#snapshots)
    - [Reindexing](#reindexing)
  - [Auth](#auth)
  - [Aggregation](#aggregation)
  - [Rate Limiting](#rate-limiting)

## Documentation & Resources

- **Online Documentation**: [https://stac-utils.github.io/stac-fastapi-elasticsearch-opensearch](https://stac-utils.github.io/stac-fastapi-elasticsearch-opensearch/)
- **Source Code**: [https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch)
- **API Examples**: [Postman Documentation](https://documenter.getpostman.com/view/12888943/2s8ZDSdRHA) - Examples of how to use the API endpoints
- **Community**:
  - [Gitter Chat](https://app.gitter.im/#/room/#stac-fastapi-elasticsearch_community:gitter.im) - For real-time discussions
  - [GitHub Discussions](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/discussions) - For longer-form questions and answers

## Collection Search Extensions

SFEOS implements extended capabilities for the `/collections` endpoint, allowing for more powerful collection discovery:

- **Sorting**: Sort collections by sortable fields using the `sortby` parameter
  - Example: `/collections?sortby=+id` (ascending sort by ID)
  - Example: `/collections?sortby=-id` (descending sort by ID)
  - Example: `/collections?sortby=-temporal` (descending sort by temporal extent)

- **Field Selection**: Request only specific fields to be returned using the `fields` parameter
  - Example: `/collections?fields=id,title,description`
  - This helps reduce payload size when only certain fields are needed

- **Free Text Search**: Search across collection text fields using the `q` parameter
  - Example: `/collections?q=landsat`
  - Searches across multiple text fields including title, description, and keywords
  - Supports partial word matching and relevance-based sorting

These extensions make it easier to build user interfaces that display and navigate through collections efficiently.

> **Configuration**: Collection search extensions can be disabled by setting the `ENABLE_COLLECTIONS_SEARCH` environment variable to `false`. By default, these extensions are enabled.

> **Note**: Sorting is only available on fields that are indexed for sorting in Elasticsearch/OpenSearch. With the default mappings, you can sort on:
> - `id` (keyword field)
> - `extent.temporal.interval` (date field)
> - `temporal` (alias to extent.temporal.interval)
>
> Text fields like `title` and `description` are not sortable by default as they use text analysis for better search capabilities. Attempting to sort on these fields will result in a user-friendly error message explaining which fields are sortable and how to make additional fields sortable by updating the mappings.
>
> **Important**: Adding keyword fields to make text fields sortable can significantly increase the index size, especially for large text fields. Consider the storage implications when deciding which fields to make sortable.

## Package Structure

This project is organized into several packages, each with a specific purpose:

- **stac_fastapi_core**: Core functionality that's database-agnostic, including API models, extensions, and shared utilities. This package provides the foundation for building STAC API implementations with any database backend. See [stac-fastapi-mongo](https://github.com/Healy-Hyperspatial/stac-fastapi-mongo) for a working example.

- **sfeos_helpers**: Shared helper functions and utilities used by both the Elasticsearch and OpenSearch backends. This package includes:
  - `database`: Specialized modules for index, document, and database utility operations
  - `aggregation`: Elasticsearch/OpenSearch-specific aggregation functionality
  - Shared logic and utilities that improve code reuse between backends

- **stac_fastapi_elasticsearch**: Complete implementation of the STAC API using Elasticsearch as the backend database. This package depends on both `stac_fastapi_core` and `sfeos_helpers`.
- 
- **stac_fastapi_opensearch**: Complete implementation of the STAC API using OpenSearch as the backend database. This package depends on both `stac_fastapi_core` and `sfeos_helpers`.

## Examples

The `/examples` directory contains several useful examples and reference implementations:

- **pip_docker**: Examples of running stac-fastapi-elasticsearch from PyPI in Docker without needing any code from the repository
- **auth**: Authentication examples including:
  - Basic authentication
  - OAuth2 with Keycloak
  - Route dependencies configuration
- **rate_limit**: Example of implementing rate limiting for API requests
- **postman_collections**: Postman collection files you can import for testing API endpoints

These examples provide practical reference implementations for various deployment scenarios and features.

## Performance

### Direct Response Mode

- The `enable_direct_response` option is provided by the stac-fastapi core library (introduced in stac-fastapi 5.2.0) and is available in this project starting from v4.0.0.
- **Control via environment variable**: Set `ENABLE_DIRECT_RESPONSE=true` to enable this feature.
- **How it works**: When enabled, endpoints return Starlette Response objects directly, bypassing FastAPI's default serialization for improved performance.
- **Important limitation**: All FastAPI dependencies (including authentication, custom status codes, and validation) are disabled for all routes when this mode is enabled.
- **Best use case**: This mode is best suited for public or read-only APIs where authentication and custom logic are not required.
- **Default setting**: `false` for safety.
- **More information**: See [issue #347](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/347) for background and implementation details.

## Quick Start

This section helps you get up and running with stac-fastapi-elasticsearch-opensearch quickly.

### Installation

- **For versions 4.0.0a1 and newer** (PEP 625 compliant naming):
  ```bash
  pip install stac-fastapi-elasticsearch  # Elasticsearch backend
  pip install stac-fastapi-opensearch    # Opensearch backend
  pip install stac-fastapi-core          # Core library
  ```

- **For versions 4.0.0a0 and older**:
  ```bash
  pip install stac-fastapi.elasticsearch  # Elasticsearch backend
  pip install stac-fastapi.opensearch    # Opensearch backend
  pip install stac-fastapi.core          # Core library
  ```

> **Important Note:** Starting with version 4.0.0a1, package names have changed from using periods (e.g., `stac-fastapi.core`) to using hyphens (e.g., `stac-fastapi-core`) to comply with PEP 625. The internal package structure uses underscores, but users should install with hyphens as shown above. Please update your requirements files accordingly.

### Running Locally

There are two main ways to run the API locally:

#### Using Pre-built Docker Images

- We provide ready-to-use Docker images through GitHub Container Registry:
  - [ElasticSearch backend](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pkgs/container/stac-fastapi-es)
  - [OpenSearch backend](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pkgs/container/stac-fastapi-os)

- **Pull and run the images**:
  ```shell
  # For Elasticsearch backend
  docker pull ghcr.io/stac-utils/stac-fastapi-es:latest

  # For OpenSearch backend
  docker pull ghcr.io/stac-utils/stac-fastapi-os:latest
  ```

#### Using Docker Compose

- **Prerequisites**: Ensure [Docker Compose](https://docs.docker.com/compose/install/) or [Podman Compose](https://podman-desktop.io/docs/compose) is installed on your machine.

- **Start the API**:
  ```shell
  docker compose up elasticsearch app-elasticsearch
  ```

- **Configuration**: By default, Docker Compose uses Elasticsearch 8.x and OpenSearch 2.11.1. To use different versions, create a `.env` file:
  ```shell
  ELASTICSEARCH_VERSION=8.11.0
  OPENSEARCH_VERSION=2.11.1
  ENABLE_DIRECT_RESPONSE=false
  ```

- **Compatibility**: The most recent Elasticsearch 7.x versions should also work. See the [opensearch-py docs](https://github.com/opensearch-project/opensearch-py/blob/main/COMPATIBILITY.md) for compatibility information.



## Configuration Reference

You can customize additional settings in your `.env` file:

| Variable                     | Description                                                                          | Default                  | Required                                                                                     |
|------------------------------|--------------------------------------------------------------------------------------|--------------------------|---------------------------------------------------------------------------------------------|
| `ES_HOST`                    | Hostname for external Elasticsearch/OpenSearch.                                      | `localhost`              | Optional                                                                                    |
| `ES_PORT`                    | Port for Elasticsearch/OpenSearch.                                                   | `9200` (ES) / `9202` (OS)| Optional                                                                                    |
| `ES_USE_SSL`                 | Use SSL for connecting to Elasticsearch/OpenSearch.                                  | `true`                   | Optional                                                                                    |
| `ES_VERIFY_CERTS`            | Verify SSL certificates when connecting.                                             | `true`                   | Optional                                                                                    |
| `ES_API_KEY`                 | API Key for external Elasticsearch/OpenSearch.                                       | N/A                      | Optional                                                                                    |
| `ES_TIMEOUT`                 | Client timeout for Elasticsearch/OpenSearch.                                         | DB client default        | Optional                                                                                    |
| `STAC_FASTAPI_TITLE`         | Title of the API in the documentation.                                               | `stac-fastapi-<backend>` | Optional                                                                                    |
| `STAC_FASTAPI_DESCRIPTION`   | Description of the API in the documentation.                                         | N/A                      | Optional                                                                                    |
| `STAC_FASTAPI_VERSION`       | API version.                                                                         | `2.1`                    | Optional                                                                                    |
| `STAC_FASTAPI_LANDING_PAGE_ID` | Landing page ID                                                                    | `stac-fastapi`           | Optional                                                                                    |
| `APP_HOST`                   | Server bind address.                                                                 | `0.0.0.0`                | Optional                                                                                    |
| `APP_PORT`                   | Server port.                                                                         | `8000`                   | Optional                                                                                    |
| `ENVIRONMENT`                | Runtime environment.                                                                 | `local`                  | Optional                                                                                    |
| `WEB_CONCURRENCY`            | Number of worker processes.                                                          | `10`                     | Optional                                                                                    |
| `RELOAD`                     | Enable auto-reload for development.                                                  | `true`                   | Optional                                                                                    |
| `STAC_FASTAPI_RATE_LIMIT`    | API rate limit per client.                                                           | `200/minute`             | Optional                                                                                    |
| `BACKEND`                    | Tests-related variable                                                               | `elasticsearch` or `opensearch` based on the backend | Optional                                                        |
| `ELASTICSEARCH_VERSION`      | Version of Elasticsearch to use.                                                     | `8.11.0`                 | Optional                                                                                    |
| `OPENSEARCH_VERSION`         | OpenSearch version                                                                   | `2.11.1`                 | Optional                                                                                    |
| `ENABLE_DIRECT_RESPONSE`     | Enable direct response for maximum performance (disables all FastAPI dependencies, including authentication, custom status codes, and validation) | `false`                  | Optional                       |
| `RAISE_ON_BULK_ERROR`        | Controls whether bulk insert operations raise exceptions on errors. If set to `true`, the operation will stop and raise an exception when an error occurs. If set to `false`, errors will be logged, and the operation will continue. **Note:** STAC Item and ItemCollection validation errors will always raise, regardless of this flag. | `false` | Optional |
| `DATABASE_REFRESH`           | Controls whether database operations refresh the index immediately after changes. If set to `true`, changes will be immediately searchable. If set to `false`, changes may not be immediately visible but can improve performance for bulk operations. If set to `wait_for`, changes will wait for the next refresh cycle to become visible. | `false` | Optional |
| `ENABLE_COLLECTIONS_SEARCH`  | Enable collection search extensions (sort, fields).                                 | `true`                   | Optional                                                                                    |
| `ENABLE_TRANSACTIONS_EXTENSIONS` | Enables or disables the Transactions and Bulk Transactions API extensions. If set to `false`, the POST `/collections` route and related transaction endpoints (including bulk transaction operations) will be unavailable in the API. This is useful for deployments where mutating the catalog via the API should be prevented. | `true` | Optional |
| `STAC_ITEM_LIMIT` | Sets the environment variable for result limiting to SFEOS for the number of returned items and STAC collections. | `10` | Optional |
| `STAC_INDEX_ASSETS` | Controls if Assets are indexed when added to Elasticsearch/Opensearch. This allows asset fields to be included in search queries. | `false` | Optional |
| `ENV_MAX_LIMIT` | Configures the environment variable in SFEOS to override the default `MAX_LIMIT`, which controls the limit parameter for returned items and STAC collections. | `10,000` | Optional |
| `USE_DATETIME` | Configures the datetime search behavior in SFEOS. When enabled, searches both datetime field and falls back to start_datetime/end_datetime range for items with null datetime. When disabled, searches only by start_datetime/end_datetime range. | True | Optional |

> [!NOTE]
> The variables `ES_HOST`, `ES_PORT`, `ES_USE_SSL`, `ES_VERIFY_CERTS` and `ES_TIMEOUT` apply to both Elasticsearch and OpenSearch backends, so there is no need to rename the key names to `OS_` even if you're using OpenSearch.

## Datetime-Based Index Management

### Overview

SFEOS supports two indexing strategies for managing STAC items:

1. **Simple Indexing** (default) - One index per collection
2. **Datetime-Based Indexing** - Time-partitioned indexes with automatic management

The datetime-based indexing strategy is particularly useful for large temporal datasets. When a user provides a datetime parameter in a query, the system knows exactly which index to search, providing **multiple times faster searches** and significantly **reducing database load**.

### When to Use

**Recommended for:**
- Systems with large collections containing millions of items
- Systems requiring high-performance temporal searching

**Pros:**
- Multiple times faster queries with datetime filter
- Reduced database load - only relevant indexes are searched

**Cons:**
- Slightly longer item indexing time (automatic index management)
- Greater management complexity

### Configuration

#### Enabling Datetime-Based Indexing

Enable datetime-based indexing by setting the following environment variable:

```bash
ENABLE_DATETIME_INDEX_FILTERING=true
```

### Related Configuration Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `ENABLE_DATETIME_INDEX_FILTERING` | Enables time-based index partitioning | `false` | `true` |
| `DATETIME_INDEX_MAX_SIZE_GB` | Maximum size limit for datetime indexes (GB) - note: add +20% to target size due to ES/OS compression | `25` | `50` |
| `STAC_ITEMS_INDEX_PREFIX` | Prefix for item indexes | `items_` | `stac_items_` |

## How Datetime-Based Indexing Works

### Index and Alias Naming Convention

The system uses a precise naming convention:

**Physical indexes:**
```
{ITEMS_INDEX_PREFIX}{collection-id}_{uuid4}
```

**Aliases:**
```
{ITEMS_INDEX_PREFIX}{collection-id}                                  # Main collection alias
{ITEMS_INDEX_PREFIX}{collection-id}_{start-datetime}                 # Temporal alias
{ITEMS_INDEX_PREFIX}{collection-id}_{start-datetime}_{end-datetime}  # Closed index alias
```

**Example:**

*Physical indexes:*
- `items_sentinel-2-l2a_a1b2c3d4-e5f6-7890-abcd-ef1234567890`

*Aliases:*
- `items_sentinel-2-l2a` - main collection alias
- `items_sentinel-2-l2a_2024-01-01` - active alias from January 1, 2024
- `items_sentinel-2-l2a_2024-01-01_2024-03-15` - closed index alias (reached size limit)

### Index Size Management

**Important - Data Compression:** Elasticsearch and OpenSearch automatically compress data. The configured `DATETIME_INDEX_MAX_SIZE_GB` limit refers to the compressed size on disk. It is recommended to add +20% to the target size to account for compression overhead and metadata.

## Interacting with the API

- **Creating a Collection**:
  ```shell
  curl -X "POST" "http://localhost:8080/collections" \
       -H 'Content-Type: application/json; charset=utf-8' \
       -d $'{
    "id": "my_collection"
  }'
  ```

- **Adding an Item to a Collection**:
  ```shell
  curl -X "POST" "http://localhost:8080/collections/my_collection/items" \
       -H 'Content-Type: application/json; charset=utf-8' \
       -d @item.json
  ```

- **Searching for Items**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json; charset=utf-8' \
       -d $'{
    "collections": ["my_collection"],
    "limit": 10
  }'
  ```

- **Filtering by Bbox**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json; charset=utf-8' \
       -d $'{
    "collections": ["my_collection"],
    "bbox": [-180, -90, 180, 90]
  }'
  ```

- **Filtering by Datetime**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json; charset=utf-8' \
       -d $'{
    "collections": ["my_collection"],
    "datetime": "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z"
  }'
  ```

## Configure the API

- **API Title and Description**: By default set to `stac-fastapi-<backend>`. Customize these by setting:
  - `STAC_FASTAPI_TITLE`: Changes the API title in the documentation
  - `STAC_FASTAPI_DESCRIPTION`: Changes the API description in the documentation

- **Database Indices**: By default, the API reads from and writes to:
  - `collections` index for collections
  - `items_<collection name>` indices for items
  - Customize with `STAC_COLLECTIONS_INDEX` and `STAC_ITEMS_INDEX_PREFIX` environment variables

- **Root Path Configuration**: The application root path is the base URL by default.
  - For AWS Lambda with Gateway API: Set `STAC_FASTAPI_ROOT_PATH` to match the Gateway API stage name (e.g., `/v1`)

- **Feature Configuration**: Control which features are enabled:
  - `ENABLE_COLLECTIONS_SEARCH`: Set to `true` (default) to enable collection search extensions (sort, fields). Set to `false` to disable.
  - `ENABLE_TRANSACTIONS_EXTENSIONS`: Set to `true` (default) to enable transaction extensions. Set to `false` to disable.


## Collection Pagination

- **Overview**: The collections route supports pagination through optional query parameters.
- **Parameters**:
  - `limit`: Controls the number of collections returned per page
  - `token`: Used to retrieve subsequent pages of results
- **Response Structure**: The `links` field in the response contains a `next` link with the token for the next page of results.
- **Example Usage**:
  ```shell
  curl -X "GET" "http://localhost:8080/collections?limit=1&token=example_token"
  ```

## Ingesting Sample Data CLI Tool

- **Overview**: The `data_loader.py` script provides a convenient way to load STAC items into the database.

- **Usage**:
  ```shell
  python3 data_loader.py --base-url http://localhost:8080
  ```

- **Options**:
  ```
  --base-url TEXT       Base URL of the STAC API  [required]
  --collection-id TEXT  ID of the collection to which items are added
  --use-bulk            Use bulk insert method for items
  --data-dir PATH       Directory containing collection.json and feature
                        collection file
  --help                Show this message and exit.
  ```

- **Example Workflows**:
  - **Loading Sample Data**: 
    ```shell
    python3 data_loader.py --base-url http://localhost:8080
    ```
  - **Loading Data to a Specific Collection**:
    ```shell
    python3 data_loader.py --base-url http://localhost:8080 --collection-id my-collection
    ```
  - **Using Bulk Insert for Performance**:
    ```shell
    python3 data_loader.py --base-url http://localhost:8080 --use-bulk
    ```

## Elasticsearch Mappings

- **Overview**: Mappings apply to search index, not source data. They define how documents and their fields are stored and indexed.
- **Implementation**: 
  - Mappings are stored in index templates that are created on application startup
  - These templates are automatically applied when creating new Collection and Item indices
  - The `sfeos_helpers` package contains shared mapping definitions used by both Elasticsearch and OpenSearch backends
- **Customization**: Custom mappings can be defined by extending the base mapping templates.

## Managing Elasticsearch Indices

### Snapshots

- **Overview**: Snapshots provide a way to backup and restore your indices.

- **Creating a Snapshot Repository**:
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
  - This creates a snapshot repository that stores files in the elasticsearch/snapshots directory in this git repo clone
  - The elasticsearch.yml and compose files create a mapping from that directory to /usr/share/elasticsearch/snapshots within the Elasticsearch container and grant permissions for using it

- **Creating a Snapshot**:
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
  - This creates a snapshot named my_snapshot_2 and waits for the action to be completed before returning
  - This can also be done asynchronously by omitting the wait_for_completion parameter, and queried for status later
  - The indices parameter determines which indices are snapshotted, and can include wildcards

- **Viewing Snapshots**:
  ```shell
  # View a specific snapshot
  curl http://localhost:9200/_snapshot/my_fs_backup/my_snapshot_2
  
  # View all snapshots
  curl http://localhost:9200/_snapshot/my_fs_backup/_all
  ```
  - These commands allow you to check the status and details of your snapshots

- **Restoring a Snapshot**:
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
  - This specific command will restore any indices that match items_* and rename them so that the new index name will be suffixed with -copy
  - The rename_pattern and rename_replacement parameters allow you to restore indices under new names

- **Updating Collection References**:
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
  - After restoring, the item documents have been restored in the new index (e.g., my-collection-copy), but the value of the collection field in those documents is still the original value of my-collection
  - This command updates these values to match the new collection name using Elasticsearch's Update By Query feature

- **Creating a New Collection**:
  ```shell
  curl -X "POST" "http://localhost:8080/collections" \
       -H 'Content-Type: application/json' \
       -d $'{
    "id": "my-collection-copy"
  }'
  ```
  - The final step is to create a new collection through the API with the new name for each of the restored indices
  - This gives you a copy of the collection that has a resource URI (/collections/my-collection-copy) and can be correctly queried by collection name

### Reindexing

- **Overview**: Reindexing allows you to copy documents from one index to another, optionally transforming them in the process.

- **Use Cases**:
  - Apply changes to documents
  - Correct dynamically generated mappings
  - Transform data (e.g., lowercase identifiers)
  - The index templates will make sure that manually created indices will also have the correct mappings and settings

- **Example: Reindexing with Transformation**:
  ```shell
  curl -X "POST" "http://localhost:9200/_reindex" \
    -H 'Content-Type: application/json' \
    -d $'{
      "source": {
        "index": "items_my-collection-lower_my-collection-hex-000001"
      }, 
      "dest": {
        "index": "items_my-collection-lower_my-collection-hex-000002"
      },
      "script": {
        "source": "ctx._source.id = ctx._source.id.toLowerCase()",
        "lang": "painless"
      }
    }'
  ```
  - In this example, we make a copy of an existing Item index but change the Item identifier to be lowercase
  - The script parameter allows you to transform documents during the reindexing process

- **Updating Aliases**:
  ```shell
  curl -X "POST" "http://localhost:9200/_aliases" \
    -H 'Content-Type: application/json' \
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
            "index": "items_my-collection-lower_my-collection-hex-000002",
            "alias": "items_my-collection"
          }
        }
      ]
    }'
  ```
  - If you are happy with the data in the newly created index, you can move the alias items_my-collection to the new index
  - This makes the modified Items with lowercase identifiers visible to users accessing my-collection in the STAC API
  - Using aliases allows you to switch between different index versions without changing the API endpoint

## Auth

- **Overview**: Authentication is an optional feature that can be enabled through Route Dependencies.
- **Implementation Options**:
  - Basic authentication
  - OAuth2 with Keycloak
  - Custom route dependencies
- **Configuration**: Authentication can be configured using the `STAC_FASTAPI_ROUTE_DEPENDENCIES` environment variable.
- **Examples and Documentation**: Detailed examples and implementation guides can be found in the [examples/auth](examples/auth) directory.

## Aggregation

- **Supported Aggregations**:
  - Spatial aggregations of points and geometries
  - Frequency distribution aggregation of any property including dates
  - Temporal distribution of datetime values

- **Endpoint Locations**:
  - Root Catalog level: `/aggregations`
  - Collection level: `/<collection_id>/aggregations`

- **Implementation Details**: The `sfeos_helpers.aggregation` package provides specialized functionality for both Elasticsearch and OpenSearch backends.

- **Documentation**: Detailed information about supported aggregations can be found in [the aggregation docs](./docs/src/aggregation.md).


## Rate Limiting

- **Overview**: Rate limiting is an optional security feature that controls API request frequency on a remote address basis.

- **Configuration**: Enabled by setting the `STAC_FASTAPI_RATE_LIMIT` environment variable:
  ```
  STAC_FASTAPI_RATE_LIMIT=500/minute
  ```

- **Functionality**: 
  - Limits each client to a specified number of requests per time period (e.g., 500 requests per minute)
  - Helps prevent API abuse and maintains system stability
  - Ensures fair resource allocation among all clients
  
- **Examples**: Implementation examples are available in the [examples/rate_limit](examples/rate_limit) directory.
