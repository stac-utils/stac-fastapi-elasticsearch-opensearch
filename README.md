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
  [![stac-fastapi](https://img.shields.io/badge/stac--fastapi-6.1.1-blue.svg)](https://github.com/stac-utils/stac-fastapi)

## Sponsors & Supporters

The following organizations have contributed time and/or funding to support the development of this project:

<p align="left">
  <a href="https://healy-hyperspatial.github.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/hh-logo-blue.png" alt="Healy Hyperspatial" height="100" hspace="20"></a>
  <a href="https://atomicmaps.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/am-logo-black.png" alt="Atomic Maps" height="100" hspace="20"></a>
  <a href="https://remotesensing.vito.be/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/VITO.png" alt="VITO Remote Sensing" height="100" hspace="20"></a>
  <a href="https://cloudferro.com/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/cloudferro-logo.png" alt="CloudFerro" height="105" hspace="20"></a>
</p>

## Latest News

- **01/11/2026:** Feature: **Hierarchical Catalog Support**. Sub-catalogs are now fully supported! Catalogs can now contain other catalogs for unlimited nesting levels. This enables complex organizational hierarchies with multi-parent support for both catalogs and collections.
- **01/09/2026:** New Feature: **Custom Index Mappings**. You can now customize Elasticsearch/OpenSearch index mappings directly via environment variables without changing source code. Use `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` to merge custom field definitions (e.g., for STAC extensions like SAR or Cube) or `STAC_FASTAPI_ES_MAPPINGS_FILE` to load mappings from a JSON file. See [Custom Index Mappings](#custom-index-mappings) for details.
- **12/09/2025:** Feature Merge: **Multi-Tenant Catalogs**. The [`STAC API - Multi-Tenant Catalogs Endpoint Extension`](https://github.com/stac-api-extensions/multi-tenant-catalogs) is now in main! This enables a registry of catalogs and supports **poly-hierarchy** (collections belonging to multiple catalogs simultaneously). Enable it via `ENABLE_CATALOGS_EXTENSION`. _Coming next: Support for nested sub-catalogs._
- **11/07/2025:** 🌍 The SFEOS STAC Viewer is now available at: https://healy-hyperspatial.github.io/sfeos-web. Use this site to examine your data and test your STAC API!
- **10/24/2025:** Added `previous_token` pagination using Redis for efficient navigation. This feature allows users to navigate backwards through large result sets by storing pagination state in Redis. To use this feature, ensure Redis is configured (see [Redis for navigation](#redis-for-navigation)) and set `REDIS_ENABLE=true` in your environment.
- **10/23/2025:** The `EXCLUDED_FROM_QUERYABLES` environment variable was added to exclude fields from the `queryables` endpoint. See [docs](#excluding-fields-from-queryables).
- **10/15/2025:** 🚀 SFEOS Tools v0.1.0 Released! - The new `sfeos-tools` CLI is now available on [PyPI](https://pypi.org/project/sfeos-tools/)
- **10/15/2025:** Added `reindex` command to **[SFEOS-tools](https://github.com/Healy-Hyperspatial/sfeos-tools)** for zero-downtime index updates when changing mappings or settings. The new `reindex` command makes it easy to apply mapping changes, update index settings, or migrate to new index structures without any service interruption, ensuring high availability of your STAC API during maintenance operations.

<details style="border: 1px solid #eaecef; border-radius: 6px; padding: 10px; margin-bottom: 16px; background-color: #f9f9f9;">
<summary style="cursor: pointer; font-weight: bold; margin: -10px -10px 0; padding: 10px; background-color: #f0f0f0; border-bottom: 1px solid #eaecef; border-top-left-radius: 6px; border-top-right-radius: 6px;">View Older News (Click to Expand)</summary>

-------------
- **10/12/2025:** Collections search **bbox** functionality added! The collections search extension now supports bbox queries. Collections will need to be updated via the API or with the new **[SFEOS-tools](https://github.com/Healy-Hyperspatial/sfeos-tools)** CLI package to support geospatial discoverability. 🙏 Thanks again to **CloudFerro** for their sponsorship of this work!
- **10/04/2025:** The **[CloudFerro](https://cloudferro.com/)** logo has been added to the sponsors and supporters list above. Their sponsorship of the ongoing collections search extension work has been invaluable. This is in addition to the many other important changes and updates their developers have added to the project.
- **09/25/2025:** v6.5.0 adds a new GET/POST /collections-search endpoint (disabled by default via ENABLE_COLLECTIONS_SEARCH_ROUTE) to avoid conflicts with the Transactions Extension, and enhances collections search with structured filtering (CQL2 JSON/text), query, and datetime filtering. These changes make collection discovery more powerful and configurable while preserving compatibility with transaction-enabled deployments.
<!-- Add more older news items here in Markdown format; GitHub will parse them thanks to the blank line implicit in this structure -->

</details>


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
  - [Sponsors & Supporters](#sponsors--supporters)
  - [Latest News](#latest-news)
  - [Project Introduction - What is SFEOS?](#project-introduction---what-is-sfeos)
  - [Common Deployment Patterns](#common-deployment-patterns)
  - [Technologies](#technologies)
  - [Table of Contents](#table-of-contents)
  - [Collection Search Extensions](#collection-search-extensions)
  - [Catalogs Route](#catalogs-route)
  - [Documentation & Resources](#documentation--resources)
  - [SFEOS STAC Viewer](#sfeos-stac-viewer)
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
  - [Excluding Fields from Queryables](#excluding-fields-from-queryables)
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
  - [SFEOS Tools CLI](#sfeos-tools-cli)
  - [Ingesting Sample Data CLI Tool](#ingesting-sample-data-cli-tool)
  - [Redis for navigation](#redis-for-navigation)
  - [Elasticsearch Mappings](#elasticsearch-mappings)
  - [Custom Index Mappings](#custom-index-mappings)
  - [Managing Elasticsearch Indices](#managing-elasticsearch-indices)
    - [Snapshots](#snapshots)
    - [Reindexing](#reindexing)
  - [Auth](#auth)
  - [Aggregation](#aggregation)
  - [Rate Limiting](#rate-limiting)
  - [Error Monitoring with Sentry](#error-monitoring-with-sentry)
  - [Hidden Items Filtering](#hidden-items-filtering)

## Documentation & Resources

- **Online Documentation**: [https://stac-utils.github.io/stac-fastapi-elasticsearch-opensearch](https://stac-utils.github.io/stac-fastapi-elasticsearch-opensearch/)
- **Source Code**: [https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch)
- **API Examples**: [Postman Documentation](https://documenter.getpostman.com/view/12888943/2s8ZDSdRHA) - Examples of how to use the API endpoints
- **Community**:
  - [Gitter Chat](https://app.gitter.im/#/room/#stac-fastapi-elasticsearch_community:gitter.im) - For real-time discussions
  - [GitHub Discussions](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/discussions) - For longer-form questions and answers

## SFEOS STAC Viewer

The SFEOS STAC viewer is a web-based application for examining and testing STAC APIs. It provides an interactive interface to explore geospatial data catalogs, visualize spatial extents, and test API endpoints.

### Access

The viewer is available at: https://healy-hyperspatial.github.io/sfeos-web/

### Features

- Browse collections and items interactively
- Interactive map visualization of spatial extents
- Test API endpoints directly from the interface
- Search and filter capabilities for exploring data

### Usage

Navigate to the URL above and connect to your SFEOS API instance by providing the base URL of your STAC API. This is done with the `API SERVER` button on the right side of the page. 

You can also override the default STAC API URL by appending the `stacApiUrl` parameter to the application URL. For example:

https://healy-hyperspatial.github.io/sfeos-web?stacApiUrl=https://stac.example.com

> [!IMPORTANT]
> To connect to a local SFEOS instance (e.g., `http://localhost:8080`), you must run the [SFEOS STAC Viewer](https://github.com/Healy-Hyperspatial/sfeos-web) locally. Browsers generally block hosted web applications from making requests to `localhost` due to security restrictions.

## Collection Search Extensions

SFEOS provides enhanced collection search capabilities through two primary routes:
- **GET/POST `/collections`**: The standard STAC endpoint with extended query parameters
- **GET/POST `/collections-search`**: A custom endpoint that supports the same parameters, created to avoid conflicts with the STAC Transactions extension if enabled (which uses POST `/collections` for collection creation)

The `/collections-search` endpoint follows the [Collection Search with Large Payloads](https://github.com/Healy-Hyperspatial/collection-search-large-payloads) specification, which provides a dedicated, conflict-free mechanism for advanced collection searching.

These endpoints support advanced collection discovery features including:

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

- **Structured Filtering**: Filter collections using CQL2 expressions
  - JSON format: `/collections?filter={"op":"=","args":[{"property":"id"},"sentinel-2"]}&filter-lang=cql2-json`
  - Text format: `/collections?filter=id='sentinel-2'&filter-lang=cql2-text` (note: string values must be quoted)
  - Advanced text format: `/collections?filter=id LIKE '%sentinel%'&filter-lang=cql2-text` (supports LIKE, BETWEEN, etc.)
  - Supports both CQL2 JSON and CQL2 text formats with various operators
  - Enables precise filtering on any collection property

- **Datetime Filtering**: Filter collections by their temporal extent using the `datetime` parameter
  - Example: `/collections?datetime=2020-01-01T00:00:00Z/2020-12-31T23:59:59Z` (finds collections with temporal extents that overlap this range)
  - Example: `/collections?datetime=2020-06-15T12:00:00Z` (finds collections whose temporal extent includes this specific time)
  - Example: `/collections?datetime=2020-01-01T00:00:00Z/..` (finds collections with temporal extents that extend to or beyond January 1, 2020)
  - Example: `/collections?datetime=../2020-12-31T23:59:59Z` (finds collections with temporal extents that begin on or before December 31, 2020)
  - Collections are matched if their temporal extent overlaps with the provided datetime parameter
  - This allows for efficient discovery of collections based on time periods

- **Spatial Filtering**: Filter collections by their spatial extent using the `bbox` parameter
  - Example: `/collections?bbox=-10,35,40,70` (finds collections whose spatial extent intersects with this bounding box)
  - Example: `/collections?bbox=-180,-90,180,90` (finds all collections with global coverage)
  - Supports both 2D bounding boxes `[minx, miny, maxx, maxy]` and 3D bounding boxes `[minx, miny, minz, maxx, maxy, maxz]` (altitude values are ignored for spatial queries)
  - Collections are matched if their spatial extent (stored in the `extent.spatial.bbox` field) intersects with the provided bbox parameter
  - **Implementation Note**: When collections are created or updated, a `bbox_shape` field is automatically generated from the collection's spatial extent and indexed as a GeoJSON polygon for efficient geospatial queries
  - **Migrating Legacy Collections**: Collections created before this feature was added will not be discoverable via bbox search until they have the `bbox_shape` field added. You can either:
    - Update each collection via the API (PUT `/collections/{collection_id}` with the existing collection data)
    - Use the [SFEOS Tools CLI](https://github.com/Healy-Hyperspatial/sfeos-tools) (install with `pip install sfeos-tools[elasticsearch]` or `pip install sfeos-tools[opensearch]`):
      - `sfeos-tools add-bbox-shape --backend elasticsearch --no-ssl`
      - `sfeos-tools add-bbox-shape --backend opensearch --host db.example.com --no-ssl`

These extensions make it easier to build user interfaces that display and navigate through collections efficiently.

> **Configuration**: Collection search extensions (sorting, field selection, free text search, structured filtering, datetime filtering, and spatial filtering) for the `/collections` endpoint can be disabled by setting the `ENABLE_COLLECTIONS_SEARCH` environment variable to `false`. By default, these extensions are enabled.
> 
> **Configuration**: The custom `/collections-search` endpoint can be enabled by setting the `ENABLE_COLLECTIONS_SEARCH_ROUTE` environment variable to `true`. By default, this endpoint is **disabled**.

> **Note**: Sorting is only available on fields that are indexed for sorting in Elasticsearch/OpenSearch. With the default mappings, you can sort on:
> - `id` (keyword field)
> - `extent.temporal.interval` (date field)
> - `temporal` (alias to extent.temporal.interval)
>
> Text fields like `title` and `description` are not sortable by default as they use text analysis for better search capabilities. Attempting to sort on these fields will result in a user-friendly error message explaining which fields are sortable and how to make additional fields sortable by updating the mappings.
>
> **Important**: Adding keyword fields to make text fields sortable can significantly increase the index size, especially for large text fields. Consider the storage implications when deciding which fields to make sortable.


## Catalogs Route

SFEOS supports a **Catalog Registry** through the `/catalogs` endpoint. This allows for organized discovery by grouping collections into specific logical catalogs.

This implementation follows the [Multi-Tenant Virtual Catalogs Endpoint](https://github.com/Healy-Hyperspatial/multi-tenant-catalogs) specification, which enables a multi-catalog STAC API architecture. SFEOS supports **hierarchical catalog structures** (Root -> Catalogs -> Sub-Catalogs -> Collections), allowing catalogs to contain other catalogs for flexible organizational hierarchies.

### Features

- **Catalog Registry**: Discover and browse a list of available catalogs
- **Hierarchical Catalogs**: Create nested catalog structures with sub-catalogs for multi-level organization
- **Multi-Catalog Collections**: Collections can belong to multiple catalogs simultaneously, enabling flexible organizational hierarchies
- **Multi-Parent Catalogs**: Catalogs can belong to multiple parent catalogs, supporting complex organizational structures
- **Collection Discovery**: Access collections within specific catalog contexts
- **STAC API Compliance**: Follows STAC specification for catalog objects and linking
- **Flexible Querying**: Support for standard STAC API query parameters when browsing collections within catalogs
- **Safety-First Data Protection**: Collection and catalog data is never deleted through the catalogs route; only containers can be destroyed

### Safety Architecture

The catalogs extension implements a **safety-first design** that protects collection data:

| Operation | Route | Behavior | Data Safety |
|-----------|-------|----------|-------------|
| Delete Catalog | `DELETE /catalogs/{id}` | Removes the catalog container; all links between catalog and collections/sub-catalogs are severed; children are adopted by root if orphaned | 🟢 Safe (structure only) |
| Unlink Collection | `DELETE /catalogs/{id}/collections/{id}` | Severs the link between collection and this catalog; collection survives at root if it has no other parents | 🟢 Safe (zero data loss) |
| Destroy Collection | `DELETE /collections/{id}` | Permanently deletes collection and all items (intentional, outside catalogs route) | 🔴 Destructive |

**Key Principle**: The catalogs route is write-safe for creation but read-only for deletion of collections. You can create collections via the catalogs route, but deleting collections is only allowed through the explicit `/collections` endpoint. This prevents accidental data loss while allowing full organizational flexibility.

**Link Removal**: When you delete a catalog or unlink a collection, the relationship links are permanently severed from the database. However, the collection data itself remains intact and is automatically adopted by the root catalog if it becomes an orphan.

**Collection Deletion**: Collections CAN be permanently deleted, but only via the `/collections/{collection_id}` endpoint (outside the catalogs route). This ensures intentional, explicit deletion of collection data and prevents accidental data loss through the catalogs API.

### Endpoints

**Catalog Management:**
- **GET `/catalogs`**: Retrieve the root catalog and its child catalogs
- **POST `/catalogs`**: Create a new catalog (requires appropriate permissions)
- **GET `/catalogs/{catalog_id}`**: Retrieve a specific catalog and its children
- **PUT `/catalogs/{catalog_id}`**: Update an existing catalog (title, description, etc.)
- **DELETE `/catalogs/{catalog_id}`**: Delete a catalog (collections and sub-catalogs are unlinked and adopted by root if orphaned)

**Sub-Catalog Hierarchy:**
- **GET `/catalogs/{catalog_id}/catalogs`**: Retrieve sub-catalogs within a specific catalog
- **POST `/catalogs/{catalog_id}/catalogs`**: Create a new sub-catalog within a specific catalog

**Children & Collections:**
- **GET `/catalogs/{catalog_id}/children`**: Retrieve all children (Catalogs and Collections) of this catalog with optional type filtering
- **GET `/catalogs/{catalog_id}/collections`**: Retrieve collections within a specific catalog
- **POST `/catalogs/{catalog_id}/collections`**: Create a new collection within a specific catalog
- **GET `/catalogs/{catalog_id}/collections/{collection_id}`**: Retrieve a specific collection within a catalog
- **DELETE `/catalogs/{catalog_id}/collections/{collection_id}`**: Unlink a collection from a catalog (collection survives at root if orphaned)

**Items:**
- **GET `/catalogs/{catalog_id}/collections/{collection_id}/items`**: Retrieve items within a collection in a catalog context
- **GET `/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}`**: Retrieve a specific item within a catalog context

### Usage Examples

```bash
# Get root catalog
curl "http://localhost:8081/catalogs"

# Get specific catalog
curl "http://localhost:8081/catalogs/earth-observation"

# Update a catalog
curl -X PUT "http://localhost:8081/catalogs/earth-observation" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "earth-observation",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "description": "Updated description for Earth observation data",
    "title": "Updated Earth Observation Catalog"
  }'

# Get sub-catalogs within a catalog
curl "http://localhost:8081/catalogs/earth-observation/catalogs"

# Create a new sub-catalog within a catalog
curl -X POST "http://localhost:8081/catalogs/earth-observation/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "sentinel-data",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "description": "Sentinel satellite data catalog",
    "title": "Sentinel Data"
  }'

# Get all children (catalogs and collections) of a catalog
curl "http://localhost:8081/catalogs/earth-observation/children"

# Get only catalog children of a catalog
curl "http://localhost:8081/catalogs/earth-observation/children?type=Catalog"

# Get only collection children of a catalog
curl "http://localhost:8081/catalogs/earth-observation/children?type=Collection"

# Get collections in a catalog
curl "http://localhost:8081/catalogs/earth-observation/collections"

# Create a new collection within a catalog
curl -X POST "http://localhost:8081/catalogs/earth-observation/collections" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "landsat-9",
    "type": "Collection",
    "stac_version": "1.0.0",
    "description": "Landsat 9 satellite imagery collection",
    "title": "Landsat 9",
    "license": "MIT",
    "extent": {
      "spatial": {"bbox": [[-180, -90, 180, 90]]},
      "temporal": {"interval": [["2021-09-27T00:00:00Z", null]]}
    }
  }'

# Get specific collection within a catalog
curl "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2"

# Get items in a collection within a catalog
curl "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2/items"

# Get specific item within a catalog
curl "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2/items/S2A_20231015_123456"

# Unlink a collection from a catalog
# The collection is removed from this catalog but survives in the database
# If it has no other parent catalogs, it is automatically adopted by root
curl -X DELETE "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2"

# Delete a catalog
# All collections are unlinked and adopted by root if they become orphans
# Collection data is NEVER deleted
curl -X DELETE "http://localhost:8081/catalogs/earth-observation"

# To permanently delete a collection and all its items, use the /collections endpoint
curl -X DELETE "http://localhost:8081/collections/sentinel-2"
```

### Delete Behavior

The catalogs extension implements a **safety-first deletion policy**:

- **`DELETE /catalogs/{id}`**: Removes the catalog container and severs all links between the catalog and its collections/sub-catalogs. Children are automatically adopted by the root catalog if they become orphans. **Collection and catalog data is never deleted.**
- **`DELETE /catalogs/{id}/collections/{id}`**: Severs the link between a collection and this catalog. If the collection has other parent catalogs, it remains linked to them. If it becomes an orphan, it is automatically adopted by root. **Collection data is never deleted.**
- **`DELETE /collections/{id}`**: Permanently deletes a collection and all its items. This is the only way to destroy collection data and must be done explicitly outside the catalogs route.

**What Gets Removed**:
- Catalog documents (when deleting a catalog)
- Relationship links between catalogs and collections/sub-catalogs (when unlinking)
- Collection documents and items (only via `/collections` endpoint)

**What Is Always Preserved**:
- Collection data (never deleted through catalogs routes)
- Catalog data (never deleted through catalogs routes)
- Item data (never deleted through catalogs routes)

> **Note**: The `cascade` parameter has been removed. Collections and catalogs are never deleted through the catalogs route. If you need to delete collections, use the `/collections` endpoint explicitly.

### Response Structure

Catalog responses include:
- **Catalog metadata**: ID, title, description, and other catalog properties
- **Sub-catalogs**: Links to nested sub-catalogs for multi-level hierarchical navigation
- **Collections**: Links to collections contained within the catalog
- **STAC links**: Properly formatted STAC API links for navigation (parent, root, self, children)

This feature enables building user interfaces that provide organized, hierarchical browsing of STAC collections and catalogs, making it easier for users to discover and navigate through large datasets organized by theme, provider, region, or any other categorization scheme. The hierarchical structure supports unlimited nesting levels for maximum organizational flexibility.

### Poly-Hierarchy & Linking Existing Resources

This extension supports **Poly-Hierarchy**, meaning a single Catalog or Collection can belong to multiple parents simultaneously. This allows you to create "Virtual" views or "Playlists" of data without duplicating content.

To link an **existing** Catalog or Collection to a new parent, simply `POST` it to the new parent's endpoint using its existing `id`. The API implements an **Upsert** (Update or Insert) logic:

1. **Check:** Does a resource with this `id` already exist?
2. **If YES (Link):** The API adds the new parent to the resource's `parent_ids` list. No data is duplicated.
3. **If NO (Create):** The API creates a new resource.

#### Example: Creating a "Forestry" Playlist

Imagine you have an existing catalog `sentinel-2` stored under `providers/esa`. You want to create a curated "Forestry" catalog that includes this existing data.

```bash
# 1. Create the new Forestry catalog
curl -X POST "http://localhost:8081/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "forestry",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "description": "Forestry-related datasets",
    "title": "Forestry"
  }'

# 2. Link the EXISTING Sentinel-2 catalog to Forestry
# Note: We use the existing ID "sentinel-2"
curl -X POST "http://localhost:8081/catalogs/forestry/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "sentinel-2",
    "type": "Catalog",
    "stac_version": "1.0.0",
    "description": "Sentinel-2 satellite imagery",
    "title": "Sentinel-2"
  }'
```

**Result:** The sentinel-2 catalog is now accessible via both paths:
- `/catalogs/providers/esa/catalogs/sentinel-2`
- `/catalogs/forestry/catalogs/sentinel-2`

Because you are linking the node (the Catalog), the entire sub-tree attached to that node is automatically shared. If sentinel-2 contains millions of items and sub-catalogs, they are all instantly visible under the new forestry path without needing to re-link individual items.

> **Configuration**: The catalogs route can be enabled or disabled by setting the `ENABLE_CATALOGS_ROUTE` environment variable to `true` or `false`. By default, this endpoint is **disabled**.

## Package Structure

This project is organized into several packages, each with a specific purpose:

- **stac_fastapi_core**: Core functionality that's database-agnostic, including API models, extensions, and shared utilities. This package provides the foundation for building STAC API implementations with any database backend. See [stac-fastapi-mongo](https://github.com/Healy-Hyperspatial/stac-fastapi-mongo) for a working example.

- **sfeos_helpers**: Shared helper functions and utilities used by both the Elasticsearch and OpenSearch backends. This package includes:
  - `database`: Specialized modules for index, document, and database utility operations
  - `aggregation`: Elasticsearch/OpenSearch-specific aggregation functionality
  - Shared logic and utilities that improve code reuse between backends

- **stac_fastapi_elasticsearch**: Complete implementation of the STAC API using Elasticsearch as the backend database. This package depends on both `stac_fastapi_core` and `sfeos_helpers`.

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


### 1. Server & Application

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `APP_HOST` | Server bind address. | `0.0.0.0` | Optional |
| `APP_PORT` | Server port. | `8000` | Optional |
| `ENVIRONMENT` | Runtime environment. | `local` | Optional |
| `WEB_CONCURRENCY` | Number of worker processes. | `10` | Optional |
| `RELOAD` | Enable auto-reload for development. | `true` | Optional |

### 2. Backend Connection (Elasticsearch / OpenSearch)

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ES_HOST` | Hostname for external Elasticsearch/OpenSearch. | `localhost` | Optional |
| `ES_PORT` | Port for Elasticsearch/OpenSearch. | `9200` (ES) / `9202` (OS) | Optional |
| `ES_USE_SSL` | Use SSL for connecting to Elasticsearch/OpenSearch. | `true` | Optional |
| `ES_VERIFY_CERTS` | Verify SSL certificates when connecting. | `true` | Optional |
| `ES_API_KEY` | API Key for external Elasticsearch/OpenSearch. | N/A | Optional |
| `ES_TIMEOUT` | Client timeout for Elasticsearch/OpenSearch. | DB client default | Optional |
| `BACKEND` | Tests-related variable | `elasticsearch` or `opensearch` based on the backend | Optional |
| `ELASTICSEARCH_VERSION` | Version of Elasticsearch to use. | `8.11.0` | Optional |
| `OPENSEARCH_VERSION` | OpenSearch version | `2.11.1` | Optional |

### 3. API Metadata

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `STAC_FASTAPI_TITLE` | Title of the API in the documentation. | `stac-fastapi-<backend>` | Optional |
| `STAC_FASTAPI_DESCRIPTION` | Description of the API in the documentation. | N/A | Optional |
| `STAC_FASTAPI_VERSION` | API version. | `2.1` | Optional |
| `STAC_FASTAPI_LANDING_PAGE_ID` | Landing page ID | `stac-fastapi` | Optional |

### 4. Feature Flags

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENABLE_DIRECT_RESPONSE` | Enable direct response for maximum performance (disables all FastAPI dependencies, including authentication, custom status codes, and validation) | `false` | Optional |
| `ENABLE_COLLECTIONS_SEARCH` | Enable collection search extensions (sort, fields, free text search, structured filtering, and datetime filtering) on the core `/collections` endpoint. | `true` | Optional |
| `ENABLE_COLLECTIONS_SEARCH_ROUTE` | Enable the custom `/collections-search` endpoint (both GET and POST methods). When disabled, the custom endpoint will not be available, but collection search extensions will still be available on the core `/collections` endpoint if `ENABLE_COLLECTIONS_SEARCH` is true. | `false` | Optional |
| `ENABLE_TRANSACTIONS_EXTENSIONS` | Enables or disables the Transactions and Bulk Transactions API extensions. This is useful for deployments where mutating the catalog via the API should be prevented. If set to `true`, the POST `/collections` route for search will be unavailable in the API. | `true` | Optional |
| `ENABLE_CATALOGS_ROUTE` | Enable the **/catalogs** endpoint for hierarchical catalog browsing and navigation. | `false` | Optional |
| `STAC_INDEX_ASSETS` | Controls if Assets are indexed when added to Elasticsearch/Opensearch. This allows asset fields to be included in search queries. | `false` | Optional |

### 5. Limits & Performance

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `STAC_FASTAPI_RATE_LIMIT` | API rate limit per client. | `200/minute` | Optional |
| `STAC_GLOBAL_COLLECTION_MAX_LIMIT` | Configures the maximum number of STAC collections that can be returned in a single search request. | N/A | Optional |
| `STAC_DEFAULT_COLLECTION_LIMIT` | Configures the default number of STAC collections returned when no limit parameter is specified in the request. | `300` | Optional |
| `STAC_GLOBAL_ITEM_MAX_LIMIT` | Configures the maximum number of STAC items that can be returned in a single search request. | N/A | Optional |
| `STAC_DEFAULT_ITEM_LIMIT` | Configures the default number of STAC items returned when no limit parameter is specified in the request. | `10` | Optional |

### 6. Database Indexing & Behavior

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `RAISE_ON_BULK_ERROR` | Controls whether bulk insert operations raise exceptions on errors. If set to `true`, the operation will stop and raise an exception when an error occurs. If set to `false`, errors will be logged, and the operation will continue. **Note:** STAC Item and ItemCollection validation errors will always raise, regardless of this flag. | `false` | Optional |
| `DATABASE_REFRESH` | Controls whether database operations refresh the index immediately after changes. If set to `true`, changes will be immediately searchable. If set to `false`, changes may not be immediately visible but can improve performance for bulk operations. If set to `wait_for`, changes will wait for the next refresh cycle to become visible. | `false` | Optional |
| `USE_DATETIME` | Configures the datetime search behavior in SFEOS. When enabled, searches both datetime field and falls back to start_datetime/end_datetime range for items with null datetime. When disabled, searches only by start_datetime/end_datetime range. | `true` | Optional |
| `USE_DATETIME_NANOS` | Enables nanosecond precision handling for `datetime` field searches as per the `date_nanos` type. When `False`, it uses 3 millisecond precision as per the type `date`. | `true` | Optional |
| `PROPERTIES_DATETIME_FIELD` | Specifies the field used for single datetime of the items in the backend database. | `properties.datetime` | Optional |
| `PROPERTIES_START_DATETIME_FIELD` | Specifies the field used for the lower value of a datetime range for the items in the backend database. | `properties.start_datetime` | Optional |
| `PROPERTIES_END_DATETIME_FIELD` | Specifies the field used for the upper value of a datetime range for the items in the backend database. | `properties.end_datetime` | Optional |
| `COLLECTION_FIELD` | Specifies the field used for the collection an item belongs to in the backend database | `collection` | Optional |
| `GEOMETRY_FIELD` | Specifies the field containing the geometry of the items in the backend database | `geometry` | Optional |
| `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` | JSON string of custom Elasticsearch/OpenSearch property mappings to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_MAPPINGS_FILE` | Path to a JSON file containing custom Elasticsearch/OpenSearch property mappings to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_DYNAMIC_MAPPING` | Controls dynamic mapping behavior for item indices. Values: `true` (default), `false`, or `strict`. See [Custom Index Mappings](#custom-index-mappings). | `true` | Optional |

### 7. Filtering, Exclusions & Queryables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `VALIDATE_QUERYABLES` | Enable validation of query parameters against the collection's queryables. If set to `true`, the API will reject queries containing fields that are not defined in the collection's queryables. | `false` | Optional |
| `QUERYABLES_CACHE_TTL` | Time-to-live (in seconds) for the queryables cache. Used when `VALIDATE_QUERYABLES` is enabled. | `1800` | Optional |
| `HIDE_ITEM_PATH` | Path to boolean field that marks items as hidden (excluded from search) or not. If null, the item is returned. | `None` | Optional |
| `EXCLUDED_FROM_QUERYABLES` | Comma-separated list of fully qualified field names to exclude from the queryables endpoint and filtering. Use full paths like `properties.auth:schemes,properties.storage:schemes`. Excluded fields and their nested children will not be exposed in queryables. | None | Optional |
| `EXCLUDED_FROM_ITEMS` | Specifies fields to exclude from STAC item responses. Supports comma-separated field names and dot notation for nested fields (e.g., `private_data,properties.confidential,assets.internal`). | `None` | Optional |

> [!NOTE]
> The variables `ES_HOST`, `ES_PORT`, `ES_USE_SSL`, `ES_VERIFY_CERTS` and `ES_TIMEOUT` apply to both Elasticsearch and OpenSearch backends, so there is no need to rename the key names to `OS_` even if you're using OpenSearch.

## Redis for Navigation environment variables:
These Redis configuration variables to enable proper navigation functionality in STAC FastAPI.

| Variable | Description| Default| Required|
|----------|------------|--------|---------|
| `REDIS_ENABLE` | Enables or disables Redis caching for navigation. Set to `true` to use Redis, or `false` to disable. | `false` | **Required** (determines whether Redis is used at all) |
| **Redis Sentinel** |    |     |    |
| `REDIS_SENTINEL_HOSTS` | Comma-separated list of Redis Sentinel hostnames/IP addresses. | `""`                     | Conditional (required if using Sentinel)                                                    |
| `REDIS_SENTINEL_PORTS` | Comma-separated list of Redis Sentinel ports (must match order). | `"26379"`                | Conditional (required if using Sentinel)                                                    |
| `REDIS_SENTINEL_MASTER_NAME` | Name of the Redis master node in Sentinel configuration. | `"master"`               | Conditional (required if using Sentinel)                                                    |
| **Redis** |                                                                                              |                          |                                                                                             |
| `REDIS_HOST` | Redis server hostname or IP address for Redis configuration. | `""`                     | Conditional (required for standalone Redis)                                                 |
| `REDIS_PORT` | Redis server port for Redis configuration. | `6379`                   | Conditional (required for standalone Redis)                                                 |
| **Both** |                                                                                              |                          |                                                                                             |
| `REDIS_DB` | Redis database number to use for caching.                                                    | `0` (Sentinel) / `15` (Standalone) | Optional                                                                                    |
| `REDIS_MAX_CONNECTIONS` | Maximum number of connections in the Redis connection pool.                                  | `10`                     | Optional                                                                                    |
| `REDIS_RETRY_TIMEOUT` | Enable retry on timeout for Redis operations.                                                | `true`                   | Optional                                                                                    |
| `REDIS_DECODE_RESPONSES`      | Automatically decode Redis responses to strings.                                             | `true`                   | Optional                                                                                    |
| `REDIS_CLIENT_NAME`           | Client name identifier for Redis connections.                                                | `"stac-fastapi-app"`     | Optional                                                                                    |
| `REDIS_HEALTH_CHECK_INTERVAL` | Interval in seconds for Redis health checks.                                                 | `30`                     | Optional                                                                                    |
| `REDIS_SELF_LINK_TTL` | Time-to-live (TTL) in seconds for storing self-links in Redis, used for pagination caching. | 1800 | Optional |


> [!NOTE]
> Use either the Sentinel configuration (`REDIS_SENTINEL_HOSTS`, `REDIS_SENTINEL_PORTS`, `REDIS_SENTINEL_MASTER_NAME`) OR the Redis configuration (`REDIS_HOST`, `REDIS_PORT`), but not both.

## Excluding Fields from Queryables

You can exclude specific fields from being exposed in the queryables endpoint and from filtering by setting the `EXCLUDED_FROM_QUERYABLES` environment variable. This is useful for hiding sensitive or internal fields that should not be queryable by API users.

**Environment Variable:**

```bash
EXCLUDED_FROM_QUERYABLES="properties.auth:schemes,properties.storage:schemes,properties.internal:metadata"
```

**Format:**

- Comma-separated list of fully qualified field names
- Use the full path including the `properties.` prefix for item properties
- Example field names:
  - `properties.auth:schemes`
  - `properties.storage:schemes`

**Behavior:**

- Excluded fields will not appear in the queryables response
- Excluded fields and their nested children will be skipped during field traversal
- Both the field itself and any nested properties will be excluded

## Queryables Validation

SFEOS supports validating query parameters against the collection's defined queryables. This ensures that users only query fields that are explicitly exposed and indexed.

**Configuration:**

To enable queryables validation, set the following environment variables:

```bash
VALIDATE_QUERYABLES=true
QUERYABLES_CACHE_TTL=1800  # Optional, defaults to 1800 seconds (30 minutes)
```

**Behavior:**

- When enabled, the API maintains a cache of all queryable fields across all collections.
- Search requests (both GET and POST) are checked against this cache.
- If a request contains a query parameter or filter field that is not in the list of allowed queryables, the API returns a `400 Bad Request` error with a message indicating the invalid field(s).
- The cache is automatically refreshed based on the `QUERYABLES_CACHE_TTL` setting.
- **Interaction with `EXCLUDED_FROM_QUERYABLES`**: If `VALIDATE_QUERYABLES` is enabled, fields listed in `EXCLUDED_FROM_QUERYABLES` will also be considered invalid for filtering. This effectively enforces the exclusion of these fields from search queries.

This feature helps prevent queries on non-queryable fields which could lead to unnecessary load on the database.

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
       -H 'Content-Type: application/json' \
       -d $'{
    "id": "my_collection"
  }'
  ```

- **Adding an Item to a Collection**:
  ```shell
  curl -X "POST" "http://localhost:8080/collections/my_collection/items" \
       -H 'Content-Type: application/json' \
       -d @item.json
  ```

- **Searching for Items**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json' \
       -d $'{
    "collections": ["my_collection"],
    "limit": 10
  }'
  ```

- **Filtering by Bbox**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json' \
       -d $'{
    "collections": ["my_collection"],
    "bbox": [-180, -90, 180, 90]
  }'
  ```

- **Filtering by Datetime**:
  ```shell
  curl -X "GET" "http://localhost:8080/search" \
       -H 'Content-Type: application/json' \
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

## SFEOS Tools CLI

- **Overview**: [SFEOS Tools](https://github.com/Healy-Hyperspatial/sfeos-tools) is an installable CLI package for managing and maintaining SFEOS deployments. This CLI package provides utilities for managing and maintaining SFEOS deployments.

- **Installation**:
  ```shell
  # For Elasticsearch (from PyPI)
  pip install sfeos-tools[elasticsearch]
  
  # For OpenSearch (from PyPI)
  pip install sfeos-tools[opensearch]
  
  ```

- **Available Commands**:
  - `add-bbox-shape`: Add bbox_shape field to existing collections for spatial search support
  - `reindex`: Reindex all STAC indices (collections and per-collection items) to new versioned indices and update aliases; supports both Elasticsearch and OpenSearch backends. Use this when you need to apply mapping changes, update index settings, or migrate to a new index structure. The command handles the entire process including creating new indices, reindexing data, and atomically updating aliases with zero downtime.

- **Basic Usage**:
  ```shell
  sfeos-tools add-bbox-shape --backend elasticsearch
  sfeos-tools add-bbox-shape --backend opensearch
  ```

- **Connection Options**: Configure database connection via CLI flags or environment variables:
  - `--host`: Database host (default: `localhost` or `ES_HOST` env var)
  - `--port`: Database port (default: `9200` or `ES_PORT` env var)
  - `--use-ssl` / `--no-ssl`: Use SSL connection (default: `true` or `ES_USE_SSL` env var)
  - `--user`: Database username (default: `ES_USER` env var)
  - `--password`: Database password (default: `ES_PASS` env var)

- **Examples**:
  ```shell
  # Local Docker Compose (no SSL)
  sfeos-tools add-bbox-shape --backend elasticsearch --no-ssl
  
  # Remote server with SSL
  sfeos-tools add-bbox-shape \
    --backend elasticsearch \
    --host db.example.com \
    --port 9200 \
    --user admin \
    --password secret
  
  # Cloud deployment with environment variables
  ES_HOST=my-es-cluster.cloud.com ES_PORT=9243 ES_USER=elastic ES_PASS=changeme \
    sfeos-tools add-bbox-shape --backend elasticsearch
  
  # Using --help for more information
  sfeos-tools --help
  sfeos-tools add-bbox-shape --help
  sfeos-tools reindex --help

  ```

- **Documentation**:
  For complete documentation, examples, and advanced usage, please visit the [SFEOS Tools GitHub repository](https://github.com/Healy-Hyperspatial/sfeos-tools).

- **Contributing**:
  Contributions, bug reports, and feature requests are welcome! Please file them on the [SFEOS Tools issue tracker](https://github.com/Healy-Hyperspatial/sfeos-tools/issues).

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

## Redis for Navigation

The Redis cache stores navigation state for paginated results, allowing the system to maintain previous page links using tokens. The configuration supports both Redis Sentinel and standalone Redis setups.

Steps to configure:
1. Ensure that a Redis instance is available, either a standalone server or a Sentinel-managed cluster.
2. Establish a connection between STAC FastAPI and Redis instance by setting the appropriate [**environment variables**](#redis-for-navigation-environment-variables). These define the Redis host, port, authentication, and optional Sentinel settings.
3. Control whether Redis caching is activated using the `REDIS_ENABLE` environment variable to `True` or `False`.
4. Ensure the appropriate version of `Redis` is installed:
```
pip install stac-fastapi-elasticsearch[redis]
```

## Elasticsearch Mappings

- **Overview**: Mappings apply to search index, not source data. They define how documents and their fields are stored and indexed.
- **Implementation**: 
  - Mappings are stored in index templates that are created on application startup
  - These templates are automatically applied when creating new Collection and Item indices
  - The `sfeos_helpers` package contains shared mapping definitions used by both Elasticsearch and OpenSearch backends
- **Customization**: Custom mappings can be defined by extending the base mapping templates.

## Custom Index Mappings

SFEOS provides environment variables to customize Elasticsearch/OpenSearch index mappings without modifying source code. This is useful for:

- Adding STAC extension fields (SAR, Cube, etc.) with proper types
- Optimizing performance by controlling which fields are indexed
- Ensuring correct field types instead of relying on dynamic mapping inference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` | JSON string of property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_MAPPINGS_FILE` | Path to a JSON file containing property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_DYNAMIC_MAPPING` | Controls dynamic mapping: `true`, `false`, or `strict` | `true` |

### Custom Mappings

You can customize the Elasticsearch/OpenSearch mappings by providing a JSON configuration. This can be done via:

1. `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` environment variable (takes precedence)
2. `STAC_FASTAPI_ES_MAPPINGS_FILE` environment variable (file path)

The configuration should have the same structure as the default ES mappings. The custom mappings are **recursively merged** with the defaults at the root level.

#### Merge Behavior

The merge follows these rules:

| Scenario | Result |
|----------|--------|
| Key only in defaults | Preserved |
| Key only in custom | Added |
| Key in both, both are dicts | Recursively merged |
| Key in both, values are not both dicts | **Custom overwrites default** |

**Example - Adding new properties (merged):**

```json
// Default has: {"geometry": {"type": "geo_shape"}}
// Custom has:  {"geometry": {"ignore_malformed": true}}
// Result:      {"geometry": {"type": "geo_shape", "ignore_malformed": true}}
```

**Example - Overriding a value (replaced):**

```json
// Default has: {"properties": {"datetime": {"type": "date_nanos"}}}
// Custom has:  {"properties": {"datetime": {"type": "date"}}}
// Result:      {"properties": {"datetime": {"type": "date"}}}
```

#### JSON Structure

The custom JSON should mirror the structure of the default mappings. For STAC item properties, the path is `properties.properties.properties`:

```
{
  "numeric_detection": false,
  "dynamic_templates": [...],
  "properties": {                    # Top-level ES mapping properties
    "id": {...},
    "geometry": {...},
    "properties": {                  # STAC item "properties" field
      "type": "object",
      "properties": {                # Nested properties within STAC properties
        "datetime": {...},
        "sar:frequency_band": {...}  # <-- Custom extension fields go here
      }
    }
  }
}
```

**Example - Adding SAR Extension Fields:**

```bash
export STAC_FASTAPI_ES_CUSTOM_MAPPINGS='{
  "properties": {
    "properties": {
      "properties": {
        "sar:frequency_band": {"type": "keyword"},
        "sar:center_frequency": {"type": "float"},
        "sar:polarizations": {"type": "keyword"},
        "sar:product_type": {"type": "keyword"}
      }
    }
  }
}'
```

**Example - Adding Cube Extension Fields:**

```bash
export STAC_FASTAPI_ES_CUSTOM_MAPPINGS='{
  "properties": {
    "properties": {
      "properties": {
        "cube:dimensions": {"type": "object", "enabled": false},
        "cube:variables": {"type": "object", "enabled": false}
      }
    }
  }
}'
```

**Example - Adding geometry options:**

```bash
export STAC_FASTAPI_ES_CUSTOM_MAPPINGS='{
  "properties": {
    "geometry": {"ignore_malformed": true}
  }
}'
```

**Example - Using a mappings file (recommended for complex configurations):**

Instead of passing large JSON blobs via environment variables, you can use a file:

```bash
# Create a mappings file
cat > custom-mappings.json <<EOF
{
  "properties": {
    "properties": {
      "properties": {
        "sar:frequency_band": {"type": "keyword"},
        "sar:center_frequency": {"type": "float"},
        "sar:polarizations": {"type": "keyword"},
        "sar:product_type": {"type": "keyword"},
        "eo:cloud_cover": {"type": "float"},
        "platform": {"type": "keyword"}
      }
    }
  }
}
EOF

# Reference the file
export STAC_FASTAPI_ES_MAPPINGS_FILE=/path/to/custom-mappings.json
```

In Docker Compose, you can mount the file:

```yaml
services:
  app-elasticsearch:
    volumes:
      - ./custom-mappings.json:/app/mappings.json:ro
    environment:
      - STAC_FASTAPI_ES_MAPPINGS_FILE=/app/mappings.json
```

In Kubernetes, use a ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: stac-mappings
data:
  mappings.json: |
    {
      "properties": {
        "properties": {
          "properties": {
            "platform": {"type": "keyword"},
            "eo:cloud_cover": {"type": "float"}
          }
        }
      }
    }
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: stac-fastapi
        env:
        - name: STAC_FASTAPI_ES_MAPPINGS_FILE
          value: /etc/stac/mappings.json
        volumeMounts:
        - name: mappings
          mountPath: /etc/stac
      volumes:
      - name: mappings
        configMap:
          name: stac-mappings
```

> [!TIP]
> If both `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` and `STAC_FASTAPI_ES_MAPPINGS_FILE` are set, the environment variable takes precedence, allowing quick overrides during testing or troubleshooting.

### Dynamic Mapping Control (`STAC_FASTAPI_ES_DYNAMIC_MAPPING`)

Controls how Elasticsearch/OpenSearch handles fields not defined in the mapping:

| Value | Behavior |
|-------|----------|
| `true` (default) | New fields are automatically added to the mapping. Maintains backward compatibility. |
| `false` | New fields are ignored and not indexed. Documents can still contain these fields, but they won't be searchable. |
| `strict` | Documents with unmapped fields are rejected. |

### Combining Both Variables for Performance Optimization

For large datasets with extensive metadata that isn't queried, you can disable dynamic mapping and define only the fields you need:

```bash
# Disable dynamic mapping
export STAC_FASTAPI_ES_DYNAMIC_MAPPING=false

# Define only queryable fields
export STAC_FASTAPI_ES_CUSTOM_MAPPINGS='{
  "properties": {
    "properties": {
      "properties": {
        "platform": {"type": "keyword"},
        "eo:cloud_cover": {"type": "float"},
        "view:sun_elevation": {"type": "float"}
      }
    }
  }
}'
```

This prevents Elasticsearch from creating mappings for unused metadata fields, reducing index size and improving ingestion performance.

> [!NOTE]
> These environment variables apply to both Elasticsearch and OpenSearch backends. Changes only affect newly created indices. For existing indices, you'll need to reindex using [SFEOS-tools](https://github.com/Healy-Hyperspatial/sfeos-tools).

> [!WARNING]
> Use caution when overriding core fields like `geometry`, `datetime`, or `id`. Incorrect types may cause search failures or data loss.

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

## Error Monitoring with Sentry

Optional integration with Sentry for error tracking, performance monitoring, and release tracking. When enabled, Sentry provides real-time insights into application errors, performance bottlenecks, and deployment health.

| Variable | Description | Default |
|----------|-------------|---------|
| `SENTRY_ENABLE` | Enable Sentry integration for error tracking and performance monitoring | `false` |
| `SENTRY_DSN` | Sentry Data Source Name (DSN) for your project | `None` |
| `SENTRY_ENVIRONMENT` | Deployment environment (production, staging, development) | `staging` |
| `SENTRY_TRACES_SAMPLE_RATE` | Percentage of transactions to sample for performance monitoring (0.0 to 1.0) | `0.1` |

## Hidden Items Filtering

SFEOS supports filtering out hidden items using the `HIDE_ITEM_PATH` environment variable. This feature is useful for temporarily removing items from search results without deleting them. To configure it, set `HIDE_ITEM_PATH` to the path of a boolean field in STAC items. Items where this field is `true` will be excluded from all results and counts.

To use this feature, set the environment variable:
  ```
  export HIDE_ITEM_PATH="properties._private.hidden"
  ```

The following item will be excluded from returned results:
  ```
  {
    "id": "item-example",
    "properties": {
      "_private": {
        "hidden": true
      }
    }
  }
  ```
