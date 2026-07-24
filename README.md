<!-- markdownlint-disable MD033 MD041 -->


  <img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/sfeos.png" width=1000>

**Jump to:** [Project Introduction](#project-introduction---what-is-sfeos) | [Quick Start](#quick-start) | [Table of Contents](#table-of-contents) | [SFEOS-tools CLI](#sfeos-tools-cli) |

  [![Downloads](https://static.pepy.tech/badge/stac-fastapi-core?color=blue)](https://pepy.tech/project/stac-fastapi-core)
  [![GitHub contributors](https://img.shields.io/github/contributors/stac-utils/stac-fastapi-elasticsearch-opensearch?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/graphs/contributors)
  [![GitHub stars](https://img.shields.io/github/stars/stac-utils/stac-fastapi-elasticsearch-opensearch.svg?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/stargazers)
  [![GitHub forks](https://img.shields.io/github/forks/stac-utils/stac-fastapi-elasticsearch-opensearch.svg?color=blue)](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/network/members)
   [![PyPI version](https://img.shields.io/pypi/v/stac-fastapi-elasticsearch.svg?color=blue)](https://pypi.org/project/stac-fastapi-elasticsearch/)
  [![STAC](https://img.shields.io/badge/STAC-1.1.0-blue.svg)](https://github.com/radiantearth/stac-spec/tree/v1.1.0)
  [![stac-fastapi](https://img.shields.io/badge/stac--fastapi-6.4.1-blue.svg)](https://github.com/stac-utils/stac-fastapi)

## Sponsors & Supporters

The following organizations have contributed time and/or funding to support the development of this project:

<a href="https://healy-hyperspatial.github.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/hh-logo-blue.png" alt="Healy Hyperspatial" height="100" hspace="20"></a>
<a href="https://atomicmaps.io/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/am-logo-black.png" alt="Atomic Maps" height="100" hspace="20"></a>
<a href="https://remotesensing.vito.be/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/VITO.png" alt="VITO Remote Sensing" height="100" hspace="20"></a>
<a href="https://cloudferro.com/"><img src="https://raw.githubusercontent.com/stac-utils/stac-fastapi-elasticsearch-opensearch/refs/heads/main/assets/cloudferro-logo.png" alt="CloudFerro" height="105" hspace="20"></a>

## Latest News

- **06/21/2026:** 🔍 **Catalogs Search Extension!** Search items across entire catalog hierarchies with the new `/catalogs/{catalog_id}/search` endpoint. This powerful feature uses **BFS recursive DAG traversal** to automatically discover all descendant collections within a catalog's subtree (including nested sub-catalogs) and searches items across all of them. Enforces strict scope boundaries with 403 Forbidden for out-of-scope collection requests. Supports all standard STAC search parameters (spatial filtering, temporal filtering, free-text search, CQL2 filtering, sorting, and pagination). Perfect for scoped item discovery within organizational hierarchies! Implements the [STAC API - Multi-Tenant Catalogs Scoped Search](https://github.com/StacLabs/multi-tenant-catalogs#scoped-search-recursive-traversal) specification.
- **06/14/2026:** 🛡️ **Native STAC & Topology Validation in v6.18.0!** SFEOS now features built-in, strict STAC schema validation via the Python `stac-validator` package (available via the new `[validator]` install extra). We've also introduced a blazing-fast, pure-Python spatial topology checker to protect your database from invalid coordinates and uncut antimeridian polygons. To support massive ingestion workloads, these powerful new firewalls include configurable chunking (`MAX_BATCH_SIZE`), fail-fast error thresholds, and Redis queue deference controls (`VALIDATE_BEFORE_QUEUE`). Finally, multi-tenant deployments gain a security boost with the new `HIDE_ALTERNATE_PARENTS` poly-hierarchy privacy toggle. 🙏 Huge thanks to **CloudFerro** for their continued sponsorship of these robust new features!
- **03/19/2026: SKOS to STAC Ingestion Demo.** 📓 Check out the interactive [SKOS-catalogs-ingestion-demo.ipynb](https://github.com/StacLabs/sfeos-tools/blob/main/demo-notebooks/SKOS-catalogs-ingestion-demo.ipynb) notebook! This tutorial demonstrates automated semantic ingestion from SKOS/RDF-XML files into hierarchical STAC catalogs, showcasing poly-hierarchy, contextual breadcrumbs, and data safety features of the Multi-Tenant Catalogs extension. Thanks to support from CloudFerro! 
- **01/11/2026: Hierarchical Catalog Support.** Sub-catalogs are now fully supported! Catalogs can now contain other catalogs for unlimited nesting levels. This enables complex organizational hierarchies with multi-parent support for both catalogs and collections.
- **01/09/2026: Custom Index Mappings.** You can now customize Elasticsearch/OpenSearch index mappings directly via environment variables without changing source code. Use `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` to merge custom field definitions (e.g., for STAC extensions like SAR or Cube) or `STAC_FASTAPI_ES_MAPPINGS_FILE` to load mappings from a JSON file. See [Custom Index Mappings](#custom-index-mappings) for details.
- **12/09/2025: Multi-Tenant Catalogs.** The [`STAC API - Multi-Tenant Catalogs Endpoint Extension`](https://github.com/stac-api-extensions/multi-tenant-catalogs) is now in main! This enables a registry of catalogs and supports **poly-hierarchy** (collections belonging to multiple catalogs simultaneously). Enable it via `ENABLE_CATALOGS_ROUTE`. _Coming next: Support for nested sub-catalogs._
- **11/07/2025:** 🌍 The SFEOS STAC Viewer is now available at: https://healy-hyperspatial.github.io/sfeos-web. Use this site to examine your data and test your STAC API!
- **10/24/2025:** Added `previous_token` pagination using Redis for efficient navigation. This feature allows users to navigate backwards through large result sets by storing pagination state in Redis. To use this feature, ensure Redis is configured (see [Redis for navigation](#redis-for-navigation)) and set `REDIS_ENABLE=true` in your environment.
- **10/23/2025:** The `EXCLUDED_FROM_QUERYABLES` environment variable was added to exclude fields from the `queryables` endpoint. See [docs](#excluding-fields-from-queryables).

<details style="border: 1px solid #eaecef; border-radius: 6px; padding: 10px; margin-bottom: 16px; background-color: #f9f9f9;">
<summary style="cursor: pointer; font-weight: bold; margin: -10px -10px 0; padding: 10px; background-color: #f0f0f0; border-bottom: 1px solid #eaecef; border-top-left-radius: 6px; border-top-right-radius: 6px;">View Older News (Click to Expand)</summary>

-------------
- **10/15/2025:** 🚀 SFEOS Tools v0.1.0 Released! - The new `sfeos-tools` CLI is now available on [PyPI](https://pypi.org/project/sfeos-tools/)
- **10/15/2025:** Added `reindex` command to **[SFEOS-tools](https://github.com/Healy-Hyperspatial/sfeos-tools)** for zero-downtime index updates when changing mappings or settings. The new `reindex` command makes it easy to apply mapping changes, update index settings, or migrate to new index structures without any service interruption, ensuring high availability of your STAC API during maintenance operations.
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
  - [Injecting Custom Extensions](#injecting-custom-extensions-out-of-tree)
  - [Custom Pydantic Settings](#custom-pydantic-settings)
  - [Catalogs Search Extension](#catalogs-search-extension)
  - [Documentation & Resources](#documentation--resources)
  - [SFEOS STAC Viewer](#sfeos-stac-viewer)
  - [Package Structure](#package-structure)
  - [Examples](#examples)
  - [Performance](#performance)
    - [Direct Response Mode](#direct-response-mode)
    - [CQL2 JSON Search with AST-based Parsing](#cql2-json-search-with-ast-based-parsing)
  - [Quick Start](#quick-start)
    - [Installation](#installation)
    - [Running Locally](#running-locally)
      - [Using Pre-built Docker Images](#using-pre-built-docker-images)
      - [Using Docker Compose](#using-docker-compose)
  - [Configuration Reference](#configuration-reference)
  - [STAC Validation](#stac-validation)
  - [Free-Text Search (`q` parameter)](#free-text-search-q-parameter)
  - [Queryables Endpoint](#queryables-endpoint)
    - [Root Queryables Configuration](#root-queryables-configuration)
    - [Excluding Fields from Queryables](#excluding-fields-from-queryables)
    - [Queryables Validation](#queryables-validation)
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
  - [Sorting and Time-Range Items (`datetime: null`)](#sorting-and-time-range-items-datetime-null)
  - [SFEOS Tools CLI](#sfeos-tools-cli)
  - [Redis for navigation](#redis-for-navigation)
  - [Elasticsearch Mappings](#elasticsearch-mappings)
  - [Custom Index Mappings](#custom-index-mappings)
  - [Managing Elasticsearch Indices](#managing-elasticsearch-indices)
    - [Snapshots](#snapshots)
    - [Reindexing](#reindexing)
  - [Auth](#auth)
  - [Aggregation](#aggregation)
  - [Rate Limiting](#rate-limiting)
  - [Prometheus metrics](#prometheus-metrics)
  - [Hidden Items Filtering](#hidden-items-filtering)
  - [Error Monitoring with Sentry](#error-monitoring-with-sentry)

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

### Installation

To use the Catalogs extension, install the Elasticsearch or OpenSearch package with the catalogs extra:

```bash
# For Elasticsearch backend
pip install stac-fastapi-elasticsearch[catalogs]

# For OpenSearch backend
pip install stac-fastapi-opensearch[catalogs]
```

Alternatively, if you're installing the core package directly:

```bash
pip install stac-fastapi-core[catalogs]
```

This ensures you have the `stac-fastapi-catalogs-extension` dependency required for the `/catalogs` endpoint.

### DAG Specification & Dynamic Link Generation

SFEOS implements the [STAC API - Multi-Tenant Catalogs Endpoint Extension](https://github.com/stac-api-extensions/multi-tenant-catalogs) (v1.0.0-beta.4) with full support for Directed Acyclic Graph (DAG) structures and strict STAC core compliance:

#### Link Relations

All link relations are generated dynamically at runtime based on the `parent_ids` field and request context:

- **`rel="parent"`** - Exactly one parent link per resource, context-aware:
  - Global endpoints (`/collections/{id}`, `/catalogs/{id}`): Points to root `/` or first parent
  - Scoped endpoints (`/catalogs/{id}/collections/{id}`): Points to the contextual catalog
  - Ensures proper breadcrumb navigation in STAC Browser

- **`rel="related"`** - Alternative parents in poly-hierarchy:
  - Exposes all other parent catalogs beyond the contextual parent
  - Allows advanced clients to discover the full organizational graph
  - Only included when a resource has multiple parents

- **`rel="canonical"`** - Authoritative global endpoint:
  - Points to the primary, global URI for the resource
  - Example: `/catalogs/{id}/collections/{id}` → canonical: `/collections/{id}`
  - Enables clients to deduplicate resources across different contexts

- **`rel="duplicate"`** - Alternative scoped URIs (RFC 6249):
  - Lists all parent-scoped endpoints where the resource can be accessed
  - Example: Collection in 2 catalogs has duplicate links to both scoped URIs
  - Helps clients identify identical resources in different organizational contexts

- **`rel="child"`** - Direct children:
  - Generated dynamically by querying the database for actual children
  - Never persisted statically, preventing stale links
  - Enables STAC Browser folder navigation

#### Contextual vs Global Navigation

**Global Endpoints** (`/collections/{id}`):

- Parent → root `/`
- Related → all catalog parents
- Canonical → self
- Duplicate → all scoped URIs

**Scoped Endpoints** (`/catalogs/{id}/collections/{id}`):

- Parent → contextual catalog
- Related → other catalog parents
- Canonical → global endpoint
- Duplicate → all scoped URIs

**Key Principle**: No static links are persisted in the database. All relationships are computed on-the-fly based on the `parent_ids` array, ensuring data consistency and preventing orphaned references.

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
- **POST `/catalogs/{catalog_id}/collections`**: Create a new collection within a catalog OR link an existing collection by posting its ID
- **GET `/catalogs/{catalog_id}/collections/{collection_id}`**: Retrieve a specific collection within a catalog
- **PUT `/catalogs/{catalog_id}/collections/{collection_id}`**: Update a collection within a catalog context (updates the collection globally)
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

# Link an EXISTING collection to a catalog
# Simply POST the collection ID to add it to the catalog
curl -X POST "http://localhost:8081/catalogs/earth-observation/collections" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "existing-collection-id"
  }'

# Get specific collection within a catalog
curl "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2"

# Update a collection within a catalog context
# This updates the collection globally (not just within this catalog)
# The update preserves the collection's parent_ids, maintaining its DAG structure
# and poly-hierarchy relationships across all parent catalogs
curl -X PUT "http://localhost:8081/catalogs/earth-observation/collections/sentinel-2" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "sentinel-2",
    "type": "Collection",
    "stac_version": "1.0.0",
    "description": "Updated description for Sentinel-2 data",
    "title": "Sentinel-2 (Updated)"
  }'

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

#### Important: Flat Catalog URL Structure

All catalogs are accessed via the **flat canonical endpoint** `/catalogs/{catalog_id}`, regardless of their position in the hierarchy. Nested routes like `/catalogs/id1/catalogs/id2` are **not supported**. This ensures consistent, cacheable URLs regardless of catalog depth or parent relationships.

To discover a catalog's children, use `/catalogs/{catalog_id}/catalogs` or `/catalogs/{catalog_id}/children`, which returns links to the child catalogs' canonical endpoints.

#### Example: Creating a "Forestry" Playlist

Imagine you have an existing catalog `sentinel-2`. You want to create a curated "Forestry" catalog that includes this existing data.

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

# 3. Access the sentinel-2 catalog via its canonical endpoint
curl "http://localhost:8081/catalogs/sentinel-2"

# 4. Discover sentinel-2 as a child of forestry
curl "http://localhost:8081/catalogs/forestry/catalogs"
# This returns links to /catalogs/sentinel-2 (canonical endpoint)
```

**Result:** The sentinel-2 catalog now has multiple parents (including forestry). It is always accessed via its canonical endpoint:

- `/catalogs/sentinel-2`

You can discover it as a child of forestry via:

- `/catalogs/forestry/catalogs` (lists sentinel-2 with a link to `/catalogs/sentinel-2`)

Because you are linking the node (the Catalog), the entire sub-tree attached to that node is automatically shared. If sentinel-2 contains millions of items and sub-catalogs, they are all instantly visible under the new forestry parent without needing to re-link individual items.

> **Configuration**: The catalogs route can be enabled or disabled by setting the `ENABLE_CATALOGS_ROUTE` environment variable to `true` or `false`. By default, this endpoint is **disabled**.

## Injecting Custom Extensions (Out-of-Tree)

If you need to add deployment-specific routes such as custom analytics, billing, or map tiles, SFEOS lets you inject custom extensions without forking or modifying the core repository.

By leveraging the `extra_map` parameter during application instantiation, your custom endpoints are mounted alongside the built-in STAC API routes and included in the OpenAPI schema.

For a practical real-world example of wiring routes, models, and dependencies in a standalone extension, see the [Catalogs Endpoint extension](https://github.com/StacLabs/stac-fastapi-catalogs-extension).

### 1. Define Your Custom Extension

Create a class that inherits from `ApiExtension` and bind your FastAPI routes inside `register()`.

This example shows a lightweight Vector Tile extension using the native Elasticsearch/OpenSearch `_mvt` API pattern:

```python
from fastapi import APIRouter
from stac_fastapi.types.extension import ApiExtension


class MVTExtension(ApiExtension):
    """Example extension serving Vector Tiles directly from the search engine."""

    def register(self, app):
        router = APIRouter()

        @router.get("/api/map/{z}/{x}/{y}.mvt")
        async def get_mvt(z: int, x: int, y: int):
            # Your custom Elasticsearch/OpenSearch _mvt generation logic here
            return {"tile": "data"}

        app.include_router(router)
```

### 2. Inject It Into the Application Factory

Once your extension is defined, instantiate the `Extensions` manager, pass your custom class into `extra_map`, and hand that manager to `instantiate_api()`.

```python
from stac_fastapi.elasticsearch.app import instantiate_api
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
from stac_fastapi.core.session import Session
from stac_fastapi.sfeos_helpers.models.extensions import Extensions
from your_project.extensions import MVTExtension


settings = ElasticsearchSettings()
database_logic = DatabaseLogic()
session = Session.create_from_settings(settings)

custom_extensions = Extensions(
    settings=settings,
    database_logic=database_logic,
    session=session,
    extra_map={
        "mvt": MVTExtension(),
    },
)

api = instantiate_api(
    settings=settings,
    database_logic=database_logic,
    extensions_manager=custom_extensions,
)
app = api.app
```

The key idea is simple: build the extension class, add it to `extra_map`, and pass the resulting manager into the backend factory.

## Custom Pydantic Settings

If you want to override backend defaults programmatically, pass a custom settings object into `instantiate_api()` as well. This is useful when you want to set feature flags or connection defaults in code instead of relying only on environment variables.

The backend factories accept concrete settings classes such as `ElasticsearchSettings` and `OpensearchSettings`, so you can subclass them or instantiate them directly before building the app.

Tip: You can also override these directly during instantiation, for example `settings = ElasticsearchSettings(enable_catalogs_route=True)`.

```python
from stac_fastapi.elasticsearch.app import instantiate_api
from stac_fastapi.elasticsearch.config import ElasticsearchSettings


class CustomElasticsearchSettings(ElasticsearchSettings):
    enable_catalogs_route: bool = True
    enable_collections_search_route: bool = True


settings = CustomElasticsearchSettings()
api = instantiate_api(settings=settings)
app = api.app
```

For most deployments, the same pattern applies if you are using the OpenSearch backend: import the OpenSearch settings class, customize the values you need, and pass the resulting instance into `instantiate_api()`.

## Catalogs Search Extension

The **Catalogs Search Extension** enables searching for items across an entire catalog's subtree, including all sub-catalogs and their collections. This is similar to the global items search, but scoped to a specific catalog hierarchy.

This implementation follows the [STAC API - Multi-Tenant Catalogs Scoped Search (Recursive Traversal)](https://github.com/StacLabs/multi-tenant-catalogs#scoped-search-recursive-traversal) specification, which defines how to safely search items within catalog hierarchies while enforcing scope boundaries.

### Overview

The catalogs search extension provides a powerful way to discover items within organizational hierarchies without needing to know which specific collections contain them. It automatically traverses the catalog DAG (Directed Acyclic Graph) to find all descendant collections and searches items across all of them.

### Key Features

- **Subtree Search**: Search items across a catalog and all its sub-catalogs
- **Rich Query Support**: All standard STAC search parameters are supported
- **Spatial Filtering**: Filter items by bounding box
- **Temporal Filtering**: Filter items by datetime range
- **Free-Text Search**: Search item properties with the `q` parameter
- **CQL2 Filtering**: Advanced structured filtering with CQL2 expressions
- **Sorting**: Sort results by any indexed field
- **Pagination**: Navigate large result sets with limit and token parameters
- **DAG Traversal**: Automatic breadth-first search through catalog hierarchies
- **Scope Enforcement**: Requests are automatically restricted to the catalog's descendant collections

### Endpoints

**Search Endpoints:**

- **GET `/catalogs/{catalog_id}/search`**: Search items using query parameters
- **POST `/catalogs/{catalog_id}/search`**: Search items using a JSON request body (supports large payloads)

### Supported Parameters

The catalogs search endpoints support all standard STAC search parameters:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `collections` | array | Limit search to specific collections within the catalog | `?collections=sentinel-2,landsat-9` |
| `ids` | array | Search for specific item IDs | `?ids=item-1,item-2` |
| `bbox` | array | Bounding box filter `[minx,miny,maxx,maxy]` | `?bbox=-10,35,40,70` |
| `intersects` | string | GeoJSON geometry filter | `?intersects={"type":"Point","coordinates":[0,0]}` |
| `datetime` | string | Datetime range filter | `?datetime=2020-01-01T00:00:00Z/2020-12-31T23:59:59Z` |
| `q` | string | Free-text search across item properties | `?q=landsat` |
| `filter` | string | CQL2 filter expression (JSON or text) | `?filter={"op":"=","args":[{"property":"eo:bands"},1]}` |
| `filter-lang` | string | CQL2 format: `cql2-json` or `cql2-text` | `?filter-lang=cql2-json` |
| `sortby` | string | Sort results by field | `?sortby=-datetime` |
| `fields` | string | Select specific fields to return | `?fields=id,properties.datetime` |
| `limit` | integer | Maximum results per page (default: 10) | `?limit=50` |
| `token` | string | Pagination token for next page | `?token=<pagination-token>` |

### How It Works

The catalogs search extension uses a **breadth-first search (BFS) DAG traversal** to discover all descendant collections:

1. **Catalog Validation**: Verifies the requested catalog exists
2. **DAG Traversal**: Performs BFS through the catalog hierarchy using the `parent_ids` field
   - Starts from the requested catalog
   - Finds all direct children (both sub-catalogs and collections)
   - Recursively traverses sub-catalogs to find their children
   - Collects all descendant collections
3. **Scope Enforcement**: Restricts search to only the descendant collections
4. **Query Delegation**: Passes the scoped search to the core search engine
5. **Result Return**: Returns items matching the search criteria

### Usage Examples

```bash
# Search all items in a catalog
curl "http://localhost:8081/catalogs/earth-observation/search?q=landsat"

# Search with spatial and temporal filters
curl "http://localhost:8081/catalogs/earth-observation/search?bbox=-180,-90,180,90&datetime=2020-01-01T00:00:00Z/2020-12-31T23:59:59Z"

# Search specific collections within a catalog
curl "http://localhost:8081/catalogs/earth-observation/search?collections=sentinel-2,landsat-9&limit=50"

# Advanced CQL2 filtering
curl "http://localhost:8081/catalogs/earth-observation/search?filter=eo:cloud_cover%3C20&filter-lang=cql2-text"

# Free-text search with sorting
curl "http://localhost:8081/catalogs/earth-observation/search?q=forest&sortby=-datetime"

# POST request with complex search (supports large payloads)
curl -X POST "http://localhost:8081/catalogs/earth-observation/search" \
  -H "Content-Type: application/json" \
  -d '{
    "bbox": [-180, -90, 180, 90],
    "datetime": "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z",
    "filter": {
      "op": "and",
      "args": [
        {"op": "<", "args": [{"property": "eo:cloud_cover"}, 20]},
        {"op": "=", "args": [{"property": "platform"}, "sentinel-2"]}
      ]
    },
    "limit": 100
  }'

# Pagination example
curl "http://localhost:8081/catalogs/earth-observation/search?limit=10"
# Use the returned 'next' link token for the next page
curl "http://localhost:8081/catalogs/earth-observation/search?limit=10&token=<token-from-previous-response>"
```

### Scope Enforcement

The catalogs search extension enforces strict scope boundaries as required by the [STAC API - Multi-Tenant Catalogs specification](https://github.com/StacLabs/multi-tenant-catalogs#scoped-search-recursive-traversal):

- **Allowed**: Search any collections within the catalog's subtree
- **Allowed**: Specify a subset of descendant collections to search
- **Blocked**: Request collections outside the catalog's scope (returns 403 Forbidden)
- **Empty Catalog**: Returns empty results if the catalog has no descendant collections

**Scope Enforcement Rules:**
- The API computes the intersection of user-requested collections and the catalog's allowed descendant collections
- Any requested collections outside the descendant tree are rejected with a 403 Forbidden error
- Users cannot escape the catalog boundary through collection parameters
- All items returned are guaranteed to be within the catalog's hierarchy

Example of scope enforcement:

```bash
# This works - sentinel-2 is in earth-observation's subtree
curl "http://localhost:8081/catalogs/earth-observation/search?collections=sentinel-2"

# This fails - landsat-archive is not in earth-observation's subtree
curl "http://localhost:8081/catalogs/earth-observation/search?collections=landsat-archive"
# Response: 403 Forbidden - "Requested collections are outside the scope of this catalog."
```

### Performance Considerations

- **DAG Traversal**: The BFS traversal is optimized with a 10,000 result limit per query level. Large hierarchies with more than 10,000 direct children at any level will log a warning.
- **Caching**: For frequently accessed catalogs, consider caching the descendant collection list at the application level
- **Index Size**: Searching large catalogs with millions of items may require tuning Elasticsearch/OpenSearch settings
- **Pagination**: Use pagination tokens for efficient navigation through large result sets

### Conformance Classes

When the catalogs search extension is enabled, SFEOS advertises the following conformance class:

- **`https://api.stacspec.org/v1.0.0-rc.2/multi-tenant-catalogs/search`** - Indicates support for scoped search endpoints with recursive traversal through catalog hierarchies

This conformance class is automatically advertised in the API's `/conformance` endpoint when `ENABLE_CATALOGS_ROUTE=true`.

### Configuration

The catalogs search extension is automatically enabled when the catalogs route is enabled:

```bash
# Enable catalogs route (which includes search)
export ENABLE_CATALOGS_ROUTE=true
```

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

### CQL2 JSON Search with AST-based Parsing

SFEOS now uses an Abstract Syntax Tree (AST) in CQL2-JSON search queries for efficient query parsing and datetime extraction, enabling the selection and management of the appropriate searchable indexes.

#### AST-based Query Processing

The CQL2 implementation uses an Abstract Syntax Tree (AST) structure that replaces the previous dictionary-based processing. This enables:

1. **Structured Query Representation**: Queries are parsed into a tree structure with different node types
2. **Efficient Parameter Access**: Easy traversal and extraction of query parameters
3. **Optimized Index Selection**: Selection of appropriate fields for selection and management of indexes

#### AST Node Types

The AST supports various node types representing different query operations:

- **Logical Nodes**: `AND`, `OR`, `NOT` operators for combining conditions
- **Comparison Nodes**: `=`, `<>`, `<`, `<=`, `>`, `>=`, `isNull` operations
- **Advanced Comparison Nodes**: `LIKE`, `BETWEEN`, `IN` operations
- **Spatial Nodes**: `s_intersects`, `s_contains`, `s_within`, `s_disjoint` for geospatial queries
- **Datetime Nodes**: Special handling for datetime range and exact value queries

The AST-based approach enables efficient extraction of datetime parameters (`datetime`, `start_datetime`, `end_datetime`) from complex queries.

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

**1. Quick Deployment (Recommended)**

To quickly run the application using optimized, pre-built images from the GitHub Container Registry (GHCR), use the dedicated deployment compose files:

```shell
# For Elasticsearch backend
docker compose -f compose.es.deploy.yml up

# For OpenSearch backend
docker compose -f compose.os.deploy.yml up
```

**2. Local Development**

If you are contributing to the project and want to build the images from your local source code with live-reloading enabled, use the default `compose.yml` file:

```shell
# For Elasticsearch backend
docker compose up elasticsearch app-elasticsearch

# For OpenSearch backend
docker compose up opensearch app-opensearch
```
- **Configuration**: By default, Docker Compose uses Elasticsearch 9.x and OpenSearch 3.5.0. To use different versions, create a `.env` file:
  ```shell
  ELASTICSEARCH_VERSION=9.3.2
  OPENSEARCH_VERSION=3.5.0
  ENABLE_DIRECT_RESPONSE=false
  ```


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
| `ELASTICSEARCH_VERSION` | Version of Elasticsearch to use. | `9.3.2` | Optional |
| `OPENSEARCH_VERSION` | OpenSearch version | `3.5.0` | Optional |
| `RETRY_MAX_ATTEMPTS_CONNECTION_ERROR` | Specifies the maximum number of retry attempts for connection errors (ConnectionError, ConnectionTimeout) before giving up. | `5` | Optional |
| `RETRY_MAX_ATTEMPTS_NOT_FOUND_ERROR` | Specifies the maximum number of retry attempts for `IndexNotFoundException` error before giving up. This is particularly useful for datetime-based index searches where indices may need to be refreshed. | `3` | Optional |
| `RETRY_WAIT_SECONDS` | Specifies the number of seconds to wait between retry attempts. | `0.5` | Optional |
| `RETRY_RERAISE` | Specifies whether the original exception should be re-raised after all retry attempts are exhausted. | `true` | Optional |
| `ES_MAX_URL_LENGTH` | Maximum URL length for Elasticsearch/OpenSearch requests. When the combined length of index names in a query exceeds this limit (minus a 300-character buffer), the API falls back to querying all item indices with a collection filter in the request body. This value should match the `http.max_initial_line_length` setting in your Elasticsearch/OpenSearch server configuration. | `4096` | Optional |

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
| `ENABLE_CATALOGS_ROUTE` | Enable the **/catalogs** endpoint for hierarchical catalog browsing and navigation. **Note:** Requires the catalogs extension to be installed via `stac-fastapi-elasticsearch[catalogs]`, `stac-fastapi-opensearch[catalogs]`, or `stac-fastapi-core[catalogs]`. See [Catalogs Route](#catalogs-route) for installation instructions. | `false` | Optional |
| `HIDE_ALTERNATE_PARENTS` | When `true`, suppresses `rel="related"` and `rel="duplicate"` links for alternate parents in poly-hierarchy. Only the contextual `rel="parent"` link is advertised. Useful for multi-tenant deployments to prevent information leakage about other tenants. Requires `ENABLE_CATALOGS_ROUTE=true`. | `false` | Optional |
| `ENABLE_STAC_VALIDATOR` | Enable [stac-validator](https://github.com/stac-utils/stac-validator) to validate STAC items and collections on ingestion. This is especially useful for items or collections that use extensions. | `false` | Optional |
| `VALIDATE_BEFORE_QUEUE` | When using Redis queue (`ENABLE_REDIS_QUEUE=true`), controls whether validation happens on the API thread before queuing (true) or deferred to the background worker (false). When queue is disabled, validation always happens on the API thread. Set to `true` for strict data quality, `false` for maximum API throughput. See [Validation Timing with Redis Queue](#validation-timing-with-redis-queue) for details. | `true` | Optional |
| `ENABLE_TOPOLOGY_VALIDATION` | Enable lightweight pure-Python validation to enforce WGS84 coordinate bounds (±180° lon, ±90° lat) and detect improper antimeridian crossing in Polygon and MultiPolygon geometries. Provides CPU-efficient spatial validation without external dependencies. See [Topology Validation](#topology-validation) for details. | `false` | Optional |
| `MAX_TOPOLOGY_VERTICES` | Maximum number of vertices allowed in a single Polygon or MultiPolygon ring when topology validation is enabled. This prevents DoS attacks with pathologically complex geometries. Only applies when `ENABLE_TOPOLOGY_VALIDATION=true`. | `5000` | Optional |
| `STAC_INDEX_ASSETS` | Controls if Assets are indexed when added to Elasticsearch/Opensearch. This allows asset fields to be included in search queries. | `false` | Optional |

### 5. Limits & Performance

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `STAC_FASTAPI_RATE_LIMIT` | API rate limit per client. | `200/minute` | Optional |
| `STAC_GLOBAL_COLLECTION_MAX_LIMIT` | Configures the maximum number of STAC collections that can be returned in a single search request. | N/A | Optional |
| `STAC_DEFAULT_COLLECTION_LIMIT` | Configures the default number of STAC collections returned when no limit parameter is specified in the request. | `300` | Optional |
| `STAC_GLOBAL_ITEM_MAX_LIMIT` | Configures the maximum number of STAC items that can be returned in a single search request. | N/A | Optional |
| `STAC_DEFAULT_ITEM_LIMIT` | Configures the default number of STAC items returned when no limit parameter is specified in the request. | `10` | Optional |
| `COUNT_TIMEOUT` | Configures the timeout for the count task with search queries. If the count query takes longer than timeout, the search results are returned without the total count. Set to 0 to disable the timeout.. | `0.5` | Optional |
| `MAX_BATCH_SIZE` | When set to a value > 0, enables chunked validation with fail-fast thresholds. Items are validated in chunks of this size. Set to 0 to disable chunked validation (uses standard atomic validation). See [Chunked Validation with Fail-Fast](#chunked-validation-with-fail-fast) for details. | `0` | Optional |
| `MAX_BATCH_ERROR_SIZE` | Maximum number of validation errors allowed before halting the validation loop and rejecting the entire batch. Only applies when `MAX_BATCH_SIZE` > 0. This is a CPU optimization gate to prevent wasting resources validating hopelessly broken payloads. | `0` | Optional |


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
| `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` | JSON string of custom Elasticsearch/OpenSearch property mappings for items to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_MAPPINGS_FILE` | Path to a JSON file containing custom Elasticsearch/OpenSearch property mappings for items to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_COLLECTIONS_CUSTOM_MAPPINGS` | JSON string of custom Elasticsearch/OpenSearch property mappings for collections to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE` | Path to a JSON file containing custom Elasticsearch/OpenSearch property mappings for collections to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_CUSTOM_DYNAMIC_TEMPLATES` | JSON string of custom Elasticsearch/OpenSearch dynamic template to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE` | Path to a JSON file containing custom Elasticsearch/OpenSearch dynamic template to merge with defaults. See [Custom Index Mappings](#custom-index-mappings). | `None` | Optional |
| `STAC_FASTAPI_ES_DYNAMIC_MAPPING` | Controls dynamic mapping behavior for item indices. Values: `true` (default), `false`, or `strict`. See [Custom Index Mappings](#custom-index-mappings). | `true` | Optional |
| `STAC_FASTAPI_ES_COLLECTIONS_DYNAMIC_MAPPING` | Controls dynamic mapping behavior for collection indices. Values: `true` (default), `false`, or `strict`. See [Custom Index Mappings](#custom-index-mappings). | `true` | Optional |
| `STAC_FASTAPI_ES_COERCE_GLOBAL` | Sets the index-level coerce setting. When true (default), coercion is allowed (e.g., "10" → 10, 5.0 → 5). When false, coercion is disabled, documents with type mismatches are rejected unless overridden at the field level. | `true` | Optional |

### 7. Filtering, Exclusions & Queryables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `VALIDATE_QUERYABLES` | Enable validation of query parameters against the collection's queryables. If set to `true`, the API will reject queries containing fields that are not defined in the collection's queryables. | `false` | Optional |
| `QUERYABLES_CACHE_TTL` | Time-to-live (in seconds) for the queryables cache. Used when `VALIDATE_QUERYABLES` is enabled. | `1800` | Optional |
| `ROOT_QUERYABLES_UNION` | If set to `true`, the root `/queryables` endpoint dynamically unions queryables from all available collections. | `false` | Optional |
| `STAC_QUERYABLES_CONFIG` | Path to a static JSON file serving as an override for the root `/queryables` endpoint. Overrides `ROOT_QUERYABLES_UNION` if provided. | `None` | Optional |
| `HIDE_ITEM_PATH` | Path to boolean field that marks items as hidden (excluded from search) or not. If null, the item is returned. | `None` | Optional |
| `EXCLUDED_FROM_QUERYABLES` | Comma-separated list of fully qualified field names to exclude from the queryables endpoint and filtering. Use full paths like `properties.auth:schemes,properties.storage:schemes`. Excluded fields and their nested children will not be exposed in queryables. | None | Optional |
| `EXCLUDED_FROM_ITEMS` | Specifies fields to exclude from STAC item responses. Supports comma-separated field names and dot notation for nested fields (e.g., `private_data,properties.confidential,assets.internal`). | `None` | Optional |
| `FREE_TEXT_FIELDS` | Comma-separated list of fields to search in free-text queries. Supports field boosting syntax (e.g., `properties.title^3` gives title 3x weight). Example: `properties.title,properties.description,properties.example_name`. **Important**: Custom properties must be mapped as `text` type to support partial word matching. By default, unmapped properties are indexed as `keyword` type (exact match only). If not set, defaults to: `id,collection,properties.title^3,properties.description,properties.keywords`. | Default fields with title boosting | Optional |

> [!NOTE]
> The variables `ES_HOST`, `ES_PORT`, `ES_USE_SSL`, `ES_VERIFY_CERTS` and `ES_TIMEOUT` apply to both Elasticsearch and OpenSearch backends, so there is no need to rename the key names to `OS_` even if you're using OpenSearch.

## STAC Validation

STAC FastAPI provides a flexible, 2-tier validation architecture for STAC items and collections on ingestion. This ensures data quality and compliance with the STAC specification while allowing you to balance strict schema enforcement with high-throughput ingestion performance.

### 1. Native Pydantic Validation (Always Enabled)

By default, all STAC items and collections are validated using **Pydantic** (via `stac-pydantic`) at the API routing layer. This validation:

- Enforces required STAC fields and correct data types.
- Validates spatial and temporal properties.
- Provides extremely fast, built-in validation without external dependencies.

This validation is always enabled and happens automatically before data reaches the database or the Redis queue.

### 2. Python STAC Validator

If you require strict validation beyond Pydantic's type checking, you can enable the Python-based `stac-validator` package.

#### Enabling STAC Validator

1. **Install the validator**:
   ```bash
   pip install stac-fastapi-core[validator]
   # or
   pip install stac-fastapi-elasticsearch[validator]
   # or
   pip install stac-fastapi-opensearch[validator]
   ```

2. **Enable validation via environment variable**:
   ```bash
   export ENABLE_STAC_VALIDATOR=true
   ```

When enabled, the STAC validator will:
- Validate items and collections against the official STAC JSON schemas
- Check compliance with STAC extensions (e.g., EO, SAR, Projection)
- Catch schema violations that Pydantic doesn't enforce
- Provide detailed error messages with schema paths and validation details

#### Example: Validation in Action

```bash
# Enable STAC validator
export ENABLE_STAC_VALIDATOR=true

# Now POST/PUT requests will validate against STAC schemas
curl -X POST http://localhost:8000/collections \
  -H "Content-Type: application/json" \
  -d @collection.json
```

If validation fails, you'll receive a detailed error response:
```json
{
  "detail": "STAC validation failed: 'eo:bands' does not match any of the regexes: '^(?!eo:)'. Error is in assets -> SR_B2"
}
```

#### Performance Considerations

- **Pydantic validation** Very fast and always enabled
- **STAC validator (Python)** (ENABLE_STAC_VALIDATOR): Uses multi-processing for feature-collections

#### Validation Timing with Redis Queue

When using the Redis queue (`ENABLE_REDIS_QUEUE=true`), you can control when validation occurs:

- **`VALIDATE_BEFORE_QUEUE=true` (default)**: Validates items on the API thread before queuing. This ensures data quality upfront but may impact API response times for large batches.
  - Use this for strict data quality requirements
  - Recommended for most production deployments
  
- **`VALIDATE_BEFORE_QUEUE=false`**: Skips validation on the API thread and lets the background worker validate items. This maximizes API throughput but delays error detection.
  - Use this for high-throughput scenarios where you can tolerate delayed validation
  - The worker will still validate and move invalid items to the Dead Letter Queue (DLQ)

**Example: Enable high-throughput mode with deferred validation**
```bash
export ENABLE_REDIS_QUEUE=true
export ENABLE_STAC_VALIDATOR=true
export VALIDATE_BEFORE_QUEUE=false
```

> **Note**: When `ENABLE_REDIS_QUEUE=false` (direct database mode), validation always happens on the API thread regardless of the `VALIDATE_BEFORE_QUEUE` setting.

#### Chunked Validation with Fail-Fast

For high-volume ingestion scenarios, you can enable **chunked validation with fail-fast thresholds** to optimize CPU usage and prevent wasting resources on hopelessly broken payloads.

**How it works:**

1. **Chunking**: Items are validated in chunks of `MAX_BATCH_SIZE` items
2. **Error Tracking**: Validation errors are accumulated across chunks
3. **Fail-Fast**: If total errors exceed `MAX_BATCH_ERROR_SIZE`, validation stops immediately and the entire batch is rejected
4. **Atomic Rejection**: The entire batch is always rejected if any errors are found (no partial inserts)

**Example: Enable chunked validation with fail-fast**
```bash
export ENABLE_STAC_VALIDATOR=true
export MAX_BATCH_SIZE=100          # Validate in chunks of 100 items
export MAX_BATCH_ERROR_SIZE=5      # Stop after 5 errors found
```

**Behavior:**
- If a batch of 1000 items has 6 validation errors distributed across chunks, validation stops after finding the 6th error
- The API returns a 400 error with details about where validation stopped and how many items were checked
- This prevents the validator from wasting CPU cycles on the remaining 994 items

**When to use:**
- High-volume ingestion (thousands of items per request)
- Unreliable data sources where large batches may contain many errors
- When you want to fail fast rather than validate everything

**When NOT to use:**
- Small batches (< 100 items) - overhead of chunking not worth it
- When you need a complete error report for all items - fail-fast stops early
- Set `MAX_BATCH_SIZE=0` (default) to disable and use standard atomic validation

> **Note**: Chunked validation only applies when `VALIDATE_BEFORE_QUEUE=true` (API thread validation). When using deferred validation (`VALIDATE_BEFORE_QUEUE=false`), the worker will validate the entire batch.

### 3. Topology Validation (Antimeridian & WGS84 Bounds Protection)

For geospatial data ingestion, you can enable **lightweight topology validation** to enforce WGS84 coordinate bounds and detect improper antimeridian crossing without external dependencies.

**How it works:**

1. **WGS84 Bounds Enforcement**: Validates all coordinates fall within standard global bounds (±180° longitude, ±90° latitude)
2. **Antimeridian Detection**: Detects improper antimeridian crossing in Polygon and MultiPolygon geometries (longitude jumps > 180°)
3. **Vertex Limit Enforcement**: Prevents DoS attacks by rejecting geometries with excessive vertices (default 5000 per ring)
4. **Recursive Validation**: Checks every coordinate pair in the geometry, not just the first
5. **Zero Dependencies**: Pure Python implementation with no external service calls

**Example: Enable topology validation**
```bash
export ENABLE_TOPOLOGY_VALIDATION=true
```

**Configuring the vertex limit:**
```bash
export ENABLE_TOPOLOGY_VALIDATION=true
export MAX_TOPOLOGY_VERTICES=10000  # Allow up to 10,000 vertices per ring
```

**Behavior:**

- Items with coordinates outside WGS84 bounds are rejected with HTTP 400
- Items with geometries crossing the antimeridian without proper truncation are rejected
- Validation runs after STAC schema validation, so it catches spatial errors that Pydantic doesn't enforce
- Works with both single-item and bulk FeatureCollection ingestion

**Example error response:**
```json
{
  "detail": "Invalid item geometry: Coordinates out of global WGS84 bounds: [200.5, 45.0]"
}
```

**When to use:**

- Ingesting data from unreliable sources with potential coordinate errors
- Enforcing strict spatial data quality standards
- Detecting antimeridian-crossing geometries that should be split or wrapped
- When you want lightweight validation without external service dependencies

**Integration with Chunked Validation:**

Topology validation integrates seamlessly with chunked validation and fail-fast thresholds. If topology errors exceed `MAX_BATCH_ERROR_SIZE`, the circuit breaker will halt validation early:

```bash
export ENABLE_TOPOLOGY_VALIDATION=true
export MAX_BATCH_SIZE=100
export MAX_BATCH_ERROR_SIZE=5
```

In this configuration, if 6 items have topology errors across the batch, validation stops after the chunk that causes the cumulative error count to exceed `MAX_BATCH_ERROR_SIZE`, preventing additional chunks from being processed. With `MAX_BATCH_SIZE=100`, this does not necessarily stop exactly when the 6th error is encountered; use smaller chunk sizes if you need earlier cutoff.

## Free-Text Search (`q` parameter)

The free-text search feature allows users to discover items and collections using keywords or phrases. By default, the search targets core fields: `id`, `collection`, `properties.title`, `properties.description`, and `properties.keywords`.

### How to Use the API

Users can submit search terms via the `q` parameter on the following routes:

* `GET /search?q=keyword` 
* `POST /search` (with `{"q": ["keyword"]}` in the body)
* `GET /collections?q=keyword`
* `POST /collections` (with `{"q": ["keyword"]}` in the body)
* `GET /collections/{collection_id}/items?q=keyword` (search items within a specific collection)

**Examples:**

* **Single Term**: `/search?q=temperature` (Finds items with "temperature" in any core field).
* **Multiple Terms (OR logic)**: `/search?q=landsat&q=sentinel` (Finds items containing either "landsat" OR "sentinel").

---

### Setting Realistic Expectations: How Search Works

To get the most out of the search engine, it is important to understand the difference between **Typo Tolerance** and **Partial Word Matching**.

#### 1. Typo Tolerance (Fuzziness)

The API uses `fuzziness: "AUTO"`. This is a safety net for **accidental misspellings**, not a way to handle abbreviations or partial words.

* **The Logic**: It calculates the "Edit Distance" (how many letters must change to match).
* **Short words (0–2 chars)**: Must be an exact match.
* **Medium words (3–5 chars)**: 1 typo allowed (e.g., `sentnel` matches `sentinel`).
* **Long words (6+ chars)**: 2 typos allowed (e.g., `temparature` matches `temperature`).

* **The Limitation**: `q=temp` will **not** find `temperature`. Because "temp" is missing 7 characters, it is too "far" for the fuzzy engine to bridge.

#### 2. Partial Matching (Tokenization)

Discovering a word *inside* a phrase (e.g., finding "Surface" within "Near-Surface Air Temperature") depends entirely on **Field Mapping**.

* **`text` fields**: These are "tokenized" (broken into words). Searching for one word in the phrase works perfectly.
* **`keyword` fields**: These are stored as a single literal string. Searching for a single word will **fail**; you must search for the *exact full phrase*.

**Summary Table:**

| User Search | Target Metadata Value | Match? | Reason |
| --- | --- | --- | --- |
| `temparature` | `temperature` | ✅ **Yes** | 1 typo (Fuzzy match) |
| `temp` | `temperature` | ❌ **No** | Too many missing letters for Fuzzy |
| `Surface` | `Near-Surface Air Temp` | ✅ **Yes** | Word found in a `text` field |
| `Surface` | `Near-Surface Air Temp` | ❌ **No** | If field is a `keyword` (requires full string) |

---

### Administrator Configuration

#### Adding Custom Searchable Fields

If your metadata uses custom fields (e.g., `properties.example_name`), follow these steps to make them discoverable:

1. **Map the field as `text`**: By default, unmapped strings are `keyword` type (exact match only). Use `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` to map them as `text`.

   ```bash
   export STAC_FASTAPI_ES_CUSTOM_MAPPINGS='{"properties":{"properties":{"example_name":{"type":"text"}}}}'
   ```

   Or using a JSON file with `STAC_FASTAPI_ES_MAPPINGS_FILE`:

   ```json
   {
     "properties": {
       "properties": {
         "example_name": {"type": "text"},
         "custom_field": {"type": "text"}
       }
     }
   }
   ```

2. **Add to Search Scope**: Update the `FREE_TEXT_FIELDS` environment variable:

   ```bash
   export FREE_TEXT_FIELDS="properties.title,properties.description,properties.example_name"
   ```

   *Note: Use `^` to boost relevance, e.g., `properties.title^3` makes title matches 3x more important.*

#### Performance & Scalability

* **Be Selective**: Only add fields to `FREE_TEXT_FIELDS` that users genuinely need to search.
* **Avoid Wildcards**: Do not use `properties.*` in `FREE_TEXT_FIELDS` for catalogs with millions of items. Searching every property simultaneously significantly increases query latency and creates "noisy" results.

## Redis for Navigation Configuration

These Redis configuration variables enable proper navigation functionality in STAC FastAPI.

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

## Queryables Endpoint

The `/queryables` endpoint in STAC APIs provides a JSON Schema detailing which fields can be used in filter expressions. SFEOS provides extensive configuration options to manage how queryables are generated, exposed, and validated.

By default, the root `/queryables` endpoint returns a baseline schema of universal STAC properties (like `id`, `datetime`, and `geometry`). On individual collections (`/collections/{collection_id}/queryables`), the endpoint dynamically surveys the database mapping of the collection's items and accurately exposes its specific properties.

### Root Queryables Configuration

For the root `/queryables` endpoint (`GET /queryables`), you can enhance the baseline response using the following environment variables:

- **`ROOT_QUERYABLES_UNION` (boolean)**: Set to `true` to dynamically scan all available collections in your catalog and merge their queryables into a single, comprehensive schema. This is highly recommended when front-end clients (like STAC Browser) rely on the root endpoint to offer "global" search filters across all diverse collections.
  
- **`STAC_QUERYABLES_CONFIG` (string)**: Provide an absolute or relative path to a local JSON file to serve as a static override for the root queryables endpoint. This allows you complete control over the exposed schema without relying on dynamic database resolution. *Note: If provided, this overrides `ROOT_QUERYABLES_UNION`.*

  **Example `queryables_config.json`:**
  ```json
  {
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "$id": "https://example.com/queryables.json",
    "type": "object",
    "title": "Custom Root Queryables",
    "properties": {
      "id": {
        "description": "ID",
        "type": "string"
      },
      "collection": {
        "description": "Collection",
        "type": "string"
      },
      "eo:cloud_cover": {
        "description": "Cloud Cover",
        "type": "number",
        "minimum": 0,
        "maximum": 100
      }
    },
    "additionalProperties": false
  }
  ```

> **Performance Note**: Dynamic union queries are automatically cached for the duration specified by `QUERYABLES_CACHE_TTL` (default is 1800 seconds) to prevent database strain.

### Excluding Fields from Queryables

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

### Queryables Validation

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

> [!IMPORTANT]
> **Redis is required** when datetime-based indexing is enabled. The system uses Redis to cache index alias mappings from Elasticsearch/OpenSearch, which significantly speeds up search queries by avoiding repeated alias lookups. Insert operations always fetch fresh aliases directly from ES/OS and then refresh the Redis cache, ensuring that search queries always see up-to-date alias data. Configure Redis using the connection variables described in the [Redis for Navigation](#redis-for-navigation-environment-variables) section (`REDIS_HOST`/`REDIS_PORT` or `REDIS_SENTINEL_HOSTS`).

### Related Configuration Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `ENABLE_DATETIME_INDEX_FILTERING` | Enables time-based index partitioning | `false` | `true` |
| `DATETIME_INDEX_MAX_SIZE_GB` | Maximum size limit for datetime indexes (GB) - note: add +20% to target size due to ES/OS compression | `25` | `50` |
| `STAC_ITEMS_INDEX_PREFIX` | Prefix for item indexes | `items_` | `stac_items_` |
| `ENABLE_REDIS_QUEUE` | Enables Redis queue for async item processing | `false` | `true` |
| `QUEUE_BATCH_SIZE` | Number of items to process in a single batch | `50` | `100` |
| `QUEUE_FLUSH_INTERVAL` | Maximum seconds to wait before flushing queue (even if batch not full) | `30` | `60` |
| `QUEUE_KEY_PREFIX` | Redis key prefix for queue data | `item_queue` | `stac_queue` |
| `WORKER_POLL_INTERVAL` | Seconds between worker polls for new items | `1.0` | `0.5` |
| `WORKER_MAX_THREADS` | Maximum concurrent threads for processing collections | `4` | `8` |

### Redis Queue for Item Processing

When datetime-based indexing is enabled, you can use Redis-based queue processing to avoid race conditions. Without the queue, concurrent requests adding items to the same collection may cause conflicts when modifying index aliases. The queue serializes item processing per collection, ensuring safe alias management.

When `ENABLE_REDIS_QUEUE=true`, you **must** run the Item Queue Worker process to process queued items. The worker reads items from the Redis queue and inserts them into Elasticsearch/OpenSearch.

**Start the worker:**
```bash
python scripts/item_queue_worker.py
```

**Important:** Without the worker running, items will remain in the Redis queue and will not be indexed in Elasticsearch/OpenSearch.

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

## Sorting and Time-Range Items (`datetime: null`)

Due to a combination of the STAC specification's rules for time-range items and underlying OpenSearch/Elasticsearch pagination constraints, this API implements specific fallback behaviors for sorting.

In STAC, items representing a time range (e.g., a multi-day composite) set their `datetime` field to `null` and provide a `start_datetime` and `end_datetime`. To prevent `search_after` pagination from crashing on these null values, the API assigns missing dates to the extreme past or future depending on your sort direction.

**What this means for your search results:**

* **Default Sort (Newest First):** The API evaluates `datetime` first. All single-snapshot items (which have a `datetime`) will appear chronologically at the top of your search results. All time-range items (which have `datetime: null`) will be grouped together and appear chronologically at the absolute bottom of the search results.
* **Sorting by `start_datetime`:** If you want to prioritize time-range items, you can explicitly query `?sortby=-start_datetime`. This reverses the behavior: time-range items will sort chronologically at the top of your results, and single-snapshot items (which are missing a start date) will be pushed to the bottom.

**Best Practice:** If your workflow relies on interleaving both single-snapshot and time-range items perfectly by date, we recommend filtering by specific datetime intervals in your query rather than relying strictly on the global sort order.

## SFEOS Tools CLI

[SFEOS Tools](https://github.com/StacLabs/sfeos-tools) is a CLI package for managing SFEOS deployments. It provides utilities for database operations, data loading, and catalog ingestion.

### Installation

```bash
# For Elasticsearch
pip install sfeos-tools[elasticsearch]

# For OpenSearch
pip install sfeos-tools[opensearch]

# For viewer (Streamlit-based)
pip install sfeos-tools[viewer]

# For development
pip install sfeos-tools[dev]
```

### Basic Usage

```bash
sfeos-tools --help
sfeos-tools --version
```

### Common Commands

**Database Operations:**

- `add-bbox-shape`: Add spatial search support to existing collections
- `reindex`: Reindex all STAC indices with zero downtime

**Data Management:**

- `load-data`: Load STAC collections and items from local JSON files into the API
- `ingest-catalog`: Ingest SKOS/RDF-XML files to create STAC catalogs

**Viewer:**

- `viewer`: Launch interactive Streamlit-based web viewer for exploring STAC data

### Data Loading with `load-data`

The `load-data` command provides flexible options for populating your STAC API with collections and items:

**Basic Usage:**

```bash
# Load from default directory (sample_data/)
sfeos-tools load-data --stac-url http://localhost:8080

# Load with custom collection ID
sfeos-tools load-data --stac-url http://localhost:8080 --collection-id my-collection

# Load from custom directory
sfeos-tools load-data --stac-url http://localhost:8080 --data-dir /path/to/stac/data

# Use bulk insert for large datasets (faster performance)
sfeos-tools load-data --stac-url http://localhost:8080 --use-bulk
```

**Data Directory Structure:**

Your data directory should contain:

- `collection.json`: STAC collection definition
- One or more `.json` files: Feature collections with STAC items

**Common Workflows:**

- **Populating a new STAC API deployment** with test or production data
- **Migrating data** between STAC API instances
- **Bulk loading** large numbers of STAC items with optimized performance
- **Creating collections** programmatically from JSON definitions

### Standardized Options

**Database Commands** (`add-bbox-shape`, `reindex`):

- `--backend`: Database backend (elasticsearch or opensearch) - required
- `--host`: Database host (default: localhost or ES_HOST env var)
- `--port`: Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)
- `--use-ssl/--no-ssl`: SSL connection (default: true or ES_USE_SSL env var)
- `--user`: Database username (default: ES_USER env var)
- `--password`: Database password (default: ES_PASS env var)

**STAC API Commands** (`load-data`, `ingest-catalog`, `viewer`):

- `--stac-url`: STAC API base URL (default: http://localhost:8080)
- `--user`: Username for basic authentication (optional)
- `--password`: Password for basic authentication (optional)
- `--use-ssl/--no-ssl`: SSL verification (optional)

For complete documentation, examples, and advanced usage, visit the [SFEOS Tools GitHub repository](https://github.com/StacLabs/sfeos-tools).

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

| Variable | Index | Description | Default |
|----------|------|-------------|---------|
| `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` | items | JSON string of property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_MAPPINGS_FILE` | items | Path to a JSON file containing property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_COLLECTIONS_CUSTOM_MAPPINGS` | collections| JSON string of property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE` | collections| Path to a JSON file containing property mappings to merge with defaults | None |
| `STAC_FASTAPI_ES_CUSTOM_DYNAMIC_TEMPLATES` | dynamic template| JSON string of templates to merge with defaults | None |
| `STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE` | dynamic template| Path to a JSON file containing templates to merge with defaults | None |
| `STAC_FASTAPI_ES_DYNAMIC_MAPPING` | dynamic mapping | Controls dynamic mapping: `true`, `false`, or `strict` | `true` |
| `STAC_FASTAPI_ES_COLLECTIONS_DYNAMIC_MAPPING ` | dynamic mapping | Controls dynamic mapping: `true`, `false`, or `strict` | `true` |

### Custom Mappings

You can customize the Elasticsearch/OpenSearch mappings by providing a JSON configuration. This can be done via:

1. `STAC_FASTAPI_ES_CUSTOM_MAPPINGS` | `STAC_FASTAPI_ES_COLLECTIONS_CUSTOM_MAPPINGS`|`STAC_FASTAPI_ES_CUSTOM_DYNAMIC_TEMPLATES` environment variable (takes precedence)
2. `STAC_FASTAPI_ES_MAPPINGS_FILE`| `STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE`| `STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE` environment variable (file path)

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

**Example - Adding dynamic template:**

```bash
export STAC_FASTAPI_ES_CUSTOM_DYNAMIC_TEMPLATES='[{
	"titles": {
		"match_mapping_type": "string",
		"match": "title",
		"mapping": {"type": "text", "fields": {
				"keyword": {"type": "keyword"}}
		}
	}
}]'
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
A similar approach can be taken for `STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE` and  `STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE`.

In Docker Compose, you can mount the file:

```yaml
services:
  app-elasticsearch:
    volumes:
      - ./custom-mappings.json:/app/mappings.json:ro
      - ./custom-collections-mappings.json:/app/collections-mappings.json:ro
      - ./custom-dynamic-templates.json:/app/dynamic-templates.json:ro
    environment:
      - STAC_FASTAPI_ES_MAPPINGS_FILE=/app/mappings.json
      - STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE=/app/collections-mappings.json
      - STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE=/app/dynamic-templates.json

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


## Prometheus metrics

- **Installation**: Install the `metrics` extra alongside your backend:
  ```bash
  pip install stac-fastapi-elasticsearch[metrics]  # Elasticsearch backend
  pip install stac-fastapi-opensearch[metrics]     # OpenSearch backend
  ```

- **Usage**: Once installed, `/metrics` is live on startup. If the package is missing, the app starts normally and logs a warning.

- **Metrics exposed** (Prometheus text format):
    - `http_requests_total` — request count by method, path, and status code
    - `http_request_duration_seconds` — request latency histogram
    - `http_requests_inprogress` — in-flight request gauge


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

## Error Monitoring with Sentry

Optional integration with Sentry for error tracking, performance monitoring, and release tracking. When enabled, Sentry provides real-time insights into application errors, performance bottlenecks, and deployment health.

| Variable | Description | Default |
|----------|-------------|---------|
| `SENTRY_ENABLE` | Enable Sentry integration for error tracking and performance monitoring | `false` |
| `SENTRY_DSN` | Sentry Data Source Name (DSN) for your project | `None` |
| `SENTRY_ENVIRONMENT` | Deployment environment (production, staging, development) | `staging` |
| `SENTRY_TRACES_SAMPLE_RATE` | Percentage of transactions to sample for performance monitoring (0.0 to 1.0) | `0.1` |
| `SENTRY_CA_CERTS` | Path to a certificate used to validate SSL/TLS connections to Sentry (useful in private networks or when using internal certificate authorities) | `None` |
