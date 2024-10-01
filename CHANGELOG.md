# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## EODHP
The following changes have been made for the EODHP project

### v0.3.10 - 2024-09-30

Updated Error Codes for Collection, Catalog, and Items Access:
- The applied changes ensure the correct error codes (401 and 403) are returned in the following scenarios:
- Unauthenticated Access: Users who are not logged in cannot access any private workspaces, 
  whether their own or others’.
  Authenticated Access: Logged-in users can only access their own private workspaces.
  Public Access: Access to public workspaces remains unaffected, regardless of the user’s login status.
- Updated all_catalogs and post_search method to return the right error codes(401,403)
Updates to better support STAC Browser:
- Updated collections search to offer catalog specific endpoints for "/collections"
- Include catalog specific endpoint for landing page
- Clean up error outputs when items, collections or catalogs are not found
- General code clean up, including comments and logging

Bugfix:
- Add missing parameter to `create_item` and `delete_item` calls - `workspace`

### v0.3.9 - 2024-07-30
Bugfixes:
- Next Links in POST Requests
- Handle case in global search where user has no access to specified catalog or collections

### V0.3.8 - 2024-07-30
Access-Control Logic in Catalog
- Adding access control to catalogs and collections added to the catalogue
- Access limited based on incoming `authorization` header username
- Workspace parameter used when creating/editing entries to ensure workspace has required access to make the requested changes
- Access determined by hashing the provided username or workspace name, storing in elasticsearch and filtering results
### v0.3.7 - 2024-07-02
Catalog creation logic improvement
- Ensure parent catalog exists before creating child
- Two new functions implemented (async and sync) for catalog preparation

Code clean up
  - Comments for nested catalog logic
  - Correct response code for incorrect query parameters when searching
  - Split up collection-search extension from core `/collections` implementation
  - Defining abstract functions for collection and discovery search extensions  

Bugfixes
  - Remove unnecessary parameter from collection-search call
  - Improve discovery-search implementation to ensure all nested catalogs and collections are returned in catalog responses
  - Corrected links in response form `/catalogs` endpoints
### v0.3.6 - 2024-06-14
## Support nested catalogs
- Data within (arbitrarily) nested catalogs can be accessed as before with the provided catalog path parameter, e.g. `/supported-datasets/dataset-1`
- Requires `catalog_path` parameter to be provided to specify where in the catalog directories the data should be placed/recalled from
- Update endpoints to support nested catalog definition in URL path
- Better support for STAC-Browser and pystac python client:
  - Updating link generator logic
### v0.3.5 - 2024-05-31
Support configuration for read-only deployments:
- Allow transaction extensions (including Bulk Transactions) to be disabled by environment variable (STAC_FASTAPI_ENABLE_TRANSACTIONS) at deployment time
- STAC-fastapi instances can be defined as either read-only (transaction endpoints disabled) or read-write (with all transactions endpoints enabled)

The following bugs were also addressed:
- Corrected update catalog functionality to reindex all items sitting within the updated catalog when the catalog id is changed
- Corrected issue with pagination links being broken and added support for discovery and collections search
### v0.3.4 - 2024-05-14
#### Added new endpoint to allow free-text searching of collections and catalogues:
- New endpoints:
  - Discovery Search (GET/POST)
  - This queries title, description and keyword (collections only) fields when available
#### Added support for catalogues to split up collections and items into catalogues of data
  - This leads to updates for the following endpoints to filter by catalogues:
    - Get Collection
    - Get ItemCollection
    - Get CatalogCollections
    - Create Item
    - Update Item
    - Delete Item
    - Create Collection
    - Update Collection
    - Delete Collection
  - New endpoints added for catalogues:
    - Update Catalog
    - Create Catalog
    - Delete Catalog
    - Get Catalogs
##### Minor changes
- Update to the datetime handling for Get ItemCollection to include string converter.
### v0.3.3 - 2024-05-08
Including fix to address incorrect links returned from `/search` and `/collection-search` endpoints defining the `self` and `next` item locations.
### v0.3.2 - 2024-05-01
Bugfixes:
- Bugfix for missing collection search links
- Bugfix to address JSON validation error in search results
### V0.3.1 - 2024-04-26
General code clean up to improve initialisation commands
### V3.0.0 - 2024-04-23
Added [collection search](https://github.com/stac-api-extensions/collection-search) to STAC Fastapi  allowing spatial and temporal searching of collections by BBOX and datetime.

## [Unreleased]

## [v2.2.0]

### Added

- use index templates for Collection and Item indices [#208](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/discussions/208)
- Added API `title`, `version`, and `description` parameters from environment variables `STAC_FASTAPI_TITLE`, `STAC_FASTAPI_VERSION` and `STAC_FASTAPI_DESCRIPTION`, respectively. [#207](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/207)
- Added a `STAC_FASTAPI_ROOT_PATH` environment variable to define the root path. Useful when working with an API gateway or load balancer. [#221](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/221)
- Added mkdocs, pdocs, to generate docs and push to gh pages via workflow. Updated documentation. [#223](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/223)


### Changed

- Updated the pip_docker example to use stac-fastapi.elasticsearch 2.1.0 and the elasticsearch 8.11.0 docker image. [#216](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/216)
- Updated the Data Loader CLI tool to accept a base_url, a data directory, a custom collection id, and an option to use bulk insert. [#218](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/218)
- Changed the default `ca_certs` value to use `certifi.where()` to find the installed certificate authority. [#222](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/222)

### Fixed

- URL encode next href: [#215](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/215)
- Do not overwrite links in Item and Collection objects before persisting in database [#210](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/210)

## [v2.1.0]

### Added

- Added explicit mapping for ID in `ES_COLLECTIONS_MAPPINGS` [#198](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/198)

### Changed

- Removed database logic from core.py all_collections [#196](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/196)
- Changed OpenSearch config ssl_version to SSLv23 [#200](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/200)

### Fixed

## [v2.0.0]

### Added

- Added core library package for common logic [#186](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/186)

### Changed

- Moved Elasticsearch and Opensearch backends into separate packages [#186](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/186)

### Fixed

- Allow additional top-level properties on collections [#191](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/191)

## [v1.1.0]

### Added

- Advanced comparison (LIKE, IN, BETWEEN) operators to the Filter extension [#178](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/178)
- Collection update endpoint no longer delete all sub items [#177](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/177)
- OpenSearch 2.11.1 support [#188](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/188)

### Changed

- Elasticsearch drivers from 7.17.9 to 8.11.0 [#169](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/169)
- Collection update endpoint no longer delete all sub items [#177](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/177)

### Fixed

- Exclude unset fields in search response [#166](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/166)
- Upgrade stac-fastapi to v2.4.9 [#172](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/172)
- Set correct default filter-lang for GET /search requests [#179](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/179)

## [v1.0.0]

### Added

- Collection-level Assets to the CollectionSerializer [#148](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/148)
- Pagination for /collections - GET all collections - route [#164](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/164)
- Examples folder with example docker setup for running sfes from pip [#147](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/147)
- GET /search filter extension queries [#163](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/163)
- Added support for GET /search intersection queries [#158](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/158)

### Changed

- Update elasticsearch version from 8.1.3 to 8.10.4 in cicd, gh actions [#164](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/164)
- Updated core stac-fastapi libraries to 2.4.8 from 2.4.3 [#151](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/151)
- Use aliases on Elasticsearch indices, add number suffix in index name. [#152](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/152)

### Fixed

- Corrected the closing of client connections in ES index management functions [#132](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/132)
- Corrected the automatic converstion of float values to int when building Filter Clauses [#135](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/135)
- Do not index `proj:geometry` field as geo_shape [#154](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/154)
- Remove unsupported characters from Elasticsearch index names [#153](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/153)
- Fixed GET /search sortby requests [#25](https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/25)


## [v0.3.0]

### Added

- Added bbox and datetime parameters and functionality to item_collection [#127](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/127)
- Added collection_id parameter to create_item function [#127](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/127)
- Added item_id and collection_id to update_item [#127](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/127)
- The default Collection objects index can be overridden by the `STAC_COLLECTIONS_INDEX` environment variable [#128](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/128)
- The default Item objects index prefix can be overridden by the `STAC_ITEMS_INDEX_PREFIX` environment variable [#128](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/128)
- Fields Extension [#129](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/129)
- Support for Python 3.11 [#131](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/131)

### Changed

- Updated core stac-fastapi libraries to 2.4.3 from 2.3.0 [#127](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/127)


## [v0.2.0]

### Added

- Filter Extension as GET with CQL2-Text and POST with CQL2-JSON,
  supporting the Basic CQL2 and Basic Spatial Operators conformance classes.
- Added Elasticsearch local config to support snapshot/restore to local filesystem

### Fixed

- Fixed search intersects query.
- Corrected the Sort and Query conformance class URIs.

### Changed

- Default to Python 3.10
- Default to Elasticsearch 8.x
- Collection objects are now stored in `collections` index rather than `stac_collections` index
- Item objects are no longer stored in `stac_items`, but in indices per collection named `items_{collection_id}`
- When using bulk ingest, items will continue to be ingested if any of them fail. Previously, the call would fail
  immediately if any items failed.


## [v0.1.0]

### Changed

- Elasticsearch index mappings updated to be more thorough.
- Endpoints that return items (e.g., /search) now sort the results by 'properties.datetime,id,collection'.
  Previously, there was no sort order defined.
- Db_to_stac serializer moved to core.py for consistency as it existed in both core and database_logic previously.
- Use genexp in execute_search and get_all_collections to return results.
- Added db_to_stac serializer to item_collection method in core.py.


[Unreleased]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v2.2.0...main>
[v2.2.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v2.1.0...v2.2.0>
[v2.1.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v2.0.0...v2.1.0>
[v2.0.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v1.1.0...v2.0.0>
[v1.1.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v1.0.0...v1.1.0>
[v1.0.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.3.0...v1.0.0>
[v0.3.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.2.0...v0.3.0>
[v0.2.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0...v0.2.0>
[v0.1.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0>