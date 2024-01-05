# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Advanced comparison (LIKE, IN, BETWEEN) operators to the Filter extension [#178](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/178)

### Changed

- Elasticsearch drivers from 7.17.9 to 8.11.0 [#169](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/169)

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


[Unreleased]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v1.0.0...main>
[v1.0.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.3.0...v1.0.0>
[v0.3.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.2.0...v0.3.0>
[v0.2.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0...v0.2.0>
[v0.1.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0>
