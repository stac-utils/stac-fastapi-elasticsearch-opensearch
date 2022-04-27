# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Deprecated

### Added

### Fixed

- Fixed search intersects query

### Changed

- Default to Python 3.10
- Default to Elasticsearch 8.x
- Collection objects are now stored in `collections` index rather than `stac_collections` index
- Item objects are no longer stored in `stac_items`, but in indices per collection named `items_{collection_id}`

### Removed

## [0.1.0]

### Deprecated

### Added

### Fixed

### Changed

- Elasticsearch index mappings updated to be more thorough.
- Endpoints that return items (e.g., /search) now sort the results by 'properties.datetime,id,collection'.
  Previously, there was no sort order defined.
- Db_to_stac serializer moved to core.py for consistency as it existed in both core and database_logic previously. 
- Use genexp in execute_search and get_all_collections to return results.
- Added db_to_stac serializer to item_collection method in core.py.

### Removed

## Versions

- [Unreleased]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0...main>
- [v0.1.0]: <https://github.com/stac-utils/stac-fastapi-elasticsearch/tree/v0.1.0>
