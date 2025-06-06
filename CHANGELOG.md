# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

- Added support for enum queryables [#390](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/390)

## [v5.0.0a1] - 2025-05-30

### Changed

- Updated mkdocs/ sfeos doucmentation page [#386](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/386)

### Fixed

- Added the ability to authenticate with OpenSearch/ElasticSearch with SSL disabled [#388](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/388)

## [v5.0.0a0] - 2025-05-29

### Added

- Created new `sfeos_helpers` package to improve code organization and maintainability [#376](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/376)
- Added introduction section - What is stac-fastapi-elasticsearch-opensearch? - to README [#384](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/384)

### Changed

- Refactored utility functions into dedicated modules within `sfeos_helpers` [#376](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/376):
  - Created `database` package with specialized modules for index, document, and utility operations
  - Created `aggregation` package for Elasticsearch/OpenSearch-specific aggregation functionality
  - Moved shared logic from core module to helper functions for better code reuse
  - Separated utility functions from constant mappings for clearer code organization
- Updated documentation to reflect recent code refactoring  [#376](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/376)
- Improved README documentation with consistent formatting and enhanced sections [#381](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/381):
  - Added sfeos logo and banner
  - Added a comprehensive Quick Start guide
  - Reorganized sections for better navigation
  - Reformatted content with bullet points for improved readability
  - Added more detailed examples for API interaction
  
### Fixed

## [v4.2.0] - 2025-05-15

### Added

- Added dynamic queryables mapping for search and aggregations [#375](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/375)
- Added configurable landing page ID `STAC_FASTAPI_LANDING_PAGE_ID` [#352](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/352)
- Added support for `S_CONTAINS`, `S_WITHIN`, `S_DISJOINT` spatial filter operations [#371](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/371)
- Introduced the `DATABASE_REFRESH` environment variable to control whether database operations refresh the index immediately after changes. If set to `true`, changes will be immediately searchable. If set to `false`, changes may not be immediately visible but can improve performance for bulk operations. If set to `wait_for`, changes will wait for the next refresh cycle to become visible. [#370](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/370)
- Added the `ENABLE_TRANSACTIONS_EXTENSIONS` environment variable to enable or disable the Transactions and Bulk Transactions API extensions. When set to `false`, endpoints provided by `TransactionsClient` and `BulkTransactionsClient` are not available. This allows for flexible deployment scenarios and improved API control. [#374](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/374)

### Changed

- Refactored CRUD methods in `TransactionsClient` to use the `validate_refresh` helper method for consistent and reusable handling of the `refresh` parameter. [#370](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/370)

### Fixed

- Fixed an issue where some routes were not passing the `refresh` parameter from `kwargs` to the database logic, ensuring consistent behavior across all CRUD operations. [#370](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/370)

## [v4.1.0] - 2025-05-09

### Added

- Added logging to bulk insertion methods to provide detailed feedback on errors encountered during operations. [#364](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/364)
- Introduced the `RAISE_ON_BULK_ERROR` environment variable to control whether bulk insertion methods raise exceptions on errors (`true`) or log warnings and continue processing (`false`). [#364](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/364)
- Added code coverage reporting to the test suite using pytest-cov. [#87](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/87)

### Changed

- Updated dynamic mapping for items to map long values to double versus float. [#326](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/326)
- Extended Datetime Search to search on start_datetime and end_datetime as well as datetime fields. [#182](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/182)
- Changed item update operation to use Elasticsearch index API instead of delete and create for better efficiency and atomicity. [#75](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/75)
- Bulk insertion via `BulkTransactionsClient` now strictly validates all STAC Items using the Pydantic model before insertion. Any invalid item will immediately raise a `ValidationError`, ensuring consistent validation with single-item inserts and preventing invalid STAC Items from being stored. This validation is enforced regardless of the `RAISE_ON_BULK_ERROR` setting. [#368](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/368)

### Changed

### Fixed

- Refactored `create_item` and `update_item` methods to share unified logic, ensuring consistent conflict detection, validation, and database operations. [#368](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/368)

## [v4.0.0] - 2025-04-23

### Added
- Added support for dynamically-generated queryables based on Elasticsearch/OpenSearch mappings, with extensible metadata augmentation [#351](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/351)
- Included default queryables configuration for seamless integration. [#351](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/351)
- Added support for high-performance direct response mode for both Elasticsearch and Opensearch backends, controlled by the `ENABLE_DIRECT_RESPONSE` environment variable. When enabled (`ENABLE_DIRECT_RESPONSE=true`), endpoints return Starlette Response objects directly, bypassing FastAPI's jsonable_encoder and Pydantic serialization for significantly improved performance on large search responses. **Note:** In this mode, all FastAPI dependencies (including authentication, custom status codes, and validation) are disabled for all routes. Default is `false` for safety. A warning is logged at startup if enabled. See [issue #347](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/347) and [PR #359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359).
- Added robust tests for the `ENABLE_DIRECT_RESPONSE` environment variable, covering both Elasticsearch and OpenSearch backends. Tests gracefully handle missing backends by attempting to import both configs and skipping if neither is available. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)

### Changed
- Refactored database logic to reduce duplication [#351](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/351)
- Replaced `fastapi-slim` with `fastapi` dependency [#351](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/351)
- Changed minimum Python version to 3.9 [#354](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/354)
- Updated stac-fastapi api, types, and extensions libraries to 5.1.1 from 3.0.0 and made various associated changes [#354](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/354)
- Changed makefile commands from 'docker-compose' to 'docker compose' [#354](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/354)
- Updated package names in setup.py files to use underscores instead of periods for PEP 625 compliance [#358](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/358)
  - Changed `stac_fastapi.opensearch` to `stac_fastapi_opensearch`
  - Changed `stac_fastapi.elasticsearch` to `stac_fastapi_elasticsearch`
  - Changed `stac_fastapi.core` to `stac_fastapi_core`
  - Updated all related dependencies to use the new naming convention
- Renamed `docker-compose.yml` to `compose.yml` to align with Docker Compose V2 conventions [#358](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/358)
- Removed deprecated `version` field from all compose files [#358](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/358)
- Updated `STAC_FASTAPI_VERSION` environment variables to 4.0.0 in all compose files [#362](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/362)
- Bumped version from 4.0.0a2 to 4.0.0 for the PEP 625 compliant release [#362](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/362)
- Updated dependency requirements to use compatible release specifiers (~=) for more controlled updates while allowing for bug fixes and security patches [#358](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/358)
- Removed elasticsearch-dsl dependency as it's now part of the elasticsearch package since version 8.18.0 [#358](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/issues/358)
- Updated test suite to use `httpx.ASGITransport(app=...)` for FastAPI app testing (removes deprecation warning). [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)
- Updated stac-fastapi parent libraries to 5.2.0. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)
- Migrated Elasticsearch index template creation from legacy `put_template` to composable `put_index_template` API in `database_logic.py`. This resolves deprecation warnings and ensures compatibility with Elasticsearch 7.x and 8.x. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)
- Updated all Pydantic models to use `ConfigDict` instead of class-based `Config` for Pydantic v2 compatibility. This resolves deprecation warnings and prepares for Pydantic v3. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)
- Migrated all Pydantic `@root_validator` validators to `@model_validator` for Pydantic v2 compatibility. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)
- Migrated startup event handling from deprecated `@app.on_event("startup")` to FastAPI's recommended lifespan context manager. This removes deprecation warnings and ensures compatibility with future FastAPI versions. [#361](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/361)
- Refactored all boolean environment variable parsing in both Elasticsearch and OpenSearch backends to use the shared `get_bool_env` utility. This ensures robust and consistent handling of environment variables such as `ES_USE_SSL`, `ES_HTTP_COMPRESS`, and `ES_VERIFY_CERTS` across both backends. [#359](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/359)

### Fixed
- Improved performance of `mk_actions` and `filter-links` methods [#351](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/351)
- Fixed inheritance relating to BaseDatabaseSettings and ApiBaseSettings [#355](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/355)
- Fixed delete_item and delete_collection methods return types [#355](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/355)
- Fixed inheritance relating to DatabaseLogic and BaseDatabaseLogic, and ApiBaseSettings [#355](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/355)

## [v3.2.5] - 2025-04-07

### Added

- Option to configure multiple Elasticsearch/OpenSearch hosts and enable `http_compress`. [#349](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/349)

## [v3.2.4] - 2025-03-14

### Added

- Support python 3.13 in SFEOS Core, Opensearch and ElasticSearch. [#330](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/330)

## [v3.2.3] - 2025-02-11

### Changed

- Added note on the use of the default `*` use in route authentication dependecies. [#325](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/325)
- Update item index naming and aliasing to allow capitalisation of collection ids [#329](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/329)
- Bugfixes for the `IsNull` operator and datetime filtering [#330](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/330)

## [v3.2.2] - 2024-12-15

### Changed

- Use base64 encoded JSON string of sort keys as pagination token instead of comma-separated string [#323](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/323)

## [v3.2.1] - 2024-11-14

### Added

- Added `dockerfiles/Dockerfile.ci.os` and `dockerfiles/Dockerfile.ci.es`, along with their respective entrypoints [#311](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/311)

### Changed

- Updated the `publish.yml` workflow to include Docker image publishing to GitHub Container Registry [#311](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/311)
- Improved the README with detailed descriptions of the new Docker images, providing guidance for images. [#311](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/311)
- Aggregation ElasticSearch `total_count` bugfix, moved aggregation text to docs. [#314](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/314)

## [v3.2.0] - 2024-10-09

### Added

- Added `datetime_frequency_interval` parameter for `datetime_frequency` aggregation. [#294](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/294)
- Added rate limiting functionality with configurable limits using environment variable `STAC_FASTAPI_RATE_LIMIT`, example: `500/minute`. [#303](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/303)
- Added publish.yml to automatically publish new releases to PyPI [#305](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/305)

### Changed

- Updated CollectionLinks to generate correct `self` link for collections endpoint. [#297](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/297)
- Refactored aggregation in database logic. [#294](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/294)
- Fixed the `self` link for the `/collections/{collection_id}/aggregations` endpoint. [#295](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/295)

## [v3.1.0] - 2024-09-02

### Added

- Added support for FreeTextExtension. [#227](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/227)

### Changed

- Support escaped backslashes in CQL2 `LIKE` queries, and reject invalid (or incomplete) escape sequences. [#286](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/286)

## [v3.0.0] - 2024-08-14

### Changed

- Aggregation bug fixes [#281](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/281)
- Updated stac-fastapi libraries to v3.0.0 [#282](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/282)

## [v3.0.0a3] - 2024-07-17

### Added

- Added an implementation of the Aggregation Extension. Enables spatial, frequency distribution, and datetime distribution aggregations. [#276](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/276)
- Added support for route depndencies configuration through the STAC_FASTAPI_ROUTE_DEPENDENCIES environment variable, directly or via json file. Allows for fastapi's inbuilt OAuth2 flows to be used as dependencies. Custom dependencies can also be written, see Basic Auth for an example. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)
- Added docker-compose.route_dependencies_file.yml that gives an example of OAuth2 workflow using keycloak as the identity provider. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)
- Added docker-compose.route_dependencies_env.yml that gives an example using the STAC_FASTAPI_ROUTE_DEPENDENCIES environment variable. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)

### Changed

- Updated to stac-fastapi 3.0.0a4. [#275](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/275)
- Converted Basic auth to a route dependency and merged with new route depndencies method. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)
- Updated docker-compose.basic_auth_protected.yml to use STAC_FASTAPI_ROUTE_DEPENDENCIES environment variable. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)

## [v3.0.0a2]

### Added

- Queryables landing page and collection links when the Filter Extension is enabled [#267](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/267)

### Changed

- Updated stac-fastapi libraries to v3.0.0a1 [#265](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/265)
- Updated stac-fastapi libraries to v3.0.0a3 [#269](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/269)
- Converted Basic auth to a route dependency and merged with new route depndencies method. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)
- Updated docker-compose.basic_auth_protected.yml to use STAC_FASTAPI_ROUTE_DEPENDENCIES environment variable. [#251](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/251)

### Fixed

- API sort extension tests [#264](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/264)
- Basic auth permission fix for checking route path instead of absolute path [#266](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/266)
- Remove deprecated filter_fields property, return all properties as default [#269](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/269)

## [v3.0.0a1]

### Changed

- Unskip temporal open window test [#254](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/254)
- Removed deprecated context extension [#255](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/255)
- Remove duplicated code from stac_fastapi.types [#257](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/257)

## [v3.0.0a0]

### Added

- Symlinks from project-specific readme files to main readme [#250](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/250)
- Support for Python 3.12 [#234](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/234)

### Changed

- Updated stac-fastapi parent libraries to v3.0.0a0 [#234](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/234)
- Removed pystac dependency [#234](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/234)

### Fixed

- Fixed issue where paginated search queries would return a `next_token` on the last page [#243](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/243)

## [v2.4.1]

### Added

- A test to ensure that pagination correctly returns expected links, particularly verifying the absence of a 'next' link on the last page of results [#244](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/244)

### Fixed

- Fixed issue where searches return an empty `links` array [#241](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/241)

## [v2.4.0]

### Added

- Added option to include Basic Auth [#232](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/232)

### Changed

- Upgrade stac-fastapi libaries to v2.5.5 [#237](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/237)

### Fixed

- Fixed `POST /collections/test-collection/items` returning an item with an empty links array [#236](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/pull/236)

## [v2.3.0]

### Changed

- Upgraded stac-fastapi libraries to v2.5.3 from v2.4.9 [#172](https://github.com/stac-utils/stac-fastapi-elasticsearch/pull/172)

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

[Unreleased]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v5.0.0a1...main
[v5.0.0a1]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v5.0.0a0...v5.0.0a1
[v5.0.0a0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v4.2.0...v5.0.0a0
[v4.2.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v4.1.0...v4.2.0
[v4.1.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v4.0.0...v4.1.0
[v4.0.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.5...v4.0.0
[v3.2.5]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.4...v3.2.5
[v3.2.4]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.3...v3.2.4
[v3.2.3]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.2...v3.2.3
[v3.2.2]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.1...v3.2.2
[v3.2.1]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.2.0...v3.2.1
[v3.2.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.1.0...v3.2.0
[v3.1.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v3.0.0...v3.1.0
[v3.0.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.4.1...v3.0.0
[v2.4.1]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.4.0...v2.4.1
[v2.4.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.3.0...v2.4.0
[v2.3.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.2.0...v2.3.0
[v2.2.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.1.0...v2.2.0
[v2.1.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v2.0.0...v2.1.0
[v2.0.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v1.1.0...v2.0.0
[v1.1.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v0.3.0...v1.0.0
[v0.3.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch/compare/v0.1.0
