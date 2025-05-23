# STAC FastAPI Database Package

This package contains shared database operations used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI. It helps reduce code duplication and ensures consistent behavior
between the two implementations.

## Package Structure

The database package is organized into five main modules:

- **index.py**: Contains functions for managing indices
  - [create_index_templates_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:15:0-48:33): Creates index templates for Collections and Items
  - [delete_item_index_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:128:0-153:30): Deletes an item index for a collection
  - [index_by_collection_id](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:86:0-100:5): Translates a collection ID into an index name
  - [index_alias_by_collection_id](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:103:0-115:5): Translates a collection ID into an index alias
  - [indices](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:118:0-132:5): Gets a comma-separated string of index names

- **query.py**: Contains functions for building and manipulating queries
  - [apply_free_text_filter_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:51:0-74:16): Applies a free text filter to a search
  - [apply_intersects_filter_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:77:0-104:5): Creates a geo_shape filter for intersecting geometry
  - [populate_sort_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:107:0-125:16): Creates a sort configuration for queries

- **mapping.py**: Contains functions for working with mappings
  - [get_queryables_mapping_shared](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/database_logic_helpers.py:156:0-185:27): Retrieves mapping of Queryables for search

- **document.py**: Contains functions for working with documents
  - [mk_item_id](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:140:0-150:5): Creates a document ID for an Item
  - [mk_actions](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:153:0-175:5): Creates bulk actions for indexing items

- **utils.py**: Contains utility functions for database operations
  - [validate_refresh](cci:1://file:///home/computer/Code/stac-fastapi-elasticsearch-opensearch/stac_fastapi/sfeos_helpers/stac_fastapi/sfeos_helpers/utilities.py:41:0-78:5): Validates the refresh parameter value

## Usage

Import the necessary components from the database package:

```python
from stac_fastapi.sfeos_helpers.database import (
    # Index operations
    create_index_templates_shared,
    delete_item_index_shared,
    index_alias_by_collection_id,
    index_by_collection_id,
    indices,
    
    # Query operations
    apply_free_text_filter_shared,
    apply_intersects_filter_shared,
    populate_sort_shared,
    
    # Mapping operations
    get_queryables_mapping_shared,
    
    # Document operations
    mk_item_id,
    mk_actions,
    
    # Utility functions
    validate_refresh,
)
```
