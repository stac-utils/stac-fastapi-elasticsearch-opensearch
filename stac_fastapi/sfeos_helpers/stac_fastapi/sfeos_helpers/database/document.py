"""Document operations for Elasticsearch/OpenSearch.

This module provides functions for working with documents in Elasticsearch/OpenSearch,
including document ID generation and bulk action creation.
"""

from typing import Any, Dict, List

from stac_fastapi.sfeos_helpers.database.index import index_alias_by_collection_id
from stac_fastapi.types.stac import Item


def mk_item_id(item_id: str, collection_id: str) -> str:
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.

    Returns:
        str: The document id for the Item, combining the Item id and the Collection id, separated by a `|` character.
    """
    return f"{item_id}|{collection_id}"


def mk_actions(collection_id: str, processed_items: List[Item]) -> List[Dict[str, Any]]:
    """Create Elasticsearch bulk actions for a list of processed items.

    Args:
        collection_id (str): The identifier for the collection the items belong to.
        processed_items (List[Item]): The list of processed items to be bulk indexed.

    Returns:
        List[Dict[str, Union[str, Dict]]]: The list of bulk actions to be executed,
        each action being a dictionary with the following keys:
        - `_index`: the index to store the document in.
        - `_id`: the document's identifier.
        - `_source`: the source of the document.
    """
    index_alias = index_alias_by_collection_id(collection_id)
    return [
        {
            "_index": index_alias,
            "_id": mk_item_id(item["id"], item["collection"]),
            "_source": item,
        }
        for item in processed_items
    ]
