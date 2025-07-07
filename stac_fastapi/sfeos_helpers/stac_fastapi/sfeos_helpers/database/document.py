"""Document operations for Elasticsearch/OpenSearch.

This module provides functions for working with documents in Elasticsearch/OpenSearch,
including document ID generation and bulk action creation.
"""


def mk_item_id(item_id: str, collection_id: str) -> str:
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.

    Returns:
        str: The document id for the Item, combining the Item id and the Collection id, separated by a `|` character.
    """
    return f"{item_id}|{collection_id}"
