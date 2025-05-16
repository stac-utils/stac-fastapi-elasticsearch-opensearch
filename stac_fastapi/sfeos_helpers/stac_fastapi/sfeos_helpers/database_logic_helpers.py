"""Shared code for elasticsearch/ opensearch database logic."""

from typing import Any

from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    ITEMS_INDEX_PREFIX,
)


async def create_index_templates_shared(settings: Any) -> None:
    """
    Create index templates for the Collection and Item indices.

    Returns:
        None

    """
    client = settings.create_client
    await client.indices.put_index_template(
        name=f"template_{COLLECTIONS_INDEX}",
        body={
            "index_patterns": [f"{COLLECTIONS_INDEX}*"],
            "template": {"mappings": ES_COLLECTIONS_MAPPINGS},
        },
    )
    await client.indices.put_index_template(
        name=f"template_{ITEMS_INDEX_PREFIX}",
        body={
            "index_patterns": [f"{ITEMS_INDEX_PREFIX}*"],
            "template": {"settings": ES_ITEMS_SETTINGS, "mappings": ES_ITEMS_MAPPINGS},
        },
    )
    await client.close()
