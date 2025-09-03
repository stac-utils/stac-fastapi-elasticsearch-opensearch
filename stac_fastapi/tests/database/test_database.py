import os
import uuid

import pytest
from stac_pydantic import api

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
)

from ..conftest import MockRequest, database


@pytest.mark.asyncio
async def test_index_mapping_collections(ctx):
    response = await database.client.indices.get_mapping(index=COLLECTIONS_INDEX)
    if not isinstance(response, dict):
        response = response.body
    actual_mappings = next(iter(response.values()))["mappings"]
    assert (
        actual_mappings["dynamic_templates"]
        == ES_COLLECTIONS_MAPPINGS["dynamic_templates"]
    )


@pytest.mark.asyncio
async def test_index_mapping_items(txn_client, load_test_data):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    collection = load_test_data("test_collection.json")
    collection["id"] = str(uuid.uuid4())
    await txn_client.create_collection(
        api.Collection(**collection), request=MockRequest
    )
    response = await database.client.indices.get_mapping(
        index=index_alias_by_collection_id(collection["id"])
    )
    if not isinstance(response, dict):
        response = response.body
    actual_mappings = next(iter(response.values()))["mappings"]
    assert (
        actual_mappings["dynamic_templates"] == ES_ITEMS_MAPPINGS["dynamic_templates"]
    )
    await txn_client.delete_collection(collection["id"])
