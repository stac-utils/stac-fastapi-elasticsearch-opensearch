import pytest

from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.types import stac as stac_types

from ..conftest import MockRequest


@pytest.mark.asyncio
async def test_item_serializer(monkeypatch, load_test_data):
    monkeypatch.setenv("STAC_INDEX_ASSETS", "false")

    request = MockRequest

    item_data = load_test_data("test_item.json")

    item = stac_types.Item(**item_data)

    serialized_item = ItemSerializer.stac_to_db(stac_data=item, base_url=str(request.base_url))

    unserialized_item = ItemSerializer.db_to_stac(item=serialized_item, base_url=str(request.base_url))

    assert unserialized_item == item


@pytest.mark.asyncio
async def test_item_serializer_with_asset_indexing(monkeypatch, load_test_data):
    monkeypatch.setenv("STAC_INDEX_ASSETS", "true")

    request = MockRequest

    item_data = load_test_data("test_item.json")

    item = stac_types.Item(**item_data)

    serialized_item = ItemSerializer.stac_to_db(stac_data=item, base_url=str(request.base_url))

    unserialized_item = ItemSerializer.db_to_stac(item=serialized_item, base_url=str(request.base_url))

    assert unserialized_item == item


@pytest.mark.asyncio
async def test_collection_serializer(monkeypatch, load_test_data):
    monkeypatch.setenv("STAC_INDEX_ASSETS", "false")

    request = MockRequest
    collection_data = load_test_data("test_collection.json")

    collection = stac_types.Collection(**collection_data)

    serialized_collection = CollectionSerializer.stac_to_db(collection=collection, request=request)

    unserialized_collection = CollectionSerializer.db_to_stac(
        collection=serialized_collection, request=request, extensions=serialized_collection["stac_extensions"]
    )

    assert unserialized_collection == collection


@pytest.mark.asyncio
async def test_collection_serializer_with_asset_indexing(monkeypatch, load_test_data):
    monkeypatch.setenv("STAC_INDEX_ASSETS", "true")

    request = MockRequest
    collection_data = load_test_data("test_collection.json")

    collection = stac_types.Collection(**collection_data)

    serialized_collection = CollectionSerializer.stac_to_db(collection=collection, request=request)

    unserialized_collection = CollectionSerializer.db_to_stac(
        collection=serialized_collection, request=request, extensions=serialized_collection["stac_extensions"]
    )

    assert unserialized_collection == collection
