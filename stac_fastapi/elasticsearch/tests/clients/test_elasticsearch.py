import time
import uuid
from copy import deepcopy
from typing import Callable

import pytest
from stac_pydantic import Item
from tests.conftest import MockStarletteRequest

from stac_fastapi.api.app import StacApi
from stac_fastapi.elasticsearch.core import CoreCrudClient
from stac_fastapi.elasticsearch.transactions import (
    BulkTransactionsClient,
    TransactionsClient,
)
from stac_fastapi.types.errors import ConflictError, NotFoundError


def test_create_collection(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    try:
        es_transactions.create_collection(data, request=MockStarletteRequest)
    except Exception:
        pass
    coll = es_core.get_collection(data["id"], request=MockStarletteRequest)
    assert coll["id"] == data["id"]
    es_transactions.delete_collection(data["id"], request=MockStarletteRequest)


@pytest.mark.skip(reason="passing but messing up the next test")
def test_create_collection_already_exists(
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    es_transactions.create_collection(data, request=MockStarletteRequest)

    # change id to avoid mongo duplicate key error
    data["_id"] = str(uuid.uuid4())

    with pytest.raises(ConflictError):
        es_transactions.create_collection(data, request=MockStarletteRequest)


def test_update_collection(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")

    es_transactions.create_collection(data, request=MockStarletteRequest)
    data["keywords"].append("new keyword")
    es_transactions.update_collection(data, request=MockStarletteRequest)

    coll = es_core.get_collection(data["id"], request=MockStarletteRequest)
    assert "new keyword" in coll["keywords"]

    es_transactions.delete_collection(data["id"], request=MockStarletteRequest)


def test_delete_collection(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    es_transactions.create_collection(data, request=MockStarletteRequest)

    es_transactions.delete_collection(data["id"], request=MockStarletteRequest)

    with pytest.raises(NotFoundError):
        es_core.get_collection(data["id"], request=MockStarletteRequest)


def test_get_collection(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    es_transactions.create_collection(data, request=MockStarletteRequest)
    coll = es_core.get_collection(data["id"], request=MockStarletteRequest)
    assert coll["id"] == data["id"]

    es_transactions.delete_collection(data["id"], request=MockStarletteRequest)


def test_get_item(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    collection_data = load_test_data("test_collection.json")
    item_data = load_test_data("test_item.json")
    es_transactions.create_collection(collection_data, request=MockStarletteRequest)
    es_transactions.create_item(item_data, request=MockStarletteRequest)
    coll = es_core.get_item(
        item_id=item_data["id"],
        collection_id=item_data["collection"],
        request=MockStarletteRequest,
    )
    assert coll["id"] == item_data["id"]
    assert coll["collection"] == item_data["collection"]

    es_transactions.delete_collection(
        collection_data["id"], request=MockStarletteRequest
    )
    es_transactions.delete_item(
        item_data["id"], coll["id"], request=MockStarletteRequest
    )


@pytest.mark.skip(reason="unknown")
def test_get_collection_items(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")

    for _ in range(5):
        item["id"] = str(uuid.uuid4())
        es_transactions.create_item(item, request=MockStarletteRequest)

    fc = es_core.item_collection(coll["id"], request=MockStarletteRequest)
    assert len(fc["features"]) == 5

    for item in fc["features"]:
        assert item["collection"] == coll["id"]

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)
    for item in fc["features"]:
        es_transactions.delete_item(
            item["id"], coll["id"], request=MockStarletteRequest
        )


def test_create_item(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)
    item = load_test_data("test_item.json")
    es_transactions.create_item(item, request=MockStarletteRequest)
    resp = es_core.get_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )
    assert Item(**item).dict(
        exclude={"links": ..., "properties": {"created", "updated"}}
    ) == Item(**resp).dict(exclude={"links": ..., "properties": {"created", "updated"}})

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)
    es_transactions.delete_item(item["id"], coll["id"], request=MockStarletteRequest)


def test_create_item_already_exists(
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")
    es_transactions.create_item(item, request=MockStarletteRequest)

    with pytest.raises(ConflictError):
        es_transactions.create_item(item, request=MockStarletteRequest)

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)
    es_transactions.delete_item(item["id"], coll["id"], request=MockStarletteRequest)


def test_update_item(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")
    es_transactions.create_item(item, request=MockStarletteRequest)

    item["properties"]["foo"] = "bar"
    es_transactions.update_item(item, request=MockStarletteRequest)

    updated_item = es_core.get_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )
    assert updated_item["properties"]["foo"] == "bar"

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)
    es_transactions.delete_item(item["id"], coll["id"], request=MockStarletteRequest)


def test_update_geometry(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")
    es_transactions.create_item(item, request=MockStarletteRequest)

    new_coordinates = [
        [
            [142.15052873427666, -33.82243006904891],
            [140.1000346138806, -34.257132625788756],
            [139.5776607193635, -32.514709769700254],
            [141.6262528041627, -32.08081674221862],
            [142.15052873427666, -33.82243006904891],
        ]
    ]

    item["geometry"]["coordinates"] = new_coordinates
    es_transactions.update_item(item, request=MockStarletteRequest)

    updated_item = es_core.get_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )
    assert updated_item["geometry"]["coordinates"] == new_coordinates

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)
    es_transactions.delete_item(item["id"], coll["id"], request=MockStarletteRequest)


def test_delete_item(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")
    es_transactions.create_item(item, request=MockStarletteRequest)

    es_transactions.delete_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )

    es_transactions.delete_collection(coll["id"], request=MockStarletteRequest)

    with pytest.raises(NotFoundError):
        es_core.get_item(item["id"], item["collection"], request=MockStarletteRequest)


# @pytest.mark.skip(reason="might need a larger timeout")
def test_bulk_item_insert(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    es_bulk_transactions: BulkTransactionsClient,
    load_test_data: Callable,
):
    coll = load_test_data("test_collection.json")
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    item = load_test_data("test_item.json")

    items = []
    for _ in range(10):
        _item = deepcopy(item)
        _item["id"] = str(uuid.uuid4())
        items.append(_item)

    # fc = es_core.item_collection(coll["id"], request=MockStarletteRequest)
    # assert len(fc["features"]) == 0

    es_bulk_transactions.bulk_item_insert(items=items)
    time.sleep(3)
    fc = es_core.item_collection(coll["id"], request=MockStarletteRequest)
    assert len(fc["features"]) >= 10

    # for item in items:
    #     es_transactions.delete_item(
    #         item["id"], item["collection"], request=MockStarletteRequest
    #     )


@pytest.mark.skip(reason="Not working")
def test_landing_page_no_collection_title(
    es_core: CoreCrudClient,
    es_transactions: TransactionsClient,
    load_test_data: Callable,
    api_client: StacApi,
):
    class MockStarletteRequestWithApp(MockStarletteRequest):
        app = api_client.app

    coll = load_test_data("test_collection.json")
    del coll["title"]
    es_transactions.create_collection(coll, request=MockStarletteRequest)

    landing_page = es_core.landing_page(request=MockStarletteRequestWithApp)
    for link in landing_page["links"]:
        if link["href"].split("/")[-1] == coll["id"]:
            assert link["title"]
