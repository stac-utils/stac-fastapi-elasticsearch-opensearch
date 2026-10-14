import sys
import uuid
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest

_repo_root = Path(__file__).resolve()
while _repo_root != _repo_root.parent and not (_repo_root / "scripts").is_dir():
    _repo_root = _repo_root.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from stac_fastapi.sfeos_helpers.database import (  # noqa: E402
    index_alias_by_collection_id,
    mk_item_id,
)
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX  # noqa: E402


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_in_past_date_change_alias_name_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2012-02-12T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2012-02-12",
        f"{ITEMS_INDEX_PREFIX}end_datetime_test-collection_2020-02-16",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_uses_existing_datetime_index_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client, monkeypatch
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-08",
        f"{ITEMS_INDEX_PREFIX}end_datetime_test-collection_2020-02-16",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_different_date_same_index_for_datetime_index(
    mock_datetime_env,
    app_client,
    load_test_data,
    txn_client,
    ctx,
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2020-02-11T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-08",
        f"{ITEMS_INDEX_PREFIX}end_datetime_test-collection_2020-02-16",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_new_index_when_size_limit_exceeded_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2020-02-11T12:30:22Z"

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.return_value = True
        response = await app_client.post(
            f"/collections/{item['collection']}/items", json=item
        )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-08-2020-02-08",
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_1970-01-11-2020-02-07",
        f"{ITEMS_INDEX_PREFIX}end_datetime_test-collection_1970-01-11",
    ]
    all_aliases = set()

    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)

    item_2 = deepcopy(item)
    item_2["id"] = str(uuid.uuid4())
    item_2["properties"]["start_datetime"] = "2020-02-10T12:30:22Z"
    response_2 = await app_client.post(
        f"/collections/{item_2['collection']}/items", json=item_2
    )
    assert response_2.status_code == 201


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_fails_without_datetime_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = None
    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 400


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_same_date_range_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(10):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["start_datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"]["datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"]["end_datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}
    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201
    for item_id in items_dict:
        response = await app_client.get(
            f"/collections/{base_item['collection']}/items/{item_id}"
        )
        assert response.status_code == 200

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-08",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_different_date_ranges_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(3):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["start_datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"]["datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"]["end_datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["start_datetime"] = f"2010-02-{10 + i}T12:30:22Z"
        item["properties"]["datetime"] = f"2010-02-{10 + i}T12:30:22Z"
        item["properties"]["end_datetime"] = f"2010-02-{10 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}

    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(index="*")

    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2010-02-10"
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_size_limit_exceeded_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2019-02", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2010-02-10-2020-02-08",
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-09",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_early_date_in_second_batch_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2008-01-15",
    ]

    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_and_retrieve_by_id_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

    response = await app_client.get(
        f"/collections/{collection_id}/items/{base_item['id']}"
    )
    assert response.status_code == 200


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_collection_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        patch_data = {
            "description": "Updated description via PATCH",
        }
        response = await app_client.patch(
            f"/collections/{collection_id}?refresh=true", json=patch_data
        )
        assert response.status_code == 200
        assert response.json()["description"] == "Updated description via PATCH"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_collection_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        collection_response = await app_client.get(f"/collections/{collection_id}")
        assert collection_response.status_code == 200
        collection_data = collection_response.json()

        collection_data["description"] = "Updated description via PUT"
        collection_data["title"] = "Updated title via PUT"
        response = await app_client.put(
            f"/collections/{collection_id}?refresh=true", json=collection_data
        )
        assert response.json()["description"] == "Updated description via PUT"
        assert response.json()["title"] == "Updated title via PUT"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        patch_data = {"properties": {"description": "Updated description via PATCH"}}

        response = await app_client.patch(
            f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
        )
        assert response.status_code == 200
        assert (
            response.json()["properties"]["description"]
            == "Updated description via PATCH"
        )


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_datetime_field_blocked_use_datetime_false(
    mock_datetime_env, app_client, load_test_data, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    patch_data = {"properties": {"start_datetime": "2020-02-10T00:00:00Z"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 400
    assert "start_datetime" in response.json()["detail"]
    assert "not yet supported" in response.json()["detail"]

    patch_data = {"properties": {"end_datetime": "2020-02-14T23:59:59Z"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 400
    assert "end_datetime" in response.json()["detail"]
    assert "not yet supported" in response.json()["detail"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_datetime_field_blocked_use_datetime_true(
    datetime_filtering_env, app_client, load_test_data, ctx, txn_client, monkeypatch
):
    monkeypatch.setenv("USE_DATETIME", "true")
    if hasattr(txn_client.database.async_index_selector, "cache_manager"):
        await txn_client.database.async_index_selector.cache_manager.clear_cache()

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    patch_data = {"properties": {"datetime": "2020-02-15T12:00:00Z"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 400
    assert "datetime" in response.json()["detail"]
    assert "not yet supported" in response.json()["detail"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_non_datetime_field_allowed(
    mock_datetime_env, app_client, load_test_data, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    patch_data = {"properties": {"platform": "updated-platform"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 200
    assert response.json()["properties"]["platform"] == "updated-platform"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_change_collection_is_rejected(app_client, txn_client, ctx):
    from stac_fastapi.sfeos_helpers.database import mk_item_id

    document = dict(
        index=index_alias_by_collection_id(ctx.item["collection"]),
        id=mk_item_id(ctx.item["id"], ctx.item["collection"]),
    )
    before = await txn_client.database.client.get(**document)
    new_collection = deepcopy(ctx.collection)
    new_collection["id"] = "test-collection-2"
    response = await app_client.post("/collections", json=new_collection)
    assert response.status_code == 201
    aliases_before = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id(new_collection["id"]),
        ignore_unavailable=True,
    )
    response = await app_client.patch(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        json={"collection": new_collection["id"]},
    )
    assert response.status_code == 400
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]
    assert (
        await txn_client.database.client.indices.get_alias(
            index=index_alias_by_collection_id(new_collection["id"]),
            ignore_unavailable=True,
        )
        == aliases_before
    )
    assert not await txn_client.database.client.exists(
        index=index_alias_by_collection_id(new_collection["id"]),
        id=mk_item_id(ctx.item["id"], new_collection["id"]),
    )


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_item_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "start_datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        item_response = await app_client.get(
            f"/collections/{collection_id}/items/{base_item['id']}"
        )
        assert item_response.status_code == 200
        item_data = item_response.json()

        item_data["properties"]["platform"] = "Updated platform via PUT"
        response = await app_client.put(
            f"/collections/{collection_id}/items/{base_item['id']}", json=item_data
        )
        assert response.json()["properties"]["platform"] == "Updated platform via PUT"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_new_item_in_new_collection_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    response = await app_client.post("/collections/new-collection/items", json=item)

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("new-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}end_datetime_new-collection_2020-02-16",
        f"{ITEMS_INDEX_PREFIX}start_datetime_new-collection_2020-02-08",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_invalid_datetime_ordering_should_fail(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client, monkeypatch
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")

    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"
    item["properties"]["start_datetime"] = "2024-02-12T12:30:22Z"
    item["properties"]["end_datetime"] = "2022-02-12T12:30:22Z"

    await app_client.post("/collections", json=new_collection)

    response = await app_client.post("/collections/new-collection/items", json=item)
    assert response.status_code == 400


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_update_item_with_changed_end_datetime(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    updated_item = item.copy()
    updated_item["properties"]["end_datetime"] = "2020-02-19T12:30:22Z"

    response = await app_client.put(
        f"/collections/new-collection/items/{item['id']}", json=updated_item
    )

    assert response.json() == {
        "detail": "Updating 'properties/end_datetime' is not yet supported for datetime-based indexing. This feature will be available in a future release, enabling automatic index and time-based alias updates when datetime values change."
    }


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_update_item_with_changed_datetime(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    updated_item = item.copy()
    updated_item["properties"]["datetime"] = "2020-02-14T12:30:22Z"

    response = await app_client.put(
        f"/collections/new-collection/items/{item['id']}", json=updated_item
    )

    assert response.status_code == 200

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("new-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}end_datetime_new-collection_2020-02-16",
        f"{ITEMS_INDEX_PREFIX}start_datetime_new-collection_2020-02-08",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_search_item_by_datetime_range_with_stac_query(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    response = await app_client.get(
        "/search?collections=new-collection&datetime=2020-02-01T00:00:00Z/2020-02-28T23:59:59Z"
    )
    assert response.status_code == 200

    result = response.json()
    assert result["numberMatched"] > 0
    assert len(result["features"]) > 0
    assert any(feature["id"] == item["id"] for feature in result["features"])


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_search_item_by_start_datetime_with_stac_query(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    response = await app_client.get(
        "/search?collections=new-collection&datetime=2020-02-08T00:00:00Z/.."
    )
    assert response.status_code == 200

    result = response.json()
    assert result["numberMatched"] > 0
    assert any(feature["id"] == item["id"] for feature in result["features"])


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_search_item_not_found_outside_datetime_range(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    response = await app_client.get(
        "/search?collections=new-collection&datetime=2021-01-01T00:00:00Z/2021-12-31T23:59:59Z"
    )
    assert response.status_code == 200

    result = response.json()
    assert result["numberMatched"] == 0
    assert len(result["features"]) == 0


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_search_item_after_datetime_update_with_stac_query(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    await app_client.post("/collections/new-collection/items", json=item)

    updated_item = item.copy()
    updated_item["properties"]["datetime"] = "2020-02-14T12:30:22Z"

    await app_client.put(
        f"/collections/new-collection/items/{item['id']}", json=updated_item
    )

    response = await app_client.get(
        "/search?collections=new-collection&datetime=2020-02-14T00:00:00Z/2020-02-14T23:59:59Z"
    )
    assert response.status_code == 200

    result = response.json()
    assert result["numberMatched"] > 0
    assert any(feature["id"] == item["id"] for feature in result["features"])


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_search_item_by_multiple_collections_with_stac_query(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    collection1 = load_test_data("test_collection.json")
    collection1["id"] = "collection-1"

    collection2 = load_test_data("test_collection.json")
    collection2["id"] = "collection-2"

    item1 = load_test_data("test_item.json")
    item1["collection"] = "collection-1"
    item1["id"] = "item-1"

    item2 = load_test_data("test_item.json")
    item2["collection"] = "collection-2"
    item2["id"] = "item-2"

    await app_client.post("/collections", json=collection1)
    await app_client.post("/collections", json=collection2)
    await app_client.post("/collections/collection-1/items", json=item1)
    await app_client.post("/collections/collection-2/items", json=item2)

    response = await app_client.get(
        "/search?collections=collection-1,collection-2&datetime=2020-02-01T00:00:00Z/2020-02-28T23:59:59Z"
    )
    assert response.status_code == 200

    result = response.json()
    assert result["numberMatched"] >= 2
    feature_ids = {feature["id"] for feature in result["features"]}
    assert "item-1" in feature_ids
    assert "item-2" in feature_ids


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_the_same_date_change_alias_name_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}start_datetime_test-collection_2020-02-08",
        f"{ITEMS_INDEX_PREFIX}end_datetime_test-collection_2020-02-16",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_datetime_field_creates_single_alias(
    datetime_filtering_env,
    app_client,
    ctx,
    load_test_data,
    txn_client,
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = None
    item["properties"]["end_datetime"] = None
    item["properties"]["datetime"] = "2024-06-15T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)
    assert not any("start_datetime" in alias for alias in all_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_alias_created_for_past_date(
    datetime_filtering_env, app_client, ctx, load_test_data, txn_client
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    dt = "2012-02-12T12:30:22Z"
    item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2012-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_reuses_existing_index_for_default_date(
    datetime_filtering_env, app_client, ctx, load_test_data, txn_client
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_groups_same_year_dates_in_single_index(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    dt = "2022-02-12T12:30:22Z"
    item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index=index_alias_by_collection_id("test-collection")
    )
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_rejects_item_without_datetime_field(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["datetime"] = None
    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 400


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_with_same_date_range(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(10):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        dt = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}
    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201
    for item_id in items_dict:
        response = await app_client.get(
            f"/collections/{base_item['collection']}/items/{item_id}"
        )
        assert response.status_code == 200

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_with_different_date_ranges(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(3):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        dt = f"2020-02-{12 + i}T12:30:22Z"
        item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
        items_dict[item["id"]] = item

    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        dt = f"2010-02-{10 + i}T12:30:22Z"
        item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}

    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(index="*")

    expected_aliases = [f"{ITEMS_INDEX_PREFIX}datetime_test-collection_2010-02-10"]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_allows_item_retrieval(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            dt = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }

        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

    response = await app_client.get(
        f"/collections/{collection_id}/items/{base_item['id']}"
    )
    assert response.status_code == 200


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_collection_patch_operation(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            dt = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        patch_data = {
            "description": "Updated description via PATCH",
        }
        response = await app_client.patch(
            f"/collections/{collection_id}?refresh=true", json=patch_data
        )
        assert response.status_code == 200
        assert response.json()["description"] == "Updated description via PATCH"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_collection_put_operation(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            dt = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        collection_response = await app_client.get(f"/collections/{collection_id}")
        assert collection_response.status_code == 200
        collection_data = collection_response.json()

        collection_data["description"] = "Updated description via PUT"
        collection_data["title"] = "Updated title via PUT"
        response = await app_client.put(
            f"/collections/{collection_id}?refresh=true", json=collection_data
        )
        assert response.json()["description"] == "Updated description via PUT"
        assert response.json()["title"] == "Updated title via PUT"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_item_patch_operation(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            dt = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        patch_data = {"properties": {"description": "Updated description via PATCH"}}

        response = await app_client.patch(
            f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
        )
        assert response.status_code == 200
        assert (
            response.json()["properties"]["description"]
            == "Updated description via PATCH"
        )


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_item_put_operation(
    datetime_filtering_env, app_client, load_test_data, txn_client, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            dt = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.side_effect = [10, 26]

        first_items = create_items("2010-02", start_day=10, count=2)
        first_payload = {
            "type": "FeatureCollection",
            "features": list(first_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=first_payload
        )
        assert response.status_code == 201

        second_items = create_items("2008-01", start_day=15, count=3)
        second_payload = {
            "type": "FeatureCollection",
            "features": list(second_items.values()),
        }
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=second_payload
        )
        assert response.status_code == 201

        item_response = await app_client.get(
            f"/collections/{collection_id}/items/{base_item['id']}"
        )
        assert item_response.status_code == 200
        item_data = item_response.json()

        item_data["properties"]["platform"] = "Updated platform via PUT"
        response = await app_client.put(
            f"/collections/{collection_id}/items/{base_item['id']}", json=item_data
        )
        assert response.json()["properties"]["platform"] == "Updated platform via PUT"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_datetime_same_value_allowed_use_datetime_false(
    mock_datetime_env, app_client, load_test_data, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    patch_data = {
        "properties": {
            "start_datetime": base_item["properties"]["start_datetime"],
            "end_datetime": base_item["properties"]["end_datetime"],
        }
    }
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 200

    patch_data = {"properties": {"start_datetime": "2020-02-10T00:00:00Z"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 400
    assert "start_datetime" in response.json()["detail"]
    assert "not yet supported" in response.json()["detail"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_patch_item_datetime_different_value_allowed_use_datetime_false(
    mock_datetime_env, app_client, load_test_data, ctx
):
    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    patch_data = {"properties": {"datetime": "2020-02-15T12:00:00Z"}}
    response = await app_client.patch(
        f"/collections/{collection_id}/items/{base_item['id']}", json=patch_data
    )
    assert response.status_code == 200
    assert response.json()["properties"]["datetime"] == "2020-02-15T12:00:00Z"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_insert_existing_id_different_datetime_index(
    mock_datetime_env, app_client, ctx, txn_client
):
    """Re-ingesting an existing item id under a different datetime must conflict,
    not silently duplicate it in another index (#571)."""
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    # Different start/end range resolves to a different physical index. Force
    # the split via is_index_oversized so the duplicate lands in a genuinely
    # separate index rather than merely widening the existing index's range.
    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["start_datetime"] = "2021-02-08T12:30:22Z"
    duplicate_item["properties"]["datetime"] = "2021-02-12T12:30:22Z"
    duplicate_item["properties"]["end_datetime"] = "2021-02-16T12:30:22Z"

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.return_value = True
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=duplicate_item
        )
    assert response.status_code == 409

    count = await txn_client.database.client.count(
        index=index_alias_by_collection_id(collection_id),
        body={"query": {"term": {"_id": mk_item_id(item_id, collection_id)}}},
    )
    assert count["count"] == 1


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_feature_collection_insert_existing_id_different_datetime_index(
    mock_datetime_env, app_client, ctx, txn_client
):
    """A FeatureCollection re-ingesting an existing item id under a different
    datetime must report a conflict, not silently duplicate it (#571)."""
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    new_item = deepcopy(ctx.item)
    new_item["id"] = str(uuid.uuid4())

    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["start_datetime"] = "2021-02-08T12:30:22Z"
    duplicate_item["properties"]["datetime"] = "2021-02-12T12:30:22Z"
    duplicate_item["properties"]["end_datetime"] = "2021-02-16T12:30:22Z"

    payload = {
        "type": "FeatureCollection",
        "features": [new_item, duplicate_item],
    }
    # Force a split so the duplicate lands in a separate physical index, which
    # is the case only alias-wide detection can catch (#571).
    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.return_value = True
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=payload
        )
    assert response.status_code == 201
    body = response.json()
    assert new_item["id"] in body["successfully_added"]
    assert item_id in body["conflict_errors"]

    count = await txn_client.database.client.count(
        index=index_alias_by_collection_id(collection_id),
        body={"query": {"term": {"_id": mk_item_id(item_id, collection_id)}}},
    )
    assert count["count"] == 1


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_item_nonexistent_returns_404_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data
):
    """PUT of an item that does not exist returns 404, as on main (#881)."""
    collection_id = ctx.collection["id"]

    new_item = load_test_data("test_item.json")
    new_item["id"] = "brand-new-item"

    response = await app_client.put(
        f"/collections/{collection_id}/items/{new_item['id']}", json=new_item
    )
    assert response.status_code == 404


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_update_item_changed_datetime_bypasses_queue(
    mock_datetime_env, app_client, ctx, monkeypatch
):
    """A PUT that changes the datetime fields must reach the database layer's
    datetime validation instead of being queued (#571)."""
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")

    collection_id = ctx.collection["id"]

    updated_item = deepcopy(ctx.item)
    updated_item["properties"]["start_datetime"] = "2020-02-10T12:30:22Z"

    response = await app_client.put(
        f"/collections/{collection_id}/items/{ctx.item['id']}", json=updated_item
    )
    assert response.status_code == 400
    assert "start_datetime" in response.json()["detail"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_queued_insert_existing_id_different_datetime_no_duplicate(
    mock_datetime_env, app_client, ctx, txn_client, monkeypatch
):
    """A queued re-ingest of an existing item id under a different datetime is
    reported as a conflict by the worker instead of duplicating (#571)."""
    from scripts.item_queue_worker import ItemQueueWorker
    from stac_fastapi.core.redis_utils import AsyncRedisQueueManager, connect_redis

    redis = await connect_redis()
    if redis is None:
        pytest.skip("Redis not configured")
    await redis.aclose()

    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")

    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["start_datetime"] = "2021-02-08T12:30:22Z"
    duplicate_item["properties"]["datetime"] = "2021-02-12T12:30:22Z"
    duplicate_item["properties"]["end_datetime"] = "2021-02-16T12:30:22Z"

    payload = {"type": "FeatureCollection", "features": [duplicate_item]}
    response = await app_client.post(
        f"/collections/{collection_id}/items", json=payload
    )
    assert response.status_code == 202

    queue_manager = await AsyncRedisQueueManager.create()
    try:
        pending_items = await queue_manager.get_pending_items(collection_id)
        assert len(pending_items) == 1

        worker = ItemQueueWorker()
        await worker._init_queue_manager()
        try:
            # Force a split so the queued duplicate lands in a separate
            # physical index, where only alias-wide detection catches it
            # (#571).
            with patch(
                "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
            ) as mock_get_size:
                mock_get_size.return_value = True
                await worker._flush_collection(collection_id)
        finally:
            await worker.queue_manager.close()

        await txn_client.database.client.indices.refresh(index="_all")

        count = await txn_client.database.client.count(
            index=index_alias_by_collection_id(collection_id),
            body={"query": {"term": {"_id": mk_item_id(item_id, collection_id)}}},
        )
        assert count["count"] == 1

        failed_key = queue_manager._get_failed_set_key(collection_id)
        failed_ids = await queue_manager.redis.smembers(failed_key)
        failed_ids_str = {
            fid.decode("utf-8") if isinstance(fid, bytes) else fid for fid in failed_ids
        }
        assert item_id in failed_ids_str
    finally:
        await queue_manager.close()


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_insert_existing_id_unrefreshed_no_duplicate(
    mock_datetime_env, app_client, ctx, txn_client
):
    """A same-id insert must conflict even when the existing document has not
    been refreshed yet; _search only sees refreshed docs, mget is real-time
    (#571)."""
    collection_id = ctx.collection["id"]

    # Write the document directly with refresh=False so it is invisible to
    # search but visible to real-time gets.
    existing = deepcopy(ctx.item)
    existing["id"] = str(uuid.uuid4())
    existing_doc_id = mk_item_id(existing["id"], collection_id)
    target_index = await txn_client.database.async_index_inserter.get_target_index(
        collection_id, existing
    )
    await txn_client.database.client.index(
        index=target_index, id=existing_doc_id, body=existing, refresh=False
    )

    # Re-ingest the same id under a datetime that resolves to a different
    # physical index (forced split).
    duplicate_item = deepcopy(existing)
    duplicate_item["properties"]["start_datetime"] = "2021-02-08T12:30:22Z"
    duplicate_item["properties"]["datetime"] = "2021-02-12T12:30:22Z"
    duplicate_item["properties"]["end_datetime"] = "2021-02-16T12:30:22Z"

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.is_index_oversized"
    ) as mock_get_size:
        mock_get_size.return_value = True
        response = await app_client.post(
            f"/collections/{collection_id}/items", json=duplicate_item
        )
    assert response.status_code == 409

    await txn_client.database.client.indices.refresh(index="_all")
    count = await txn_client.database.client.count(
        index=index_alias_by_collection_id(collection_id),
        body={"query": {"term": {"_id": existing_doc_id}}},
    )
    assert count["count"] == 1


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_item_changed_datetime_unrefreshed_no_duplicate(
    mock_datetime_env, app_client, ctx, txn_client
):
    """A PUT that changes the datetime of an unrefreshed existing item must
    still hit datetime-field validation instead of upserting a second copy
    into another index (#571)."""
    collection_id = ctx.collection["id"]

    # Write the document directly with refresh=False so it is invisible to
    # search but visible to real-time gets.
    existing = deepcopy(ctx.item)
    existing["id"] = str(uuid.uuid4())
    existing_doc_id = mk_item_id(existing["id"], collection_id)
    target_index = await txn_client.database.async_index_inserter.get_target_index(
        collection_id, existing
    )
    await txn_client.database.client.index(
        index=target_index, id=existing_doc_id, body=existing, refresh=False
    )

    updated_item = deepcopy(existing)
    updated_item["properties"]["start_datetime"] = "2021-02-08T12:30:22Z"
    updated_item["properties"]["datetime"] = "2021-02-12T12:30:22Z"
    updated_item["properties"]["end_datetime"] = "2021-02-16T12:30:22Z"

    response = await app_client.put(
        f"/collections/{collection_id}/items/{existing['id']}", json=updated_item
    )
    assert response.status_code == 400
    assert "start_datetime" in response.json()["detail"]

    await txn_client.database.client.indices.refresh(index="_all")
    count = await txn_client.database.client.count(
        index=index_alias_by_collection_id(collection_id),
        body={"query": {"term": {"_id": existing_doc_id}}},
    )
    assert count["count"] == 1


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_async_intra_batch_duplicate_id_no_duplicate(
    mock_datetime_env, app_client, ctx, txn_client, load_test_data
):
    """Two same-id items in a single bulk batch must not both be written.

    The second copy is reported as a synthetic 409 conflict even though neither
    item exists in the database yet, because _id uniqueness is only enforced per
    physical index and the two datetimes may resolve to different shards (#571).
    The spy on prepare_bulk_actions asserts the duplicate never reaches the bulk
    request, independent of index routing.
    """
    collection_id = ctx.collection["id"]

    first = deepcopy(ctx.item)
    first["id"] = "intra-batch-duplicate"
    second = deepcopy(first)
    second["properties"]["datetime"] = "2020-02-13T00:00:00Z"

    inserter = txn_client.database.async_index_inserter
    real_prepare = inserter.prepare_bulk_actions
    seen_batches = []

    async def spy_prepare(cid, items, op_type="create"):
        seen_batches.append([i["id"] for i in items])
        return await real_prepare(cid, items, op_type)

    inserter.prepare_bulk_actions = spy_prepare

    success, errors = await txn_client.database.bulk_async(
        collection_id,
        [first, second],
        op_type="create",
        refresh="wait_for",
    )

    assert seen_batches == [[first["id"]]]
    assert success == 1
    assert len(errors) == 1
    conflict = errors[0]["create"]
    assert conflict["status"] == 409
    assert conflict["_id"] == mk_item_id(first["id"], collection_id)
    assert conflict["_index"] == index_alias_by_collection_id(collection_id)

    count = await txn_client.database.client.count(
        index=index_alias_by_collection_id(collection_id),
        body={"query": {"term": {"_id": mk_item_id(first["id"], collection_id)}}},
    )
    assert count["count"] == 1


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_item_after_index_rollover_no_duplicate(
    datetime_filtering_env, app_client, ctx, txn_client
):
    """A PUT on an item whose physical index has rolled over must overwrite
    the document in place, not write a second copy to the new index (#571)."""
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]
    doc_id = mk_item_id(item_id, collection_id)
    alias = index_alias_by_collection_id(collection_id)
    client = txn_client.database.client

    # Simulate a rollover: a second physical index that sorts after the one
    # holding ctx.item, so datetime target resolution would pick it.
    await client.indices.create(
        index=f"items_{collection_id}-{uuid.uuid4()}",
        body={
            "aliases": {
                alias: {},
                f"items_datetime_{collection_id}_2020-02-20-2020-02-28": {},
            }
        },
    )

    # Clear the shared alias cache so the target is resolved from the
    # post-rollover index layout deterministically.
    selector = txn_client.database.async_index_selector
    if hasattr(selector, "cache_manager") and hasattr(
        selector.cache_manager, "clear_cache"
    ):
        await selector.cache_manager.clear_cache()

    # PUT the original item with a benign non-datetime change. Datetime
    # routing would send it to the new index; it must instead be written back
    # to the index where it already lives.
    updated_item = deepcopy(ctx.item)
    updated_item["properties"]["gsd"] = 99
    response = await app_client.put(
        f"/collections/{collection_id}/items/{item_id}", json=updated_item
    )
    assert response.status_code == 200

    await client.indices.refresh(index="_all")
    count = await client.count(
        index=alias,
        body={"query": {"term": {"_id": doc_id}}},
    )
    assert count["count"] == 1
