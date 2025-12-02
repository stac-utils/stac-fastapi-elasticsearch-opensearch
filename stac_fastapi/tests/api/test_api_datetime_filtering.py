import os
import uuid
from copy import deepcopy
from unittest.mock import patch

import pytest


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_in_past_date_change_alias_name_for_datetime_index(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2012-02-12T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_start_datetime_test-collection_2012-02-12",
        "items_end_datetime_test-collection_2020-02-16",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_start_datetime_test-collection_2020-02-08",
        "items_end_datetime_test-collection_2020-02-16",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2020-02-11T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_start_datetime_test-collection_2020-02-08",
        "items_end_datetime_test-collection_2020-02-16",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["start_datetime"] = "2020-02-11T12:30:22Z"

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
    ) as mock_get_size:
        mock_get_size.return_value = 26.0
        response = await app_client.post(
            f"/collections/{item['collection']}/items", json=item
        )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        "items_start_datetime_test-collection_2020-02-08-2020-02-11",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        "items_start_datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    return all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_different_date_ranges_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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

    expected_aliases = ["items_start_datetime_test-collection_2010-02-10"]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_size_limit_exceeded_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
        "items_start_datetime_test-collection_2010-02-10-2020-02-08",
        "items_start_datetime_test-collection_2020-02-09",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
        "items_start_datetime_test-collection_2008-01-15-2020-02-08",
        "items_start_datetime_test-collection_2020-02-09",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
async def test_put_item_for_datetime_index(
    mock_datetime_env, app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

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
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    new_collection = load_test_data("test_collection.json")
    new_collection["id"] = "new-collection"

    item = load_test_data("test_item.json")
    item["collection"] = "new-collection"

    await app_client.post("/collections", json=new_collection)
    response = await app_client.post("/collections/new-collection/items", json=item)

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index="items_new-collection"
    )
    expected_aliases = [
        "items_end_datetime_new-collection_2020-02-16",
        "items_start_datetime_new-collection_2020-02-08",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_invalid_datetime_ordering_should_fail(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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

    assert response.status_code == 200

    indices = await txn_client.database.client.indices.get_alias(
        index="items_new-collection"
    )
    expected_aliases = [
        "items_end_datetime_new-collection_2020-02-19",
        "items_start_datetime_new-collection_2020-02-08",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_update_item_with_changed_datetime(
    mock_datetime_env, app_client, ctx, load_test_data, txn_client
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
        index="items_new-collection"
    )
    expected_aliases = [
        "items_end_datetime_new-collection_2020-02-16",
        "items_start_datetime_new-collection_2020-02-08",
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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_start_datetime_test-collection_2020-02-08",
        "items_end_datetime_test-collection_2020-02-16",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_datetime_field_creates_single_alias(
    app_client,
    ctx,
    load_test_data,
    txn_client,
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
        index="items_test-collection"
    )
    expected_aliases = [
        "items_datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)
    assert not any("start_datetime" in alias for alias in all_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_alias_created_for_past_date(
    app_client, ctx, load_test_data, txn_client
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["datetime"] = "2012-02-12T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )
    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_datetime_test-collection_2012-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_reuses_existing_index_for_default_date(
    app_client, ctx, load_test_data, txn_client
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_groups_same_year_dates_in_single_index(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["datetime"] = "2022-02-12T12:30:22Z"

    response = await app_client.post(
        f"/collections/{item['collection']}/items", json=item
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(
        index="items_test-collection"
    )
    expected_aliases = [
        "items_datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_creates_new_index_when_size_limit_exceeded(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["properties"]["datetime"] = "2024-02-12T12:30:22Z"

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
    ) as mock_get_size:
        mock_get_size.return_value = 26.0
        response = await app_client.post(
            f"/collections/{item['collection']}/items", json=item
        )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        "items_datetime_test-collection_2020-02-12-2024-02-12",
        "items_datetime_test-collection_2024-02-13",
    ]
    all_aliases = set()

    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)

    item_2 = deepcopy(item)
    item_2["id"] = str(uuid.uuid4())
    item_2["properties"]["datetime"] = "2023-02-12T12:30:22Z"
    response_2 = await app_client.post(
        f"/collections/{item_2['collection']}/items", json=item_2
    )
    assert response_2.status_code == 201


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_rejects_item_without_datetime_field(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

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
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(10):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}
    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201

    indices = await txn_client.database.client.indices.get_alias(index="*")
    expected_aliases = [
        "items_datetime_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    return all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_with_different_date_ranges(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    base_item = load_test_data("test_item.json")
    items_dict = {}

    for i in range(3):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["datetime"] = f"2020-02-{12 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = str(uuid.uuid4())
        item["properties"]["datetime"] = f"2010-02-{10 + i}T12:30:22Z"
        items_dict[item["id"]] = item

    payload = {"type": "FeatureCollection", "features": list(items_dict.values())}

    response = await app_client.post(
        f"/collections/{base_item['collection']}/items", json=payload
    )

    assert response.status_code == 201
    indices = await txn_client.database.client.indices.get_alias(index="*")

    expected_aliases = ["items_datetime_test-collection_2010-02-10"]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_handles_size_limit_correctly(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
        "items_datetime_test-collection_2010-02-10-2020-02-12",
        "items_datetime_test-collection_2020-02-13",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_splits_index_when_earlier_date_added_after_limit(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
        "items_datetime_test-collection_2008-01-15-2020-02-12",
        "items_datetime_test-collection_2020-02-13",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_datetime_index_bulk_insert_allows_item_retrieval(
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
    app_client, load_test_data, txn_client, ctx
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("Datetime index filtering not enabled")

    base_item = load_test_data("test_item.json")
    collection_id = base_item["collection"]

    def create_items(date_prefix: str, start_day: int, count: int) -> dict:
        items = {}
        for i in range(count):
            item = deepcopy(base_item)
            item["id"] = str(uuid.uuid4())
            item["properties"][
                "datetime"
            ] = f"{date_prefix}-{start_day + i:02d}T12:30:22Z"
            items[item["id"]] = item
        return items

    with patch(
        "stac_fastapi.sfeos_helpers.search_engine.managers.IndexSizeManager.get_index_size_in_gb"
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
