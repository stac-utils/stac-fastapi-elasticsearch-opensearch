"""Regression coverage for zero-success ItemCollection classifications."""

import os
import uuid
from copy import deepcopy
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.core.validate import validate_batch_with_stac_validator
from stac_fastapi.extensions.bulk_transactions import Items
from stac_fastapi.sfeos_helpers.database import ItemAlreadyExistsError
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX

from ..conftest import SearchSettings, create_collection, create_item, instantiate_api

pytestmark = pytest.mark.datetime_filtering


@pytest.fixture(autouse=True)
def synchronous_batch(monkeypatch):
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    monkeypatch.setenv("MAX_BATCH_SIZE", "0")


def _conflict(item_id):
    return {
        "create": {
            "_id": f"{item_id}|collection",
            "status": 409,
            "error": {"type": "conflict", "reason": "already exists"},
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
@pytest.mark.parametrize("response_models", [False, True])
@pytest.mark.parametrize("validator", ["false", "true"])
async def test_all_conflict_post_returns_409_and_preserves_items(
    ctx, app_client, txn_client, monkeypatch, strict, response_models, validator
):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    second = deepcopy(ctx.item)
    second["id"] += "-second"
    await create_item(txn_client, second)
    items = [ctx.item, second]
    before = [
        (await app_client.get(f"/collections/{i['collection']}/items/{i['id']}")).json()
        for i in items
    ]
    changed = deepcopy(items)
    for item in changed:
        item["properties"]["title"] = "must not replace"

    api = instantiate_api(
        settings=SearchSettings(enable_response_models=response_models)
    )
    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test-server"
    ) as client:
        response = await client.post(
            f"/collections/{ctx.collection['id']}/items",
            json={"type": "FeatureCollection", "features": changed},
        )

    assert response.status_code == 409
    if strict == "false":
        detail = response.json()["detail"]
        assert detail["message"] == "No items were added to the database."
        assert detail["summary"]["conflict_count"] == 2
        assert detail["validation_errors"] == {}
        assert set(detail["conflict_errors"]) == {item["id"] for item in items}
    assert [
        (await app_client.get(f"/collections/{i['collection']}/items/{i['id']}")).json()
        for i in items
    ] == before


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "scenario",
    [
        "all_conflicts",
        "all_invalid",
        "all_preprocessing_skipped",
        "conflict_and_database_error",
        "conflict_and_validation_error",
        "conflict_and_preprocessing_skip",
        "input_duplicate",
        "incomplete_conflicts",
        "no_errors",
    ],
)
async def test_zero_success_classification(monkeypatch, strict, scenario):
    """Only a clean, complete conflict batch is classified as 409."""
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    features = [
        {"id": "first", "collection": "collection"},
        {"id": "second", "collection": "collection"},
    ]
    processed = features.copy()
    valid = features.copy()
    validation_errors = {}
    errors = [_conflict("first"), _conflict("second")]

    if scenario == "all_invalid":
        valid = []
        validation_errors = {"invalid": ["first", "second"]}
        errors = []
    elif scenario == "all_preprocessing_skipped":
        processed = valid = []
        errors = []
    elif scenario == "conflict_and_database_error":
        errors[1]["create"]["status"] = 400
        errors[1]["create"]["error"]["reason"] = "invalid field"
    elif scenario == "conflict_and_validation_error":
        valid = features[:1]
        validation_errors = {"invalid": ["second"]}
        errors = errors[:1]
    elif scenario == "conflict_and_preprocessing_skip":
        processed = valid = features[:1]
        errors = errors[:1]
    elif scenario == "input_duplicate":
        features.append(features[0].copy())
    elif scenario == "incomplete_conflicts":
        errors = errors[:1]
    elif scenario == "no_errors":
        errors = []

    monkeypatch.setattr(
        BulkTransactionsClient,
        "preprocess_item",
        lambda self, item, base_url: item if item in processed else None,
    )
    database = Mock()
    database.bulk_async = AsyncMock(return_value=(0, errors))
    client = TransactionsClient(database=database, session=None, settings=Mock())
    monkeypatch.setattr(
        client,
        "_validate_feature_collection",
        AsyncMock(return_value=(valid, validation_errors)),
    )

    strict_conflict = strict and errors and not validation_errors
    expected_error = ItemAlreadyExistsError if strict_conflict else HTTPException
    with pytest.raises(expected_error) as exc:
        await client._create_feature_collection(
            "collection",
            {"type": "FeatureCollection", "features": features},
            "http://test-server/",
            False,
        )

    if not strict_conflict:
        assert exc.value.status_code == (409 if scenario == "all_conflicts" else 400)

    if strict and (validation_errors or not valid):
        database.bulk_async.assert_not_awaited()
    else:
        database.bulk_async.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
async def test_batch_missing_collection_remains_404(
    app_client, load_test_data, monkeypatch, strict
):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    item = load_test_data("test_item.json")
    item["collection"] = f"missing-{uuid.uuid4()}"
    response = await app_client.post(
        f"/collections/{item['collection']}/items",
        json={"type": "FeatureCollection", "features": [item]},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("response_models", [False, True])
async def test_bulk_items_serializes_conflicts_and_preserves_content(
    ctx, app_client, txn_client, monkeypatch, strict, validator, response_models
):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    existing = deepcopy(ctx.item)
    url = f"/collections/{existing['collection']}/items/{existing['id']}"
    before = (await app_client.get(url)).json()
    existing["properties"]["title"] = "must not replace"
    new_item = deepcopy(existing)
    new_item["id"] = str(uuid.uuid4())
    api = instantiate_api(
        settings=SearchSettings(enable_response_models=response_models)
    )
    async with AsyncClient(
        transport=ASGITransport(app=api.app), base_url="http://test-server"
    ) as client:
        response = await client.post(
            f"/collections/{existing['collection']}/bulk_items",
            json={
                "items": {item["id"]: item for item in (existing, new_item)},
                "method": "insert",
            },
        )
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING", "").lower() == "true":
        assert response.status_code == 400
        assert "bulk_items endpoint is invalid" in response.json()["detail"]
        assert (await app_client.get(url)).json() == before
        assert (
            await app_client.get(
                f"/collections/{new_item['collection']}/items/{new_item['id']}"
            )
        ).status_code == 404
        return
    assert response.status_code == (409 if strict == "true" else 200), response.text
    if strict == "false":
        body = response.json()
        assert (body["received"], body["success"], body["skipped"]) == (2, 1, 1)
        assert len(body["errors"]) == 1
        error = body["errors"][0]
        assert set(error) == {"id", "msg"}
        assert error["id"] == existing["id"]
        assert "already exists" in error["msg"]
    await txn_client.database.client.indices.refresh(index=f"{ITEMS_INDEX_PREFIX}*")
    assert (await app_client.get(url)).json() == before
    created = await app_client.get(
        f"/collections/{new_item['collection']}/items/{new_item['id']}"
    )
    assert created.status_code == 200
    assert created.json()["properties"]["title"] == new_item["properties"]["title"]


async def _post_bulk_items(collection_id, items):
    api = instantiate_api()
    async with AsyncClient(
        transport=ASGITransport(app=api.app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        return await client.post(
            f"/collections/{collection_id}/bulk_items",
            json={"items": items, "method": "insert"},
        )


def _without_id(item):
    item = deepcopy(item)
    del item["id"]
    return item


def _with_id(item_id):
    def make(item):
        return {**item, "id": item_id}

    return make


MALFORMED_ENTRIES = [
    pytest.param(lambda item: "x", id="string"),
    pytest.param(lambda item: None, id="null"),
    pytest.param(lambda item: 5, id="number"),
    pytest.param(lambda item: [1], id="array"),
    pytest.param(_without_id, id="id-missing"),
    pytest.param(_with_id(None), id="id-null"),
    pytest.param(_with_id(""), id="id-empty"),
    pytest.param(_with_id(5), id="id-number"),
    pytest.param(_with_id([]), id="id-array"),
    pytest.param(_with_id({}), id="id-object"),
]


def _mixed_batch(item, make_bad):
    valid = deepcopy(item)
    valid["id"] = str(uuid.uuid4())
    return valid, {valid["id"]: valid, "bad": make_bad(item)}


@pytest.mark.asyncio
@pytest.mark.parametrize("make_bad", MALFORMED_ENTRIES)
async def test_bulk_items_reports_malformed_entry_and_writes_valid_sibling(
    ctx, app_client, monkeypatch, make_bad
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    valid, items = _mixed_batch(ctx.item, make_bad)
    response = await _post_bulk_items(valid["collection"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["received"], body["success"], body["skipped"]) == (2, 1, 0)
    assert [error["id"] for error in body["errors"]] == ["bad"]
    created = await app_client.get(
        f"/collections/{valid['collection']}/items/{valid['id']}"
    )
    assert created.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("make_bad", MALFORMED_ENTRIES)
async def test_bulk_items_strict_rejects_malformed_batch_without_writing(
    ctx, app_client, monkeypatch, make_bad
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "true")
    valid, items = _mixed_batch(ctx.item, make_bad)
    response = await _post_bulk_items(valid["collection"], items)
    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert [error["id"] for error in detail["errors"]] == ["bad"]
    missing = await app_client.get(
        f"/collections/{valid['collection']}/items/{valid['id']}"
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_bulk_items_validator_reports_missing_id_instead_of_skipping(
    ctx, app_client, monkeypatch
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    valid, items = _mixed_batch(ctx.item, _without_id)
    response = await _post_bulk_items(valid["collection"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["success"], body["skipped"]) == (1, 0)
    assert [error["id"] for error in body["errors"]] == ["bad"]


def _validator_messages(item):
    """Messages the validator reports for `item`, in the order `core.py` joins them."""
    _, errors = validate_batch_with_stac_validator([item])
    messages = [msg for msg, item_ids in errors.items() if item["id"] in item_ids]
    assert messages
    return messages


def _validator_msg(item):
    return "; ".join(_validator_messages(item))


def _invalid_item(item):
    invalid = deepcopy(item)
    invalid["id"] = str(uuid.uuid4())
    del invalid["properties"]["datetime"]
    return invalid


def _validator_env(monkeypatch, strict):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")


@pytest.mark.asyncio
async def test_bulk_items_reports_invalid_item_and_writes_valid_sibling(
    ctx, app_client, monkeypatch
):
    _validator_env(monkeypatch, "false")
    valid, items = _mixed_batch(ctx.item, _invalid_item)
    invalid_id = items["bad"]["id"]
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "received": 2,
        "success": 1,
        "skipped": 0,
        "errors": [{"id": invalid_id, "msg": _validator_msg(items["bad"])}],
    }
    created = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert created.status_code == 200
    missing = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{invalid_id}"
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_bulk_items_strict_rejects_invalid_item_without_writing(
    ctx, app_client, monkeypatch
):
    _validator_env(monkeypatch, "true")
    valid, items = _mixed_batch(ctx.item, _invalid_item)
    invalid_id = items["bad"]["id"]
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 400, response.text
    assert response.json()["detail"] == {
        "message": "Bulk insertion rejected. 1 items failed validation.",
        "summary": {
            "input_count": 2,
            "processed_count": 2,
            "valid_count": 1,
            "skipped_total": 1,
            "validation_error_count": 1,
            "conflict_count": 0,
            "database_error_count": 0,
        },
        "errors": {msg: [invalid_id] for msg in _validator_messages(items["bad"])},
    }
    missing = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_bulk_items_all_invalid_returns_200_with_errors(ctx, monkeypatch):
    _validator_env(monkeypatch, "false")
    invalid = _invalid_item(ctx.item)
    response = await _post_bulk_items(ctx.collection["id"], {invalid["id"]: invalid})
    assert response.status_code == 200, response.text
    assert response.json() == {
        "received": 1,
        "success": 0,
        "skipped": 0,
        "errors": [{"id": invalid["id"], "msg": _validator_msg(invalid)}],
    }


@pytest.mark.asyncio
async def test_bulk_items_reports_every_error_type_once_in_order(
    ctx, app_client, monkeypatch
):
    """Admission errors come first, then validation, then database conflicts."""
    _validator_env(monkeypatch, "false")
    valid = {**deepcopy(ctx.item), "id": str(uuid.uuid4())}
    invalid = _invalid_item(ctx.item)
    items = {
        valid["id"]: valid,
        "bad": "x",
        invalid["id"]: invalid,
        ctx.item["id"]: ctx.item,
    }
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["received"], body["success"], body["skipped"]) == (4, 1, 1)
    assert body["errors"][:2] == [
        {"id": "bad", "msg": "Item must be a JSON object."},
        {"id": invalid["id"], "msg": _validator_msg(invalid)},
    ]
    assert [error["id"] for error in body["errors"]] == [
        "bad",
        invalid["id"],
        ctx.item["id"],
    ]
    assert "already exists" in body["errors"][2]["msg"]
    created = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert created.status_code == 200


@pytest.mark.asyncio
async def test_bulk_items_reports_malformed_and_invalid_entries_together(
    ctx, monkeypatch
):
    _validator_env(monkeypatch, "false")
    invalid = _invalid_item(ctx.item)
    response = await _post_bulk_items(
        ctx.collection["id"], {invalid["id"]: invalid, "bad": "x"}
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "received": 2,
        "success": 0,
        "skipped": 0,
        "errors": [
            {"id": "bad", "msg": "Item must be a JSON object."},
            {"id": invalid["id"], "msg": _validator_msg(invalid)},
        ],
    }


def test_bulk_items_joins_validator_messages_per_item_in_batch_order(monkeypatch):
    """One entry per failing item, keyed by item id, messages joined in validator order."""
    _validator_env(monkeypatch, "false")
    monkeypatch.setattr(
        "stac_fastapi.core.validate.validate_batch_with_stac_validator",
        lambda items: ([], {"second msg": ["b", "a"], "first msg": ["a"]}),
    )
    database = Mock()
    client = BulkTransactionsClient(database=database, settings=Mock())
    result = client.bulk_item_insert(
        Items(
            items={
                "key-a": {"id": "a", "collection": "c"},
                "key-b": {"id": "b", "collection": "c"},
            }
        )
    )
    assert result == {
        "received": 2,
        "success": 0,
        "skipped": 0,
        "errors": [
            {"id": "a", "msg": "second msg; first msg"},
            {"id": "b", "msg": "second msg"},
        ],
    }
    database.bulk_sync.assert_not_called()


OTHER_COLLECTION = "bulk-items-other-collection"

MISSING_COLLECTIONS = [
    pytest.param(
        lambda item: {k: v for k, v in item.items() if k != "collection"},
        id="absent",
    ),
    pytest.param(lambda item: {**item, "collection": None}, id="null"),
    pytest.param(lambda item: {**item, "collection": ""}, id="empty"),
]

MISMATCHED_COLLECTIONS = [
    pytest.param(OTHER_COLLECTION, f"'{OTHER_COLLECTION}'", id="other"),
    pytest.param("no-such-collection", "'no-such-collection'", id="nonexistent"),
    pytest.param(5, "5", id="number"),
    pytest.param(0, "0", id="zero"),
    pytest.param([], "[]", id="array"),
    pytest.param({}, "{}", id="object"),
    pytest.param(False, "false", id="false"),
    pytest.param(2**64, "18446744073709551616", id="big-int"),
]


def _mismatch_error(shown, collection_id):
    return {
        "id": "bad",
        "msg": f"Item collection {shown} does not match path collection '{collection_id}'",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
@pytest.mark.parametrize("strip_collection", MISSING_COLLECTIONS)
async def test_bulk_items_fills_missing_collection_from_path(
    ctx, app_client, monkeypatch, strict, strip_collection
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    item = strip_collection({**deepcopy(ctx.item), "id": str(uuid.uuid4())})
    response = await _post_bulk_items(ctx.collection["id"], {item["id"]: item})
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["success"], body["errors"]) == (1, [])
    created = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{item['id']}"
    )
    assert created.status_code == 200
    assert created.json()["collection"] == ctx.collection["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value,shown", MISMATCHED_COLLECTIONS)
async def test_bulk_items_reports_mismatched_collection_and_writes_valid_sibling(
    ctx, app_client, txn_client, monkeypatch, value, shown
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    if value == OTHER_COLLECTION:
        await create_collection(txn_client, {**ctx.collection, "id": value})
    valid, items = _mixed_batch(ctx.item, lambda item: {**item, "collection": value})
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["received"], body["success"], body["skipped"]) == (2, 1, 0)
    assert body["errors"] == [_mismatch_error(shown, ctx.collection["id"])]
    created = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert created.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("value,shown", MISMATCHED_COLLECTIONS)
async def test_bulk_items_strict_rejects_mismatched_collection_without_writing(
    ctx, app_client, txn_client, monkeypatch, value, shown
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "true")
    if value == OTHER_COLLECTION:
        await create_collection(txn_client, {**ctx.collection, "id": value})
    valid, items = _mixed_batch(ctx.item, lambda item: {**item, "collection": value})
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 400, response.text
    assert response.json()["detail"] == {
        "message": "Bulk insertion rejected. 1 items are malformed.",
        "errors": [_mismatch_error(shown, ctx.collection["id"])],
    }
    missing = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
async def test_bulk_items_nonexistent_path_collection_returns_404_without_writing(
    ctx, app_client, monkeypatch, strict
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    missing_collection = f"missing-{uuid.uuid4()}"
    item = {**deepcopy(ctx.item), "id": str(uuid.uuid4())}
    response = await _post_bulk_items(missing_collection, {item["id"]: item})
    assert response.status_code == 404, response.text
    assert response.json() == {
        "code": "NotFoundError",
        "description": f"Collection {missing_collection} does not exist",
    }
    not_written = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{item['id']}"
    )
    assert not_written.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
async def test_bulk_items_nonexistent_path_collection_returns_404_before_admission(
    monkeypatch, strict
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    missing_collection = f"missing-{uuid.uuid4()}"
    response = await _post_bulk_items(missing_collection, {"bad": "x", "no-id": {}})
    assert response.status_code == 404, response.text
    assert response.json() == {
        "code": "NotFoundError",
        "description": f"Collection {missing_collection} does not exist",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_first", [True, False])
async def test_bulk_items_mixed_batch_writes_only_to_path_collection(
    ctx, app_client, txn_client, monkeypatch, valid_first
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    await create_collection(txn_client, {**ctx.collection, "id": OTHER_COLLECTION})
    ok = {**deepcopy(ctx.item), "id": str(uuid.uuid4())}
    bad = {
        **deepcopy(ctx.item),
        "id": str(uuid.uuid4()),
        "collection": OTHER_COLLECTION,
    }
    entries = [("ok", ok), ("bad", bad)]
    items = dict(entries if valid_first else entries[::-1])
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] == 1
    assert body["errors"] == [
        _mismatch_error(f"'{OTHER_COLLECTION}'", ctx.collection["id"])
    ]
    await txn_client.database.client.indices.refresh(index=f"{ITEMS_INDEX_PREFIX}*")
    found = await app_client.get(
        "/search", params={"collections": ctx.collection["id"], "ids": ok["id"]}
    )
    assert [feature["id"] for feature in found.json()["features"]] == [ok["id"]]
    assert (
        await app_client.get(f"/collections/{OTHER_COLLECTION}/items/{bad['id']}")
    ).status_code == 404
    other_items = await app_client.get(f"/collections/{OTHER_COLLECTION}/items")
    assert other_items.json()["features"] == []


@pytest.mark.asyncio
async def test_bulk_items_validator_reports_mismatched_collection(
    ctx, app_client, monkeypatch
):
    monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", "false")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    valid, items = _mixed_batch(
        ctx.item, lambda item: {**item, "collection": OTHER_COLLECTION}
    )
    response = await _post_bulk_items(ctx.collection["id"], items)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] == 1
    assert body["errors"] == [
        _mismatch_error(f"'{OTHER_COLLECTION}'", ctx.collection["id"])
    ]
    created = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{valid['id']}"
    )
    assert created.status_code == 200
