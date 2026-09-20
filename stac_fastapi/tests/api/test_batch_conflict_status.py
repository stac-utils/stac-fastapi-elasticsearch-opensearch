"""Regression coverage for zero-success ItemCollection classifications."""

import os
import uuid
from copy import deepcopy
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.sfeos_helpers.database import ItemAlreadyExistsError
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX

from ..conftest import SearchSettings, create_item, instantiate_api

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
