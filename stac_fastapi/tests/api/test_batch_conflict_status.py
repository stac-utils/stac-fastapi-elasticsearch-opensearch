from copy import deepcopy
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.sfeos_helpers.database import ItemAlreadyExistsError

from ..conftest import SearchSettings, create_item, instantiate_api


@pytest.fixture(autouse=True)
def synchronous_batch(monkeypatch):
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    monkeypatch.setenv("MAX_BATCH_SIZE", "0")


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
@pytest.mark.parametrize("response_models", [False, True])
@pytest.mark.parametrize("validator", ["false", "true"])
async def test_all_conflict_batch_preserves_items(
    ctx, app_client, txn_client, monkeypatch, strict, response_models, validator
):
    """An all-conflict batch returns 409 without replacing either stored item."""
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    second = deepcopy(ctx.item)
    second["id"] += "-second"
    await create_item(txn_client, second)
    items = [ctx.item, second]
    urls = [f"/collections/{item['collection']}/items/{item['id']}" for item in items]
    before = [(await app_client.get(url)).json() for url in urls]
    changed = deepcopy(items)
    for item in changed:
        item["properties"]["title"] = "must not replace stored item"

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
        assert detail["conflict_errors"]
    assert [(await app_client.get(url)).json() for url in urls] == before


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", ["false", "true"])
async def test_batch_missing_collection_remains_404(
    app_client, load_test_data, monkeypatch, strict
):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", strict)
    item = load_test_data("test_item.json")
    item["collection"] = "missing-batch-collection"
    response = await app_client.post(
        "/collections/missing-batch-collection/items",
        json={"type": "FeatureCollection", "features": [item]},
    )
    assert response.status_code == 404


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
async def test_zero_success_batch_classification(monkeypatch, strict, scenario):
    """Zero writes imply 409 only for an otherwise clean, fully conflicting batch."""
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    features = [
        {"id": "first", "collection": "collection"},
        {"id": "second", "collection": "collection"},
    ]
    processed = features.copy()
    valid = features.copy()
    validation_errors = {}

    def conflict(item_id):
        return {
            "create": {
                "_id": f"{item_id}|collection",
                "status": 409,
                "error": {
                    "type": "version_conflict_engine_exception",
                    "reason": "already exists",
                },
            }
        }

    errors = [conflict("first"), conflict("second")]
    if scenario == "all_invalid":
        valid = []
        validation_errors = {"invalid item": ["first", "second"]}
        errors = []
    elif scenario == "all_preprocessing_skipped":
        processed = valid = []
        errors = []
    elif scenario == "conflict_and_database_error":
        errors[1] = {
            "create": {
                "_id": "second|collection",
                "status": 400,
                "error": {
                    "type": "mapper_parsing_exception",
                    "reason": "invalid field",
                },
            }
        }
    elif scenario == "conflict_and_validation_error":
        valid = features[:1]
        validation_errors = {"invalid item": ["second"]}
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
