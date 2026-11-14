"""Unit tests for bulk transaction error serialization."""

from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.core.utilities import format_bulk_errors
from stac_fastapi.extensions.bulk_transactions import Items
from stac_fastapi.sfeos_helpers.database import BulkIndexError, ItemAlreadyExistsError

COLLECTION_ID = "fao:collection|v1"
FAILED_ID = "item|with|pipes"


@pytest.fixture(autouse=True)
def synchronous_unvalidated_batch(monkeypatch):
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    monkeypatch.setenv("MAX_BATCH_SIZE", "0")
    monkeypatch.setenv("ITEMS_INDEX_STRATEGY", "simple")
    monkeypatch.delenv("ENABLE_DATETIME_INDEX_FILTERING", raising=False)
    monkeypatch.setattr(
        BulkTransactionsClient, "preprocess_item", lambda self, item, base_url: item
    )


def _bulk_error(status):
    return {
        "create": {
            "_id": f"{FAILED_ID}|{COLLECTION_ID}",
            "status": status,
            "error": {"reason": "already exists" if status == 409 else "invalid field"},
        }
    }


def test_format_bulk_errors_preserves_item_id_and_reason():
    errors = [
        {
            "create": {
                "_id": "item-1|collection-1",
                "status": 409,
                "error": {"type": "conflict", "reason": "already exists"},
            }
        },
        {
            "index": {
                "_id": "item-2|collection-1",
                "status": 400,
                "error": {"type": "mapper", "reason": "invalid field"},
            }
        },
    ]

    item_ids = {
        "item-1|collection-1": ("item-1", "collection-1"),
        "item-2|collection-1": ("item-2", "collection-1"),
    }
    assert format_bulk_errors(errors, item_ids) == [
        {"id": "item-1", "msg": "already exists"},
        {"id": "item-2", "msg": "invalid field"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("success", [0, 1])
@pytest.mark.parametrize("status", [400, 409])
@pytest.mark.parametrize("strict", [False, True])
async def test_feature_collection_errors_preserve_public_ids(
    monkeypatch, success, status, strict
):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    features = [{"id": FAILED_ID, "collection": COLLECTION_ID}]
    if success:
        features.append({"id": "added|item", "collection": COLLECTION_ID})
    error = _bulk_error(status)
    database = Mock()
    database.bulk_async = AsyncMock(return_value=(success, [error]))
    client = TransactionsClient(database=database, session=None, settings=Mock())
    monkeypatch.setattr(
        client, "_validate_feature_collection", AsyncMock(return_value=(features, {}))
    )
    request = {"type": "FeatureCollection", "features": features}

    if strict:
        expected = ItemAlreadyExistsError if status == 409 else BulkIndexError
        with pytest.raises(expected) as exc:
            await client._create_feature_collection(COLLECTION_ID, request, "", False)
        assert exc.value.collection_id == COLLECTION_ID
        if status == 409:
            assert exc.value.item_id == FAILED_ID
        else:
            assert exc.value.errors == [error]
    else:
        if success:
            result = await client._create_feature_collection(
                COLLECTION_ID, request, "", False
            )
            assert result["successfully_added"] == ["added|item"]
            label = "conflicts" if status == 409 else "database errors"
            assert result["message"] == f"Processed 2 items: 1 added | 1 {label}"
        else:
            with pytest.raises(HTTPException) as exc:
                await client._create_feature_collection(
                    COLLECTION_ID, request, "", False
                )
            assert exc.value.status_code == status
            result = exc.value.detail
            assert result["message"] == "No items were added to the database."
            assert result["summary"] == {
                "input_count": 1,
                "processed_count": 1,
                "valid_count": 1,
                "skipped_total": int(status == 409),
                "validation_error_count": 0,
                "conflict_count": int(status == 409),
                "database_error_count": int(status == 400),
            }
        if status == 409:
            assert result["conflict_errors"] == {
                FAILED_ID: f"Item '{FAILED_ID}' already exists in collection '{COLLECTION_ID}'"
            }
            assert "database_errors" not in result
        else:
            assert result["database_errors"] == [error]

    if status == 400:
        assert error["create"]["item_id"] == FAILED_ID
        assert error["create"]["collection_id"] == COLLECTION_ID
    assert error["create"]["_id"] == f"{FAILED_ID}|{COLLECTION_ID}"
    database.bulk_async.assert_awaited_once()


@pytest.mark.parametrize("success", [0, 1])
@pytest.mark.parametrize("status", [400, 409])
@pytest.mark.parametrize("strict", [False, True])
def test_bulk_items_errors_preserve_public_ids(monkeypatch, success, status, strict):
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", str(strict).lower())
    features = {FAILED_ID: {"id": FAILED_ID, "collection": COLLECTION_ID}}
    if success:
        features["added|item"] = {"id": "added|item", "collection": COLLECTION_ID}
    error = _bulk_error(status)
    database = Mock()
    database.bulk_sync.return_value = (success, [error])
    client = BulkTransactionsClient(database=database, settings=Mock())

    if strict:
        expected = ItemAlreadyExistsError if status == 409 else BulkIndexError
        with pytest.raises(expected) as exc:
            client.bulk_item_insert(Items(items=features))
        assert exc.value.collection_id == COLLECTION_ID
        if status == 409:
            assert exc.value.item_id == FAILED_ID
        else:
            assert exc.value.errors == [error]
    else:
        assert client.bulk_item_insert(Items(items=features)) == {
            "received": 1 + success,
            "success": success,
            "skipped": int(status == 409),
            "errors": [{"id": FAILED_ID, "msg": error["create"]["error"]["reason"]}],
        }

    if status == 400:
        assert error["create"]["item_id"] == FAILED_ID
        assert error["create"]["collection_id"] == COLLECTION_ID
    assert error["create"]["_id"] == f"{FAILED_ID}|{COLLECTION_ID}"
    database.bulk_sync.assert_called_once()
