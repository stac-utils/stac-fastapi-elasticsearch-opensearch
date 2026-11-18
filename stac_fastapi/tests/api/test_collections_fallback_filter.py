"""CQL2 filters keep their meaning in the URL-length index fallback (#1029)."""

import copy
import importlib
import os

import pytest
import pytest_asyncio

from ..conftest import create_collection, create_item, delete_collections_and_items

BACKEND = os.getenv("BACKEND", "elasticsearch").lower()
database_logic = importlib.import_module(f"stac_fastapi.{BACKEND}.database_logic")

BEFORE_2021 = "2021-01-01T00:00:00Z"


def _branch(collection_id: str, op: str) -> dict:
    return {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "collection"}, collection_id]},
            {"op": op, "args": [{"property": "datetime"}, BEFORE_2021]},
        ],
    }


@pytest_asyncio.fixture
async def two_collections(
    datetime_filtering_env, txn_client, test_collection, test_item, monkeypatch
):
    """Collections A and B with one 2020 item each, searched via the fallback.

    Only the datetime strategy passes CQL2 collection ids to the fallback.
    """
    monkeypatch.setattr(database_logic, "ES_MAX_URL_LENGTH", 0)
    await delete_collections_and_items(txn_client)
    try:
        for collection_id in ("A", "B"):
            collection = copy.deepcopy(test_collection)
            collection["id"] = collection_id
            await create_collection(txn_client, collection)
            item = copy.deepcopy(test_item)
            item["id"] = f"item-{collection_id}"
            item["collection"] = collection_id
            await create_item(txn_client, item)
        yield
    finally:
        await delete_collections_and_items(txn_client)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cql2_filter, expected_ids",
    [
        (
            {"op": "or", "args": [_branch("A", ">"), _branch("B", "<")]},
            ["item-B"],
        ),
        (
            {"op": "=", "args": [{"property": "collection"}, "A"]},
            ["item-A"],
        ),
    ],
    ids=["or-branches", "single-comparison"],
)
async def test_fallback_search_matches_filter(
    app_client, two_collections, cql2_filter, expected_ids
):
    resp = await app_client.post(
        "/search", json={"filter-lang": "cql2-json", "filter": cql2_filter}
    )

    assert resp.status_code == 200, resp.text
    assert [f["id"] for f in resp.json()["features"]] == expected_ids
