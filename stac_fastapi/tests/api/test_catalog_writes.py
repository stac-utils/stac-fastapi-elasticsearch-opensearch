"""Catalog create-only and missing-resource regressions."""

import asyncio
import uuid

import pytest

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX
from stac_fastapi.types.errors import ConflictError, NotFoundError

pytestmark = pytest.mark.datetime_filtering


async def _create_catalog(catalogs_app_client, load_test_data, prefix="catalog"):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"{prefix}-{uuid.uuid4()}"
    response = await catalogs_app_client.post("/catalogs", json=catalog)
    assert response.status_code == 201
    return catalog


@pytest.mark.asyncio
async def test_duplicate_catalog_post_returns_409_and_preserves_content(
    catalogs_app_client, load_test_data
):
    catalog = await _create_catalog(catalogs_app_client, load_test_data)
    duplicate = {**catalog, "description": "must not replace"}

    response = await catalogs_app_client.post("/catalogs", json=duplicate)

    assert response.status_code == 409
    fetched = await catalogs_app_client.get(f"/catalogs/{catalog['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["description"] == catalog["description"]


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_child", [False, True])
async def test_unlink_sub_catalog_returns_204_and_keeps_child(
    catalogs_app_client, load_test_data, existing_child
):
    parent = await _create_catalog(catalogs_app_client, load_test_data, "parent")
    child = load_test_data("test_catalog.json")
    child["id"] = f"child-{uuid.uuid4()}"
    if existing_child:
        child = await _create_catalog(catalogs_app_client, load_test_data, "child")
    response = await catalogs_app_client.post(
        f"/catalogs/{parent['id']}/catalogs",
        json={"id": child["id"]} if existing_child else child,
    )
    assert response.status_code == (200 if existing_child else 201)
    duplicate = await catalogs_app_client.post(
        f"/catalogs/{parent['id']}/catalogs",
        json={**child, "description": "must not replace"},
    )
    assert duplicate.status_code == 409

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}/catalogs/{child['id']}"
    )

    assert response.status_code == 204
    fetched = await catalogs_app_client.get(f"/catalogs/{child['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["description"] == child["description"]
    children = await catalogs_app_client.get(f"/catalogs/{parent['id']}/catalogs")
    assert child["id"] not in [c["id"] for c in children.json()["catalogs"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["conformance", "queryables"])
async def test_catalog_metadata_for_missing_catalog_returns_404(
    catalogs_app_client, endpoint
):
    response = await catalogs_app_client.get(
        f"/catalogs/missing-{uuid.uuid4()}/{endpoint}"
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_unlink_missing_parent_returns_404(catalogs_app_client, load_test_data):
    child = await _create_catalog(catalogs_app_client, load_test_data, "child")
    response = await catalogs_app_client.delete(
        f"/catalogs/missing-{uuid.uuid4()}/catalogs/{child['id']}"
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_catalog_create_has_one_winner(
    catalogs_app_client, load_test_data, txn_client
):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"concurrent-{uuid.uuid4()}"
    candidates = [{**catalog, "description": description} for description in ("a", "b")]
    responses = await asyncio.gather(
        *(catalogs_app_client.post("/catalogs", json=body) for body in candidates)
    )
    assert sorted(response.status_code for response in responses) == [201, 409]
    winner = candidates[
        next(i for i, r in enumerate(responses) if r.status_code == 201)
    ]
    stored = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=catalog["id"]
    )
    assert stored["_source"]["description"] == winner["description"]
    assert stored["_version"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,upsert", [("Catalog", False), ("Collection", False), ("Collection", True)]
)
async def test_backend_catalog_conflict_preserves_document(
    catalogs_app_client, txn_client, load_test_data, kind, upsert
):
    document = load_test_data(
        "test_catalog.json" if kind == "Catalog" else "test_collection.json"
    )
    document["id"] = f"collision-{uuid.uuid4()}"
    database = txn_client.database
    await database.client.index(
        index=COLLECTIONS_INDEX, id=document["id"], body=document, refresh=True
    )
    before = await database.client.get(index=COLLECTIONS_INDEX, id=document["id"])
    with pytest.raises(ConflictError):
        await database.create_catalog(
            {**document, "type": "Catalog", "description": "must not replace"},
            upsert=upsert,
        )
    after = await database.client.get(index=COLLECTIONS_INDEX, id=document["id"])
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.asyncio
async def test_catalog_delete_race_returns_not_found(
    catalogs_app_client, txn_client, load_test_data, monkeypatch
):
    catalog = await _create_catalog(catalogs_app_client, load_test_data)
    database = txn_client.database
    original_delete = database.client.delete

    async def delete_after_competing_delete(**kwargs):
        await original_delete(**kwargs)
        return await original_delete(**kwargs)

    monkeypatch.setattr(database.client, "delete", delete_after_competing_delete)
    with pytest.raises(NotFoundError, match=catalog["id"]):
        await database.delete_catalog(catalog["id"], refresh=True)
    assert (
        await catalogs_app_client.get(f"/catalogs/{catalog['id']}")
    ).status_code == 404
