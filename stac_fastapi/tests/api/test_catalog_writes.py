"""Catalog create-only and missing-resource regressions."""

import uuid

import pytest


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
async def test_unlink_sub_catalog_returns_204_and_keeps_child(
    catalogs_app_client, load_test_data
):
    parent = await _create_catalog(catalogs_app_client, load_test_data, "parent")
    child = await _create_catalog(catalogs_app_client, load_test_data, "child")
    response = await catalogs_app_client.post(
        f"/catalogs/{parent['id']}/catalogs", json={"id": child["id"]}
    )
    assert response.status_code == 200

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}/catalogs/{child['id']}"
    )

    assert response.status_code == 204
    assert (
        await catalogs_app_client.get(f"/catalogs/{child['id']}")
    ).status_code == 200


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
