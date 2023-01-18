import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from random import randint
from urllib.parse import parse_qs, urlparse, urlsplit

import ciso8601
import pystac
import pytest
from geojson_pydantic.geometries import Polygon
from pystac.utils import datetime_to_str

from stac_fastapi.elasticsearch.core import CoreClient
from stac_fastapi.elasticsearch.datetime_utils import now_to_rfc3339_str
from stac_fastapi.types.core import LandingPageMixin

from ..conftest import create_item, refresh_indices


def rfc3339_str_to_datetime(s: str) -> datetime:
    return ciso8601.parse_rfc3339(s)


async def test_create_and_delete_item(app_client, ctx, txn_client):
    """Test creation and deletion of a single item (transactions extension)"""

    test_item = ctx.item

    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200

    resp = await app_client.delete(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 204

    await refresh_indices(txn_client)

    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 404


async def test_create_item_conflict(app_client, ctx):
    """Test creation of an item which already exists (transactions extension)"""

    test_item = ctx.item

    resp = await app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 409


async def test_delete_missing_item(app_client, load_test_data):
    """Test deletion of an item which does not exist (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = await app_client.delete(
        f"/collections/{test_item['collection']}/items/hijosh"
    )
    assert resp.status_code == 404


async def test_create_item_missing_collection(app_client, ctx):
    """Test creation of an item without a parent collection (transactions extension)"""
    ctx.item["collection"] = "stac_is_cool"
    resp = await app_client.post(
        f"/collections/{ctx.item['collection']}/items", json=ctx.item
    )
    assert resp.status_code == 404


async def test_update_item_already_exists(app_client, ctx):
    """Test updating an item which already exists (transactions extension)"""

    assert ctx.item["properties"]["gsd"] != 16
    ctx.item["properties"]["gsd"] = 16
    await app_client.put(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}", json=ctx.item
    )
    resp = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )
    updated_item = resp.json()
    assert updated_item["properties"]["gsd"] == 16

    await app_client.delete(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )


async def test_update_new_item(app_client, ctx):
    """Test updating an item which does not exist (transactions extension)"""
    test_item = ctx.item
    test_item["id"] = "a"

    resp = await app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 404


async def test_update_item_missing_collection(app_client, ctx):
    """Test updating an item without a parent collection (transactions extension)"""
    # Try to update collection of the item
    ctx.item["collection"] = "stac_is_cool"
    resp = await app_client.put(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}", json=ctx.item
    )
    assert resp.status_code == 404


async def test_update_item_geometry(app_client, ctx):
    ctx.item["id"] = "update_test_item_1"

    # Create the item
    resp = await app_client.post(
        f"/collections/{ctx.item['collection']}/items", json=ctx.item
    )
    assert resp.status_code == 200

    new_coordinates = [
        [
            [142.15052873427666, -33.82243006904891],
            [140.1000346138806, -34.257132625788756],
            [139.5776607193635, -32.514709769700254],
            [141.6262528041627, -32.08081674221862],
            [142.15052873427666, -33.82243006904891],
        ]
    ]

    # Update the geometry of the item
    ctx.item["geometry"]["coordinates"] = new_coordinates
    resp = await app_client.put(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}", json=ctx.item
    )
    assert resp.status_code == 200

    # Fetch the updated item
    resp = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )
    assert resp.status_code == 200
    assert resp.json()["geometry"]["coordinates"] == new_coordinates


async def test_get_item(app_client, ctx):
    """Test read an item by id (core)"""
    get_item = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )
    assert get_item.status_code == 200


async def test_returns_valid_item(app_client, ctx):
    """Test validates fetched item with jsonschema"""
    test_item = ctx.item
    get_item = await app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert get_item.status_code == 200
    item_dict = get_item.json()
    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    item = pystac.Item.from_dict(item_dict, preserve_dict=False, root=mock_root)
    item.validate()


async def test_get_item_collection(app_client, ctx, txn_client):
    """Test read an item collection (core)"""
    item_count = randint(1, 4)

    for idx in range(item_count):
        ctx.item["id"] = f'{ctx.item["id"]}{idx}'
        await create_item(txn_client, ctx.item)

    resp = await app_client.get(f"/collections/{ctx.item['collection']}/items")
    assert resp.status_code == 200

    item_collection = resp.json()
    if matched := item_collection["context"].get("matched"):
        assert matched == item_count + 1


async def test_item_collection_filter_bbox(app_client, ctx):
    item = ctx.item
    collection = item["collection"]

    bbox = "100,-50,170,-20"
    resp = await app_client.get(
        f"/collections/{collection}/items", params={"bbox": bbox}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1

    bbox = "1,2,3,4"
    resp = await app_client.get(
        f"/collections/{collection}/items", params={"bbox": bbox}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


async def test_item_collection_filter_datetime(app_client, ctx):
    item = ctx.item
    collection = item["collection"]

    datetime_range = "2020-01-01T00:00:00.00Z/.."
    resp = await app_client.get(
        f"/collections/{collection}/items", params={"datetime": datetime_range}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1

    datetime_range = "2018-01-01T00:00:00.00Z/2019-01-01T00:00:00.00Z"
    resp = await app_client.get(
        f"/collections/{collection}/items", params={"datetime": datetime_range}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


@pytest.mark.skip(reason="Pagination extension not implemented")
async def test_pagination(app_client, load_test_data):
    """Test item collection pagination (paging extension)"""
    item_count = 10
    test_item = load_test_data("test_item.json")

    for idx in range(item_count):
        _test_item = deepcopy(test_item)
        _test_item["id"] = test_item["id"] + str(idx)
        resp = await app_client.post(
            f"/collections/{test_item['collection']}/items", json=_test_item
        )
        assert resp.status_code == 200

    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items", params={"limit": 3}
    )
    assert resp.status_code == 200
    first_page = resp.json()
    assert first_page["context"]["returned"] == 3

    url_components = urlsplit(first_page["links"][0]["href"])
    resp = await app_client.get(f"{url_components.path}?{url_components.query}")
    assert resp.status_code == 200
    second_page = resp.json()
    assert second_page["context"]["returned"] == 3


async def test_item_timestamps(app_client, ctx, load_test_data):
    """Test created and updated timestamps (common metadata)"""
    # start_time = now_to_rfc3339_str()

    created_dt = ctx.item["properties"]["created"]

    # todo, check lower bound
    # assert start_time < created_dt < now_to_rfc3339_str()
    assert created_dt < now_to_rfc3339_str()

    # Confirm `updated` timestamp
    ctx.item["properties"]["proj:epsg"] = 4326
    resp = await app_client.put(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        json=dict(ctx.item),
    )
    assert resp.status_code == 200
    updated_item = resp.json()

    # Created shouldn't change on update
    assert ctx.item["properties"]["created"] == updated_item["properties"]["created"]
    assert updated_item["properties"]["updated"] > created_dt

    await app_client.delete(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )


async def test_item_search_by_id_post(app_client, ctx, txn_client):
    """Test POST search by item id (core)"""
    ids = ["test1", "test2", "test3"]
    for _id in ids:
        ctx.item["id"] = _id
        await create_item(txn_client, ctx.item)

    params = {"collections": [ctx.item["collection"]], "ids": ids}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert set([feat["id"] for feat in resp_json["features"]]) == set(ids)


async def test_item_search_spatial_query_post(app_client, ctx):
    """Test POST search with spatial query (core)"""
    test_item = ctx.item

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_query_post(app_client, ctx):
    """Test POST search with single-tailed spatio-temporal query (core)"""

    test_item = ctx.item

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date = item_date + timedelta(seconds=1)

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": f"../{datetime_to_str(item_date)}",
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


async def test_item_search_temporal_window_post(app_client, load_test_data, ctx):
    """Test POST search with two-tailed spatio-temporal query (core)"""
    test_item = ctx.item

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date_before = item_date - timedelta(seconds=1)
    item_date_after = item_date + timedelta(seconds=1)

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


@pytest.mark.skip(reason="KeyError: 'features")
async def test_item_search_temporal_open_window(app_client, ctx):
    """Test POST search with open spatio-temporal query (core)"""
    test_item = ctx.item
    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": "../..",
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


@pytest.mark.skip(reason="sortby date not implemented")
async def test_item_search_sort_post(app_client, load_test_data):
    """Test POST search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    resp = await app_client.post(
        f"/collections/{first_item['collection']}/items", json=first_item
    )
    assert resp.status_code == 200

    second_item = load_test_data("test_item.json")
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = datetime_to_str(another_item_date)
    resp = await app_client.post(
        f"/collections/{second_item['collection']}/items", json=second_item
    )
    assert resp.status_code == 200

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "datetime", "direction": "desc"}],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]
    await app_client.delete(
        f"/collections/{first_item['collection']}/items/{first_item['id']}"
    )


async def test_item_search_by_id_get(app_client, ctx, txn_client):
    """Test GET search by item id (core)"""
    ids = ["test1", "test2", "test3"]
    for _id in ids:
        ctx.item["id"] = _id
        await create_item(txn_client, ctx.item)

    params = {"collections": ctx.item["collection"], "ids": ",".join(ids)}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert set([feat["id"] for feat in resp_json["features"]]) == set(ids)


async def test_item_search_bbox_get(app_client, ctx):
    """Test GET search with spatial query (core)"""
    params = {
        "collections": ctx.item["collection"],
        "bbox": ",".join([str(coord) for coord in ctx.item["bbox"]]),
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == ctx.item["id"]


async def test_item_search_get_without_collections(app_client, ctx):
    """Test GET search without specifying collections"""

    params = {
        "bbox": ",".join([str(coord) for coord in ctx.item["bbox"]]),
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200


async def test_item_search_get_with_non_existent_collections(app_client, ctx):
    """Test GET search with non-existent collections"""

    params = {"collections": "non-existent-collection,or-this-one"}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200


async def test_item_search_temporal_window_get(app_client, ctx):
    """Test GET search with spatio-temporal query (core)"""
    test_item = ctx.item
    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date_before = item_date - timedelta(seconds=1)
    item_date_after = item_date + timedelta(seconds=1)

    params = {
        "collections": test_item["collection"],
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


@pytest.mark.skip(reason="sorting not fully implemented")
async def test_item_search_sort_get(app_client, ctx, txn_client):
    """Test GET search with sorting (sort extension)"""
    first_item = ctx.item
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    await create_item(txn_client, ctx.item)

    second_item = ctx.item.copy()
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item.update({"properties": {"datetime": datetime_to_str(another_item_date)}})
    await create_item(txn_client, second_item)

    params = {"collections": [first_item["collection"]], "sortby": "-datetime"}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


async def test_item_search_post_without_collection(app_client, ctx):
    """Test POST search without specifying a collection"""
    test_item = ctx.item
    params = {
        "bbox": test_item["bbox"],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200


async def test_item_search_properties_es(app_client, ctx):
    """Test POST search with JSONB query (query extension)"""

    test_item = ctx.item
    # EPSG is a JSONB key
    params = {"query": {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] + 1}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


async def test_item_search_properties_field(app_client):
    """Test POST search indexed field with query (query extension)"""

    # Orientation is an indexed field
    params = {"query": {"orientation": {"eq": "south"}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


async def test_item_search_get_query_extension(app_client, ctx):
    """Test GET search with JSONB query (query extension)"""

    test_item = ctx.item

    params = {
        "collections": [test_item["collection"]],
        "query": json.dumps(
            {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] + 1}}
        ),
    }
    resp = await app_client.get("/search", params=params)
    assert resp.json()["context"]["returned"] == 0

    params["query"] = json.dumps(
        {"proj:epsg": {"eq": test_item["properties"]["proj:epsg"]}}
    )
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )


async def test_get_missing_item_collection(app_client):
    """Test reading a collection which does not exist"""
    resp = await app_client.get("/collections/invalid-collection/items")
    assert resp.status_code == 404


async def test_pagination_item_collection(app_client, ctx, txn_client):
    """Test item collection pagination links (paging extension)"""
    ids = [ctx.item["id"]]

    # Ingest 5 items
    for _ in range(5):
        ctx.item["id"] = str(uuid.uuid4())
        await create_item(txn_client, item=ctx.item)
        ids.append(ctx.item["id"])

    # Paginate through all 6 items with a limit of 1 (expecting 7 requests)
    page = await app_client.get(
        f"/collections/{ctx.item['collection']}/items", params={"limit": 1}
    )

    item_ids = []
    idx = 0
    for idx in range(100):
        page_data = page.json()
        next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))
        if not next_link:
            assert not page_data["features"]
            break

        assert len(page_data["features"]) == 1
        item_ids.append(page_data["features"][0]["id"])

        href = next_link[0]["href"][len("http://test-server") :]
        page = await app_client.get(href)

    assert idx == len(ids)

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


async def test_pagination_post(app_client, ctx, txn_client):
    """Test POST pagination (paging extension)"""
    ids = [ctx.item["id"]]

    # Ingest 5 items
    for _ in range(5):
        ctx.item["id"] = str(uuid.uuid4())
    await create_item(txn_client, ctx.item)
    ids.append(ctx.item["id"])

    # Paginate through all 5 items with a limit of 1 (expecting 5 requests)
    request_body = {"ids": ids, "limit": 1}
    page = await app_client.post("/search", json=request_body)
    idx = 0
    item_ids = []
    for _ in range(100):
        idx += 1
        page_data = page.json()
        next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))
        if not next_link:
            break

        item_ids.append(page_data["features"][0]["id"])

        # Merge request bodies
        request_body.update(next_link[0]["body"])
        page = await app_client.post("/search", json=request_body)

    # Our limit is 1, so we expect len(ids) number of requests before we run out of pages
    assert idx == len(ids) + 1

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


async def test_pagination_token_idempotent(app_client, ctx, txn_client):
    """Test that pagination tokens are idempotent (paging extension)"""
    ids = [ctx.item["id"]]

    # Ingest 5 items
    for _ in range(5):
        ctx.item["id"] = str(uuid.uuid4())
    await create_item(txn_client, ctx.item)
    ids.append(ctx.item["id"])

    page = await app_client.get("/search", params={"ids": ",".join(ids), "limit": 3})
    page_data = page.json()
    next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))

    # Confirm token is idempotent
    resp1 = await app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp2 = await app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp1_data = resp1.json()
    resp2_data = resp2.json()

    # Two different requests with the same pagination token should return the same items
    assert [item["id"] for item in resp1_data["features"]] == [
        item["id"] for item in resp2_data["features"]
    ]


async def test_field_extension_get_includes(app_client):
    """Test GET search with included fields (fields extension)"""
    params = {"fields": "+properties.proj:epsg,+properties.gsd"}
    resp = await app_client.get("/search", params=params)
    feat_properties = resp.json()["features"][0]["properties"]
    assert not set(feat_properties) - {"proj:epsg", "gsd", "datetime"}


async def test_field_extension_get_excludes(app_client):
    """Test GET search with included fields (fields extension)"""
    params = {"fields": "-properties.proj:epsg,-properties.gsd"}
    resp = await app_client.get("/search", params=params)
    resp_json = resp.json()
    assert "proj:epsg" not in resp_json["features"][0]["properties"].keys()
    assert "gsd" not in resp_json["features"][0]["properties"].keys()


async def test_field_extension_post(app_client):
    """Test POST search with included and excluded fields (fields extension)"""
    body = {
        "fields": {
            "exclude": ["assets.B1"],
            "include": ["properties.eo:cloud_cover", "properties.orientation"],
        }
    }

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "B1" not in resp_json["features"][0]["assets"].keys()
    assert not set(resp_json["features"][0]["properties"]) - {
        "orientation",
        "eo:cloud_cover",
        "datetime",
    }


async def test_field_extension_exclude_and_include(app_client, load_test_data):
    """Test POST search including/excluding same field (fields extension)"""
    body = {
        "fields": {
            "exclude": ["properties.eo:cloud_cover"],
            "include": ["properties.eo:cloud_cover"],
        }
    }

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "eo:cloud_cover" not in resp_json["features"][0]["properties"]


async def test_field_extension_exclude_default_includes(app_client, load_test_data):
    """Test POST search excluding a forbidden field (fields extension)"""
    body = {"fields": {"exclude": ["gsd"]}}

    resp = await app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "gsd" not in resp_json["features"][0]


async def test_search_intersects_and_bbox(app_client):
    """Test POST search intersects and bbox are mutually exclusive (core)"""
    bbox = [-118, 34, -117, 35]
    geoj = Polygon.from_bounds(*bbox).dict(exclude_none=True)
    params = {"bbox": bbox, "intersects": geoj}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 400


async def test_get_missing_item(app_client, load_test_data):
    """Test read item which does not exist (transactions extension)"""
    test_coll = load_test_data("test_collection.json")
    resp = await app_client.get(f"/collections/{test_coll['id']}/items/invalid-item")
    assert resp.status_code == 404


@pytest.mark.skip(reason="invalid queries not implemented")
async def test_search_invalid_query_field(app_client):
    body = {"query": {"gsd": {"lt": 100}, "invalid-field": {"eq": 50}}}
    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 400


async def test_search_bbox_errors(app_client):
    body = {"query": {"bbox": [0]}}
    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 400

    body = {"query": {"bbox": [100.0, 0.0, 0.0, 105.0, 1.0, 1.0]}}
    resp = await app_client.post("/search", json=body)
    assert resp.status_code == 400

    params = {"bbox": "100.0,0.0,0.0,105.0"}
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 400


async def test_conformance_classes_configurable():
    """Test conformance class configurability"""
    landing = LandingPageMixin()
    landing_page = landing._landing_page(
        base_url="http://test/test",
        conformance_classes=["this is a test"],
        extension_schemas=[],
    )
    assert landing_page["conformsTo"][0] == "this is a test"

    # Update environment to avoid key error on client instantiation
    os.environ["READER_CONN_STRING"] = "testing"
    os.environ["WRITER_CONN_STRING"] = "testing"
    client = CoreClient(base_conformance_classes=["this is a test"])
    assert client.conformance_classes()[0] == "this is a test"


async def test_search_datetime_validation_errors(app_client):
    bad_datetimes = [
        "37-01-01T12:00:27.87Z",
        "1985-13-12T23:20:50.52Z",
        "1985-12-32T23:20:50.52Z",
        "1985-12-01T25:20:50.52Z",
        "1985-12-01T00:60:50.52Z",
        "1985-12-01T00:06:61.52Z",
        "1990-12-31T23:59:61Z",
        "1986-04-12T23:20:50.52Z/1985-04-12T23:20:50.52Z",
    ]
    for dt in bad_datetimes:
        body = {"query": {"datetime": dt}}
        resp = await app_client.post("/search", json=body)
        assert resp.status_code == 400

        resp = await app_client.get("/search?datetime={}".format(dt))
        assert resp.status_code == 400
