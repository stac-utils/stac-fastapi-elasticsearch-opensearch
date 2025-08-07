import os
import random
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from stac_fastapi.types.errors import ConflictError

from ..conftest import create_collection, create_item

ROUTES = {
    "GET /_mgmt/ping",
    "GET /_mgmt/health",
    "GET /docs/oauth2-redirect",
    "HEAD /docs/oauth2-redirect",
    "GET /",
    "GET /conformance",
    "GET /api",
    "GET /api.html",
    "HEAD /api",
    "HEAD /api.html",
    "GET /queryables",
    "GET /collections",
    "GET /collections/{collection_id}",
    "GET /collections/{collection_id}/queryables",
    "GET /collections/{collection_id}/items",
    "POST /collections/{collection_id}/bulk_items",
    "GET /collections/{collection_id}/items/{item_id}",
    "GET /search",
    "POST /search",
    "DELETE /collections/{collection_id}",
    "DELETE /collections/{collection_id}/items/{item_id}",
    "POST /collections",
    "POST /collections/{collection_id}/items",
    "PUT /collections/{collection_id}",
    "PATCH /collections/{collection_id}",
    "PUT /collections/{collection_id}/items/{item_id}",
    "PATCH /collections/{collection_id}/items/{item_id}",
    "POST /collections/{collection_id}/bulk_items",
    "GET /aggregations",
    "GET /aggregate",
    "POST /aggregations",
    "POST /aggregate",
    "GET /collections/{collection_id}/aggregations",
    "GET /collections/{collection_id}/aggregate",
    "POST /collections/{collection_id}/aggregations",
    "POST /collections/{collection_id}/aggregate",
}


@pytest.mark.asyncio
async def test_post_search_content_type(app_client, ctx):
    params = {"limit": 1}
    resp = await app_client.post("/search", json=params)
    assert resp.headers["Content-Type"] == "application/geo+json"


@pytest.mark.asyncio
async def test_get_search_content_type(app_client, ctx):
    resp = await app_client.get("/search")
    assert resp.headers["Content-Type"] == "application/geo+json"


@pytest.mark.asyncio
async def test_api_headers(app_client):
    resp = await app_client.get("/api")
    assert (
        resp.headers["Content-Type"] == "application/vnd.oai.openapi+json;version=3.0"
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_router(app):
    api_routes = set([f"{list(route.methods)[0]} {route.path}" for route in app.routes])
    assert len(api_routes - ROUTES) == 0


@pytest.mark.asyncio
async def test_app_transaction_extension(app_client, ctx, load_test_data):
    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    resp = await app_client.post(f"/collections/{item['collection']}/items", json=item)
    assert resp.status_code == 201

    await app_client.delete(f"/collections/{item['collection']}/items/{item['id']}")


@pytest.mark.asyncio
async def test_app_search_response(app_client, ctx):
    resp = await app_client.get("/search", params={"ids": ["test-item"]})
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    # stac_version and stac_extensions were removed in v1.0.0-beta.3
    assert resp_json.get("stac_version") is None
    assert resp_json.get("stac_extensions") is None


@pytest.mark.asyncio
async def test_app_context_results(app_client, txn_client, ctx, load_test_data):
    test_item = load_test_data("test_item.json")
    test_item["id"] = "test-item-2"
    test_item["collection"] = "test-collection-2"
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test-collection-2"

    await create_collection(txn_client, test_collection)
    await create_item(txn_client, test_item)

    resp = await app_client.get(
        f"/collections/{test_collection['id']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_item["id"]
    assert resp_json["collection"] == test_item["collection"]

    resp = await app_client.get(f"/collections/{test_collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_collection["id"]

    resp = await app_client.post("/search", json={"collections": ["test-collection-2"]})
    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert resp_json["numReturned"] == 1
    if matched := resp_json.get("numMatched"):
        assert matched == 1


@pytest.mark.asyncio
async def test_app_fields_extension(app_client, ctx, txn_client):
    resp = await app_client.get(
        "/search",
        params={"collections": ["test-collection"], "fields": "+properties.datetime"},
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert list(resp_json["features"][0]["properties"]) == ["datetime"]


@pytest.mark.asyncio
async def test_app_fields_extension_query(app_client, ctx, txn_client):
    item = ctx.item
    resp = await app_client.post(
        "/search",
        json={
            "query": {"proj:epsg": {"gte": item["properties"]["proj:epsg"]}},
            "collections": ["test-collection"],
            "fields": {"include": ["properties.datetime", "properties.proj:epsg"]},
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert set(resp_json["features"][0]["properties"]) == set(["datetime", "proj:epsg"])


@pytest.mark.asyncio
async def test_app_fields_extension_no_properties_get(app_client, ctx, txn_client):
    resp = await app_client.get(
        "/search", params={"collections": ["test-collection"], "fields": "-properties"}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "properties" not in resp_json["features"][0]


@pytest.mark.asyncio
async def test_app_fields_extension_no_properties_post(app_client, ctx, txn_client):
    resp = await app_client.post(
        "/search",
        json={
            "collections": ["test-collection"],
            "fields": {"exclude": ["properties"]},
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "properties" not in resp_json["features"][0]


@pytest.mark.asyncio
async def test_app_fields_extension_no_null_fields(app_client, ctx, txn_client):
    resp = await app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    # check if no null fields: https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/166
    for feature in resp_json["features"]:
        # assert "bbox" not in feature["geometry"]
        for link in feature["links"]:
            assert all(a not in link or link[a] is not None for a in ("title", "asset"))
        for asset in feature["assets"]:
            assert all(
                a not in asset or asset[a] is not None
                for a in ("start_datetime", "created")
            )


@pytest.mark.asyncio
async def test_app_fields_extension_return_all_properties(
    app_client, ctx, txn_client, load_test_data
):
    item = load_test_data("test_item.json")
    resp = await app_client.get(
        "/search", params={"collections": ["test-collection"], "fields": "properties"}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    feature = resp_json["features"][0]
    assert len(feature["properties"]) >= len(item["properties"])
    for expected_prop, expected_value in item["properties"].items():
        if expected_prop in (
            "datetime",
            "start_datetime",
            "end_datetime",
            "created",
            "updated",
        ):
            assert feature["properties"][expected_prop][0:19] == expected_value[0:19]
        else:
            assert feature["properties"][expected_prop] == expected_value


@pytest.mark.asyncio
async def test_app_query_extension_gt(app_client, ctx):
    params = {"query": {"proj:epsg": {"gt": ctx.item["properties"]["proj:epsg"]}}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


@pytest.mark.asyncio
async def test_app_query_extension_gte(app_client, ctx):
    params = {"query": {"proj:epsg": {"gte": ctx.item["properties"]["proj:epsg"]}}}
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_app_query_extension_limit_lt0(app_client):
    assert (
        await app_client.post("/search", json={"query": {}, "limit": -1})
    ).status_code == 400


@pytest.mark.skip(reason="removal of context extension")
@pytest.mark.asyncio
async def test_app_query_extension_limit_gt10000(app_client):
    resp = await app_client.post("/search", json={"limit": 10001})
    assert resp.status_code == 200
    assert resp.json()["context"]["limit"] == 10000


@pytest.mark.asyncio
async def test_app_query_extension_limit_10000(app_client):
    params = {"limit": 10000}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_app_sort_extension_get_asc(app_client, txn_client, ctx):
    first_item = ctx.item

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    await create_item(txn_client, second_item)

    resp = await app_client.get("/search?sortby=+properties.datetime")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][1]["id"] == first_item["id"]
    assert resp_json["features"][0]["id"] == second_item["id"]


@pytest.mark.asyncio
async def test_app_sort_extension_get_desc(app_client, txn_client, ctx):
    first_item = ctx.item

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    await create_item(txn_client, second_item)

    resp = await app_client.get("/search?sortby=-properties.datetime")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


@pytest.mark.asyncio
async def test_app_sort_extension_post_asc(app_client, txn_client, ctx):
    first_item = ctx.item

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    await create_item(txn_client, second_item)

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "properties.datetime", "direction": "asc"}],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][1]["id"] == first_item["id"]
    assert resp_json["features"][0]["id"] == second_item["id"]


@pytest.mark.asyncio
async def test_app_sort_extension_post_desc(app_client, txn_client, ctx):
    first_item = ctx.item

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    await create_item(txn_client, second_item)

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


@pytest.mark.asyncio
async def test_search_invalid_date(app_client, ctx):
    params = {
        "datetime": "2020-XX-01/2020-10-30",
        "collections": [ctx.item["collection"]],
    }

    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_search_point_intersects_get(app_client, ctx):
    resp = await app_client.get(
        '/search?intersects={"type":"Point","coordinates":[150.04,-33.14]}'
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_polygon_intersects_get(app_client, ctx):
    resp = await app_client.get(
        '/search?intersects={"type":"Polygon","coordinates":[[[149.04, -34.14],[149.04, -32.14],[151.04, -32.14],[151.04, -34.14],[149.04, -34.14]]]}'
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_point_intersects_post(app_client, ctx):
    point = [150.04, -33.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_point_does_not_intersect(app_client, ctx):
    point = [15.04, -3.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


@pytest.mark.asyncio
async def test_datetime_response_format(app_client, txn_client, ctx):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    first_item = dict(ctx.item)

    second_item = deepcopy(first_item)
    second_item["id"] = "second-item"
    second_item["properties"]["datetime"] = None

    await create_item(txn_client, second_item)

    third_item = deepcopy(first_item)
    third_item["id"] = "third-item"
    del third_item["properties"]["start_datetime"]
    del third_item["properties"]["end_datetime"]

    await create_item(txn_client, third_item)

    dt_formats = [
        "2020-02-12T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z",
        "2020-02-12T12:30:22Z",
        "2020-02-12T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        # datetime is returned in this format "2020-02-12T12:30:22Z"
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == dt[0:19]


@pytest.mark.asyncio
async def test_datetime_non_interval(app_client, txn_client, ctx):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    first_item = dict(ctx.item)

    second_item = deepcopy(first_item)
    second_item["id"] = "second-item"
    second_item["properties"]["datetime"] = None

    await create_item(txn_client, second_item)

    third_item = deepcopy(first_item)
    third_item["id"] = "third-item"
    del third_item["properties"]["start_datetime"]
    del third_item["properties"]["end_datetime"]

    await create_item(txn_client, third_item)

    dt_formats = [
        "2020-02-12T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z",
        "2020-02-12T12:30:22Z",
        "2020-02-12T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        assert len(resp_json["features"]) == 3


@pytest.mark.asyncio
async def test_datetime_interval(app_client, txn_client, ctx):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    first_item = dict(ctx.item)

    second_item = deepcopy(first_item)
    second_item["id"] = "second-item"
    second_item["properties"]["datetime"] = None

    await create_item(txn_client, second_item)

    third_item = deepcopy(first_item)
    third_item["id"] = "third-item"
    del third_item["properties"]["start_datetime"]
    del third_item["properties"]["end_datetime"]

    await create_item(txn_client, third_item)

    dt_formats = [
        "2020-02-06T12:30:22+00:00/2020-02-13T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z/2020-02-20T12:30:22.00Z",
        "2020-02-12T12:30:22Z/2020-02-13T12:30:22Z",
        "2020-02-06T12:30:22.00+00:00/2020-02-20T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        assert len(resp_json["features"]) == 3


@pytest.mark.asyncio
async def test_datetime_bad_non_interval(app_client, txn_client, ctx):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    first_item = dict(ctx.item)

    second_item = deepcopy(first_item)
    second_item["id"] = "second-item"
    second_item["properties"]["datetime"] = None

    await create_item(txn_client, second_item)

    third_item = deepcopy(first_item)
    third_item["id"] = "third-item"
    del third_item["properties"]["start_datetime"]
    del third_item["properties"]["end_datetime"]

    await create_item(txn_client, third_item)

    dt_formats = [
        "2020-02-06T12:30:22+00:00",
        "2020-02-06T12:30:22.00Z",
        "2020-02-06T12:30:22Z",
        "2020-02-06T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        assert len(resp_json["features"]) == 0


@pytest.mark.asyncio
async def test_datetime_bad_interval(app_client, txn_client, ctx):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    first_item = dict(ctx.item)

    second_item = deepcopy(first_item)
    second_item["id"] = "second-item"
    second_item["properties"]["datetime"] = None

    await create_item(txn_client, second_item)

    third_item = deepcopy(first_item)
    third_item["id"] = "third-item"
    del third_item["properties"]["start_datetime"]
    del third_item["properties"]["end_datetime"]

    await create_item(txn_client, third_item)

    dt_formats = [
        "1920-02-04T12:30:22+00:00/1920-02-06T12:30:22+00:00",
        "1920-02-04T12:30:22.00Z/1920-02-06T12:30:22.00Z",
        "1920-02-04T12:30:22Z/1920-02-06T12:30:22Z",
        "1920-02-04T12:30:22.00+00:00/1920-02-06T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": dt,
            "collections": [ctx.item["collection"]],
        }

        resp = await app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        assert len(resp_json["features"]) == 0


@pytest.mark.asyncio
async def test_bbox_3d(app_client, ctx):
    australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
    params = {
        "bbox": australia_bbox,
        "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_patch_json_collection(app_client, ctx):
    data = {
        "summaries": {"hello": "world", "gsd": [50], "instruments": None},
    }

    resp = await app_client.patch(f"/collections/{ctx.collection['id']}", json=data)

    assert resp.status_code == 200

    new_resp = await app_client.get(f"/collections/{ctx.collection['id']}")

    assert new_resp.status_code == 200

    new_resp_json = new_resp.json()

    assert new_resp_json["summaries"]["hello"] == "world"
    assert "instruments" not in new_resp_json["summaries"]
    assert new_resp_json["summaries"]["gsd"] == [50]
    assert new_resp_json["summaries"]["platform"] == ["landsat-8"]


@pytest.mark.asyncio
async def test_patch_operations_collection(app_client, ctx):
    operations = [
        {"op": "add", "path": "/summaries/hello", "value": "world"},
        {"op": "replace", "path": "/summaries/gsd", "value": [50]},
        {
            "op": "move",
            "path": "/summaries/instrument",
            "from": "/summaries/instruments",
        },
        {"op": "copy", "from": "/license", "path": "/summaries/license"},
    ]

    resp = await app_client.patch(
        f"/collections/{ctx.collection['id']}",
        json=operations,
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert resp.status_code == 200

    new_resp = await app_client.get(f"/collections/{ctx.collection['id']}")

    assert new_resp.status_code == 200

    new_resp_json = new_resp.json()

    assert new_resp_json["summaries"]["hello"] == "world"
    assert new_resp_json["summaries"]["gsd"] == [50]
    assert "instruments" not in new_resp_json["summaries"]
    assert (
        new_resp_json["summaries"]["instrument"]
        == ctx.collection["summaries"]["instruments"]
    )
    assert new_resp_json["license"] == ctx.collection["license"]
    assert new_resp_json["summaries"]["license"] == ctx.collection["license"]


@pytest.mark.asyncio
async def test_patch_json_item(app_client, ctx):

    data = {
        "properties": {"hello": "world", "proj:epsg": 1000, "landsat:column": None},
    }

    resp = await app_client.patch(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}", json=data
    )

    assert resp.status_code == 200

    new_resp = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )

    assert new_resp.status_code == 200

    new_resp_json = new_resp.json()

    assert new_resp_json["properties"]["hello"] == "world"
    assert "landsat:column" not in new_resp_json["properties"]
    assert new_resp_json["properties"]["proj:epsg"] == 1000
    assert new_resp_json["properties"]["platform"] == "landsat-8"


@pytest.mark.asyncio
async def test_patch_operations_item(app_client, ctx):
    operations = [
        {"op": "add", "path": "/properties/hello", "value": "world"},
        {"op": "remove", "path": "/properties/landsat:column"},
        {"op": "replace", "path": "/properties/proj:epsg", "value": 1000},
        {"op": "move", "path": "/properties/foo", "from": "/properties/instrument"},
        {"op": "copy", "path": "/properties/bar", "from": "/properties/height"},
    ]

    resp = await app_client.patch(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        json=operations,
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert resp.status_code == 200

    new_resp = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )

    assert new_resp.status_code == 200

    new_resp_json = new_resp.json()

    assert new_resp_json["properties"]["hello"] == "world"
    assert "landsat:column" not in new_resp_json["properties"]
    assert new_resp_json["properties"]["proj:epsg"] == 1000
    assert "instrument" not in new_resp_json["properties"]
    assert new_resp_json["properties"]["foo"] == ctx.item["properties"]["instrument"]
    assert new_resp_json["properties"]["bar"] == ctx.item["properties"]["height"]
    assert new_resp_json["properties"]["height"] == ctx.item["properties"]["height"]


@pytest.mark.asyncio
async def test_search_line_string_intersects(app_client, ctx):
    line = [[150.04, -33.14], [150.22, -33.89]]
    intersects = {"type": "LineString", "coordinates": line}
    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, expected",
    [
        (32767, 1),  # Short Limit
        (2147483647, 1),  # Int Limit
        (2147483647 + 5000, 1),  # Above Int Limit
        (21474836470, 1),  # Above Int Limit
    ],
)
async def test_big_int_eo_search(
    app_client, txn_client, test_item, test_collection, value, expected
):
    random_str = "".join(random.choice("abcdef") for _ in range(5))
    collection_id = f"test-collection-eo-{random_str}"

    test_collection["id"] = collection_id
    test_collection["stac_extensions"] = [
        "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
    ]

    test_item["collection"] = collection_id
    test_item["stac_extensions"] = test_collection["stac_extensions"]

    # Remove "eo:bands" to simplify the test
    del test_item["properties"]["eo:bands"]

    # Attribute to test
    attr = "eo:full_width_half_max"

    try:
        await create_collection(txn_client, test_collection)
    except ConflictError:
        pass

    # Create items with deterministic offsets
    for val in [value, value + 100, value - 100]:
        item = deepcopy(test_item)
        item["id"] = str(uuid.uuid4())
        item["properties"][attr] = val
        await create_item(txn_client, item)

    # Search for the exact value
    params = {
        "collections": [collection_id],
        "filter": {
            "args": [
                {
                    "args": [
                        {"property": f"properties.{attr}"},
                        value,
                    ],
                    "op": "=",
                }
            ],
            "op": "and",
        },
    }
    resp = await app_client.post("/search", json=params)
    resp_json = resp.json()

    # Validate results
    results = {x["properties"][attr] for x in resp_json["features"]}
    assert len(results) == expected
    assert results == {value}


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_in_past_date_change_alias_name_for_datetime_index(
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
        "items_test-collection_2012-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())

    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_uses_existing_datetime_index_for_datetime_index(
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
        "items_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_item_with_different_date_same_index_for_datetime_index(
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
        "items_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_create_new_index_when_size_limit_exceeded_for_datetime_index(
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
        "items_test-collection_2020-02-12-2024-02-12",
        "items_test-collection_2024-02-13",
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
async def test_create_item_fails_without_datetime_for_datetime_index(
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
async def test_bulk_create_items_with_same_date_range_for_datetime_index(
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
        "items_test-collection_2020-02-12",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    return all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_different_date_ranges_for_datetime_index(
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

    expected_aliases = ["items_test-collection_2010-02-10"]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_size_limit_exceeded_for_datetime_index(
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
        "items_test-collection_2010-02-10-2020-02-12",
        "items_test-collection_2020-02-13",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_with_early_date_in_second_batch_for_datetime_index(
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
        "items_test-collection_2008-01-15-2020-02-12",
        "items_test-collection_2020-02-13",
    ]
    all_aliases = set()
    for index_info in indices.values():
        all_aliases.update(index_info.get("aliases", {}).keys())
    assert all(alias in all_aliases for alias in expected_aliases)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_bulk_create_items_and_retrieve_by_id_for_datetime_index(
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
async def test_patch_collection_for_datetime_index(
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
async def test_put_collection_for_datetime_index(
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
async def test_patch_item_for_datetime_index(
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
async def test_put_item_for_datetime_index(app_client, load_test_data, txn_client, ctx):
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
