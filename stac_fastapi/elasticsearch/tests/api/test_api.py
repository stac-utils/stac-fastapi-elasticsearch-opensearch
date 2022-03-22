from datetime import datetime, timedelta

import pytest

from ..conftest import MockStarletteRequest, create_collection, create_item

STAC_CORE_ROUTES = [
    "GET /",
    "GET /collections",
    "GET /collections/{collectionId}",
    "GET /collections/{collectionId}/items",
    "GET /collections/{collectionId}/items/{itemId}",
    "GET /conformance",
    "GET /search",
    "POST /search",
]

STAC_TRANSACTION_ROUTES = [
    "DELETE /collections/{collectionId}",
    "DELETE /collections/{collectionId}/items/{itemId}",
    "POST /collections",
    "POST /collections/{collectionId}/items",
    "PUT /collections",
    "PUT /collections/{collectionId}/items",
]


@pytest.mark.skip(reason="fails ci only")
def test_post_search_content_type(app_client):
    params = {"limit": 1}
    resp = app_client.post("search", json=params)
    assert resp.headers["content-type"] == "application/geo+json"


@pytest.mark.skip(reason="fails ci only")
def test_get_search_content_type(app_client):
    resp = app_client.get("search")
    assert resp.headers["content-type"] == "application/geo+json"


def test_api_headers(app_client):
    resp = app_client.get("/api")
    assert (
        resp.headers["content-type"] == "application/vnd.oai.openapi+json;version=3.0"
    )
    assert resp.status_code == 200


@pytest.mark.skip(reason="not working")
def test_core_router(api_client):
    core_routes = set(STAC_CORE_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )
    assert not core_routes - api_routes


@pytest.mark.skip(reason="not working")
def test_transactions_router(api_client):
    transaction_routes = set(STAC_TRANSACTION_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )
    assert not transaction_routes - api_routes


@pytest.mark.skip(reason="unknown")
def test_app_transaction_extension(app_client, load_test_data, es_txn_client):
    item = load_test_data("test_item.json")
    resp = app_client.post(f"/collections/{item['collection']}/items", json=item)
    assert resp.status_code == 200

    es_txn_client.delete_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )


def test_app_search_response(app_client, ctx):
    resp = app_client.get("/search", params={"ids": ["test-item"]})
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    # stac_version and stac_extensions were removed in v1.0.0-beta.3
    assert resp_json.get("stac_version") is None
    assert resp_json.get("stac_extensions") is None


def test_app_context_extension(app_client, ctx, es_txn_client):
    test_item = ctx.item
    test_item["id"] = "test-item-2"
    test_item["collection"] = "test-collection-2"
    test_collection = ctx.collection
    test_collection["id"] = "test-collection-2"

    create_collection(es_txn_client, test_collection)
    create_item(es_txn_client, test_item)

    resp = app_client.get(
        f"/collections/{test_collection['id']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_item["id"]
    assert resp_json["collection"] == test_item["collection"]

    resp = app_client.get(f"/collections/{test_collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["id"] == test_collection["id"]

    resp = app_client.post("/search", json={"collections": ["test-collection-2"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
    assert "context" in resp_json
    assert resp_json["context"]["returned"] == resp_json["context"]["matched"] == 1


@pytest.mark.skip(reason="fields not implemented yet")
def test_app_fields_extension(load_test_data, app_client, es_txn_client):
    item = load_test_data("test_item.json")
    es_txn_client.create_item(item, request=MockStarletteRequest, refresh=True)

    resp = app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert list(resp_json["features"][0]["properties"]) == ["datetime"]

    es_txn_client.delete_item(
        item["id"], item["collection"], request=MockStarletteRequest
    )


def test_app_query_extension_gt(app_client, ctx):
    params = {"query": {"proj:epsg": {"gt": ctx.item["properties"]["proj:epsg"]}}}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


def test_app_query_extension_gte(app_client, ctx):
    params = {"query": {"proj:epsg": {"gte": ctx.item["properties"]["proj:epsg"]}}}
    resp = app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


def test_app_query_extension_limit_lt0(app_client, ctx):
    assert app_client.post("/search", json={"limit": -1}).status_code == 400


def test_app_query_extension_limit_gt10000(app_client, ctx):
    assert app_client.post("/search", json={"limit": 10001}).status_code == 400


def test_app_query_extension_limit_10000(app_client, ctx):
    params = {"limit": 10000}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200


def test_app_sort_extension(app_client, es_txn_client, ctx):
    first_item = ctx.item
    item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    )

    second_item = dict(first_item)
    second_item["id"] = "another-item"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    create_item(es_txn_client, second_item)

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


def test_search_invalid_date(app_client, ctx):
    params = {
        "datetime": "2020-XX-01/2020-10-30",
        "collections": [ctx.item["collection"]],
    }

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


def test_search_point_intersects(app_client, ctx):
    point = [150.04, -33.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }
    resp = app_client.post("/search", json=params)

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


def test_datetime_non_interval(app_client, ctx):
    dt_formats = [
        "2020-02-12T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z",
        "2020-02-12T12:30:22Z",
        "2020-02-12T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {
            "datetime": ctx.item["properties"]["datetime"],
            "collections": [ctx.item["collection"]],
        }

        resp = app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        # datetime is returned in this format "2020-02-12T12:30:22Z"
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == dt[0:19]


def test_bbox_3d(app_client, ctx):
    australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
    params = {
        "bbox": australia_bbox,
        "collections": [ctx.item["collection"]],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


def test_search_line_string_intersects(app_client, ctx):
    line = [[150.04, -33.14], [150.22, -33.89]]
    intersects = {"type": "LineString", "coordinates": line}
    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
    }

    resp = app_client.post("/search", json=params)

    assert resp.status_code == 200

    resp_json = resp.json()
    assert len(resp_json["features"]) == 1
