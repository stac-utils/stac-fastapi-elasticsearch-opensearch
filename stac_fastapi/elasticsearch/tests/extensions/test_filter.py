import json
import os
from os import listdir
from os.path import isfile, join

import pytest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.asyncio
async def test_search_filters_post(app_client, ctx):

    filters = []
    pwd = f"{THIS_DIR}/cql2"
    for fn in [fn for f in listdir(pwd) if isfile(fn := join(pwd, f))]:
        with open(fn) as f:
            filters.append(json.loads(f.read()))

    for _filter in filters:
        resp = await app_client.post("/search", json={"filter": _filter})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_filter_extension_eq_get(app_client, ctx):
    resp = await app_client.get(
        '/search?filter-lang=cql2-json&filter={"op":"=","args":[{"property":"id"},"test-item"]}'
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_eq_post(app_client, ctx):
    params = {"filter": {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_gte_get(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get(
        '/search?filter-lang=cql2-json&filter={"op":"<=","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1

    resp = await app_client.get(
        '/search?filter-lang=cql2-json&filter={"op":">","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0


@pytest.mark.asyncio
async def test_search_filter_extension_gte_post(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    params = {
        "filter": {
            "op": "<=",
            "args": [
                {"property": "properties.proj:epsg"},
                ctx.item["properties"]["proj:epsg"],
            ],
        }
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1

    params = {
        "filter": {
            "op": ">",
            "args": [
                {"property": "properties.proj:epsg"},
                ctx.item["properties"]["proj:epsg"],
            ],
        }
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0


@pytest.mark.asyncio
async def test_search_filter_ext_and_get(app_client, ctx):
    resp = await app_client.get(
        '/search?filter-lang=cql2-json&filter={"op":"and","args":[{"op":"<=","args":[{"property":"properties.proj:epsg"},32756]},{"op":"=","args":[{"property":"id"},"test-item"]}]}'
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_ext_and_get_id(app_client, ctx):
    collection = ctx.item["collection"]
    id = ctx.item["id"]
    filter = f"id='{id}' AND collection='{collection}'"
    resp = await app_client.get(f"/search?&filter={filter}")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_ext_and_get_cql2text_id(app_client, ctx):
    collection = ctx.item["collection"]
    id = ctx.item["id"]
    filter = f"id='{id}' AND collection='{collection}'"
    resp = await app_client.get(f"/search?filter-lang=cql2-text&filter={filter}")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_ext_and_get_cql2text_cloud_cover(app_client, ctx):
    collection = ctx.item["collection"]
    cloud_cover = ctx.item["properties"]["eo:cloud_cover"]
    filter = f"cloud_cover={cloud_cover} AND collection='{collection}'"
    resp = await app_client.get(f"/search?filter-lang=cql2-text&filter={filter}")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_ext_and_get_cql2text_cloud_cover_no_results(
    app_client, ctx
):
    collection = ctx.item["collection"]
    cloud_cover = ctx.item["properties"]["eo:cloud_cover"] + 1
    filter = f"cloud_cover={cloud_cover} AND collection='{collection}'"
    resp = await app_client.get(f"/search?filter-lang=cql2-text&filter={filter}")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0


@pytest.mark.asyncio
async def test_search_filter_ext_and_post(app_client, ctx):
    params = {
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "<=",
                    "args": [
                        {"property": "properties.proj:epsg"},
                        ctx.item["properties"]["proj:epsg"],
                    ],
                },
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
            ],
        }
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_floats_get(app_client, ctx):
    resp = await app_client.get(
        """/search?filter-lang=cql2-json&filter={"op":"and","args":[{"op":"=","args":[{"property":"id"},"test-item"]},{"op":">","args":[{"property":"properties.view:sun_elevation"},"-37.30891534"]},{"op":"<","args":[{"property":"properties.view:sun_elevation"},"-37.30691534"]}]}"""
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1

    resp = await app_client.get(
        """/search?filter-lang=cql2-json&filter={"op":"and","args":[{"op":"=","args":[{"property":"id"},"test-item-7"]},{"op":">","args":[{"property":"properties.view:sun_elevation"},"-37.30891534"]},{"op":"<","args":[{"property":"properties.view:sun_elevation"},"-37.30691534"]}]}"""
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0

    resp = await app_client.get(
        """/search?filter-lang=cql2-json&filter={"op":"and","args":[{"op":"=","args":[{"property":"id"},"test-item"]},{"op":">","args":[{"property":"properties.view:sun_elevation"},"-37.30591534"]},{"op":"<","args":[{"property":"properties.view:sun_elevation"},"-37.30491534"]}]}"""
    )

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0


@pytest.mark.asyncio
async def test_search_filter_extension_floats_post(app_client, ctx):
    sun_elevation = ctx.item["properties"]["view:sun_elevation"]

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": ">",
                    "args": [
                        {"property": "properties.view:sun_elevation"},
                        sun_elevation - 0.01,
                    ],
                },
                {
                    "op": "<",
                    "args": [
                        {"property": "properties.view:sun_elevation"},
                        sun_elevation + 0.01,
                    ],
                },
            ],
        }
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_wildcard_cql2(app_client, ctx):
    single_char = ctx.item["id"][:-1] + "_"
    multi_char = ctx.item["id"][:-3] + "%"

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "like",
                    "args": [
                        {"property": "id"},
                        single_char,
                    ],
                },
                {
                    "op": "like",
                    "args": [
                        {"property": "id"},
                        multi_char,
                    ],
                },
            ],
        }
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_wildcard_es(app_client, ctx):
    single_char = ctx.item["id"][:-1] + "?"
    multi_char = ctx.item["id"][:-3] + "*"

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "like",
                    "args": [
                        {"property": "id"},
                        single_char,
                    ],
                },
                {
                    "op": "like",
                    "args": [
                        {"property": "id"},
                        multi_char,
                    ],
                },
            ],
        }
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_escape_chars(app_client, ctx):
    esc_chars = (
        ctx.item["properties"]["landsat:product_id"].replace("_", "\\_")[:-1] + "_"
    )

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "like",
                    "args": [
                        {"property": "properties.landsat:product_id"},
                        esc_chars,
                    ],
                },
            ],
        }
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_in(app_client, ctx):
    product_id = ctx.item["properties"]["landsat:product_id"]

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "in",
                    "args": [
                        {"property": "properties.landsat:product_id"},
                        [product_id],
                    ],
                },
            ],
        }
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_in_no_list(app_client, ctx):
    product_id = ctx.item["properties"]["landsat:product_id"]

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "in",
                    "args": [
                        {"property": "properties.landsat:product_id"},
                        product_id,
                    ],
                },
            ],
        }
    }

    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 400
    assert resp.json() == {
        "detail": f"Error with cql2_json filter: Arg {product_id} is not a list"
    }


@pytest.mark.asyncio
async def test_search_filter_extension_between(app_client, ctx):
    sun_elevation = ctx.item["properties"]["view:sun_elevation"]

    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
                {
                    "op": "between",
                    "args": [
                        {"property": "properties.view:sun_elevation"},
                        sun_elevation - 0.01,
                        sun_elevation + 0.01,
                    ],
                },
            ],
        }
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1
