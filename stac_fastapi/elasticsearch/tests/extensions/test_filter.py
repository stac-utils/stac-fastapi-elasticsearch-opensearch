import json
import os
from os import listdir
from os.path import isfile, join

import pytest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.asyncio
async def test_search_filters(app_client, ctx):

    filters = []
    pwd = f"{THIS_DIR}/cql2"
    for fn in [fn for f in listdir(pwd) if isfile(fn := join(pwd, f))]:
        with open(fn) as f:
            filters.append(json.loads(f.read()))

    for _filter in filters:
        resp = await app_client.post("/search", json={"filter": _filter})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_filter_extension_eq(app_client, ctx):
    params = {"filter": {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]}}
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_gte(app_client, ctx):
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
async def test_search_filter_ext_and(app_client, ctx):
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
async def test_search_filter_extension_floats(app_client, ctx):
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
