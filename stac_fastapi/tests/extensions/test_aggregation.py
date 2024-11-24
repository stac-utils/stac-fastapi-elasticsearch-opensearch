import os
from urllib.parse import urlparse

import pytest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.asyncio
async def test_aggregation_extension_landing_page_link(app_client):
    """Test if the `aggregations` and `aggregate` links are included in the landing page"""
    resp = await app_client.get("/")
    assert resp.status_code == 200

    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]

    assert "aggregations" in keys
    assert "aggregate" in keys


@pytest.mark.asyncio
async def test_aggregation_extension_collection_link(app_client, ctx):
    """Test if the `aggregations` and `aggregate` links are included in the collection links"""

    resp = await app_client.get(f"/collections/{ctx.collection['id']}")
    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]
    assert "aggregations" in keys
    assert "aggregate" in keys


@pytest.mark.asyncio
async def test_get_catalog_aggregations(app_client):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get("/aggregations")

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 7


@pytest.mark.asyncio
async def test_post_catalog_aggregations(app_client):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.post("/aggregations")

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 7


@pytest.mark.asyncio
async def test_get_collection_aggregations(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't

    resp = await app_client.get(f"/collections/{ctx.collection['id']}/aggregations")
    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 15
    rj = resp.json()
    href_self = urlparse(
        next(link["href"] for link in rj["links"] if link["rel"] == "self")
    )
    assert href_self.path == f"/collections/{ctx.collection['id']}/aggregations"


@pytest.mark.asyncio
async def test_post_collection_aggregations(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't

    resp = await app_client.post(f"/collections/{ctx.collection['id']}/aggregations")
    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 15
    rj = resp.json()
    href_self = urlparse(
        next(link["href"] for link in rj["links"] if link["rel"] == "self")
    )
    assert href_self.path == f"/collections/{ctx.collection['id']}/aggregations"


@pytest.mark.asyncio
async def test_aggregate_search_point_does_not_intersect(app_client, ctx):
    point = [15.04, -3.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [ctx.item["collection"]],
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0


@pytest.mark.asyncio
async def test_get_collection_aggregate_no_collection(app_client):

    resp = await app_client.get(
        "/collections/not-a-collection/aggregate?aggregations=total_count"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_collection_aggregate(app_client, ctx):

    resp = await app_client.get(
        f"/collections/{ctx.collection['id']}/aggregate?aggregations=total_count"
    )
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_post_collection_aggregate(app_client, ctx):

    params = {
        "aggregations": ["total_count"],
    }

    resp = await app_client.post(
        f"/collections/{ctx.collection['id']}/aggregate", json=params
    )
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_datetime_out_of_range(app_client):
    params = {
        "datetime": "2023-07-14T02:05:01.324Z/2024-02-28T23:13:08.000Z",
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0


@pytest.mark.asyncio
async def test_aggregate_datetime_in_range(app_client):
    params = {
        "datetime": "2020-02-11T12:30:22Z/2020-02-13T12:30:22Z",
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_filter_extension_eq_post(app_client, ctx):
    params = {
        "filter": {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_filter_extension_neq_post(app_client, ctx):
    params = {
        "filter": {"op": "<>", "args": [{"property": "id"}, ctx.item["id"]]},
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
        "collections": [ctx.item["collection"]],
    }
    resp = await app_client.post("/aggregate", json=params)
    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0


@pytest.mark.asyncio
async def test_aggregate_extension_gte_get(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get(
        '/aggregate?aggregations=total_count&filter-lang=cql2-json&filter={"op":"<=","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1

    resp = await app_client.get(
        '/aggregate?aggregations=total_count&filter-lang=cql2-json&filter={"op":">","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0


@pytest.mark.asyncio
async def test_aggregate_filter_extension_gte_post(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    params = {
        "filter": {
            "op": "<=",
            "args": [
                {"property": "properties.proj:epsg"},
                ctx.item["properties"]["proj:epsg"],
            ],
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1

    params = {
        "filter": {
            "op": ">",
            "args": [
                {"property": "properties.proj:epsg"},
                ctx.item["properties"]["proj:epsg"],
            ],
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }
    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0


@pytest.mark.asyncio
async def test_aggregate_filter_ext_and_get_id(app_client, ctx):
    collection = ctx.item["collection"]
    id = ctx.item["id"]
    filter = f"id='{id}' AND collection='{collection}'"
    resp = await app_client.get(f"/aggregate?aggregations=total_count&filter={filter}")

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_search_aggregate_extension_wildcard_cql2(app_client, ctx):
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
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_filter_extension_wildcard_es(app_client, ctx):
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
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_filter_extension_in(app_client, ctx):
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
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_aggregate_filter_extension_in_no_list(app_client, ctx):
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
        },
        "filter-lang": "cql2-json",
        "aggregations": ["total_count"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 400
    assert resp.json() == {
        "detail": f"Error with cql2 filter: Arg {product_id} is not a list"
    }


@pytest.mark.asyncio
async def test_aggregate_datetime_non_interval(app_client):
    dt_formats = [
        "2020-02-12T12:30:22+00:00",
        "2020-02-12T12:30:22.00Z",
        "2020-02-12T12:30:22Z",
        "2020-02-12T12:30:22.00+00:00",
    ]

    for dt in dt_formats:
        params = {"datetime": dt, "aggregations": ["total_count"]}

        resp = await app_client.post("/aggregate", json=params)
        assert resp.status_code == 200
        assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_post_aggregate_total_count(app_client):

    params = {"aggregations": ["total_count"]}

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_get_aggregate_total_count(app_client):

    resp = await app_client.get("/aggregate?aggregations=total_count")

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1


@pytest.mark.asyncio
async def test_get_aggregate_datetime_max(app_client):

    resp = await app_client.get("/aggregate?aggregations=datetime_max")

    assert resp.status_code == 200
    assert (
        resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22.000Z"
        or resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22Z"
    )


@pytest.mark.asyncio
async def test_post_aggregate_datetime_max(app_client):

    params = {
        "aggregations": ["datetime_max"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert (
        resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22.000Z"
        or resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22Z"
    )


@pytest.mark.asyncio
async def test_get_aggregate_datetime_min(app_client):

    resp = await app_client.get("/aggregate?aggregations=datetime_min")

    assert resp.status_code == 200
    assert (
        resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22.000Z"
        or resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22Z"
    )


@pytest.mark.asyncio
async def test_post_aggregate_datetime_min(app_client, ctx):

    params = {
        "aggregations": ["datetime_min"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert (
        resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22.000Z"
        or resp.json()["aggregations"][0]["value"] == "2020-02-12T12:30:22Z"
    )


@pytest.mark.asyncio
async def test_get_aggregate_datetime_frequency(app_client, ctx):

    resp = await app_client.get("/aggregate?aggregations=datetime_frequency")

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert (
        resp.json()["aggregations"][0]["buckets"][0]["key"]
        == "2020-02-01T00:00:00.000Z"
    )


@pytest.mark.asyncio
async def test_post_aggregate_datetime_frequency(app_client, ctx):

    params = {
        "aggregations": ["datetime_frequency"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert (
        resp.json()["aggregations"][0]["buckets"][0]["key"]
        == "2020-02-01T00:00:00.000Z"
    )


@pytest.mark.asyncio
async def test_get_aggregate_collection_frequency(app_client, ctx):

    resp = await app_client.get("/aggregate?aggregations=collection_frequency")

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "test-collection"


@pytest.mark.asyncio
async def test_post_aggregate_collection_frequency(app_client, ctx):

    params = {
        "aggregations": ["collection_frequency"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "test-collection"


@pytest.mark.asyncio
async def test_get_aggregate_attribute_frequency(app_client, ctx):

    resp = await app_client.get(
        "/aggregate?aggregations=platform_frequency&collections=test-collection"
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "landsat-8"


@pytest.mark.asyncio
async def test_post_aggregate_attribute_frequency(app_client, ctx):

    params = {
        "aggregations": ["platform_frequency"],
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "landsat-8"


@pytest.mark.asyncio
async def test_post_aggregate_unsupported_aggregation(app_client):

    params = {
        "aggregations": ["this_is_not_an_aggregation"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_post_aggregate_unsupported_collection_aggregation(app_client, ctx):

    params = {
        "aggregations": ["this_is_not_an_aggregation"],
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_aggregate_precision_outside_range(app_client):

    resp = await app_client.get(
        "/aggregate?aggregations=centroid_geohash_grid_frequency&centroid_geohash_grid_frequency_precision=55"
    )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_post_aggregate_precision_outside_range(app_client, ctx):

    params = {
        "aggregations": ["centroid_geohash_grid_frequency"],
        "centroid_geohash_grid_frequency_precision": 55,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_aggregate_centroid_geohash_frequency(app_client, ctx):

    resp = await app_client.get(
        "/aggregate?aggregations=centroid_geohash_grid_frequency&centroid_geohash_grid_frequency_precision=5&collections=test-collection"
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "r6572"


@pytest.mark.asyncio
async def test_post_aggregate_centroid_geohash_frequency(app_client, ctx):

    params = {
        "aggregations": ["centroid_geohash_grid_frequency"],
        "centroid_geohash_grid_frequency_precision": 5,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "r6572"


@pytest.mark.asyncio
async def test_get_aggregate_centroid_geohex_frequency(app_client, ctx):

    # geohex is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    resp = await app_client.get(
        "/aggregate?aggregations=centroid_geohex_grid_frequency&centroid_geohex_grid_frequency_precision=5&collections=test-collection"
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "85be0a8ffffffff"


@pytest.mark.asyncio
async def test_post_aggregate_centroid_geohex_frequency(app_client, ctx):

    # geohex is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    params = {
        "aggregations": ["centroid_geohex_grid_frequency"],
        "centroid_geohex_grid_frequency_precision": 5,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "85be0a8ffffffff"


@pytest.mark.asyncio
async def test_get_aggregate_centroid_geotile_frequency(app_client, ctx):

    resp = await app_client.get(
        "/aggregate?aggregations=centroid_geotile_grid_frequency&centroid_geotile_grid_frequency_precision=5&collections=test-collection"
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "5/29/19"


@pytest.mark.asyncio
async def test_post_aggregate_centroid_geotile_frequency(app_client, ctx):

    params = {
        "aggregations": ["centroid_geotile_grid_frequency"],
        "centroid_geotile_grid_frequency_precision": 5,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "5/29/19"


@pytest.mark.asyncio
async def test_get_aggregate_geometry_geotile_frequency(app_client, ctx):

    # geometry geo-aggregation is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    resp = await app_client.get(
        "/aggregate?aggregations=geometry_geotile_grid_frequency&geometry_geotile_grid_frequency_precision=5&collections=test-collection"
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "5/29/19"


@pytest.mark.asyncio
async def test_post_aggregate_geometry_geotile_frequency(app_client, ctx):

    # geometry geo-aggregation is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    params = {
        "aggregations": ["geometry_geotile_grid_frequency"],
        "geometry_geotile_grid_frequency_precision": 5,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["buckets"][0]["frequency"] == 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "5/29/19"


@pytest.mark.asyncio
async def test_get_aggregate_geometry_geohash_frequency(app_client, ctx):

    # geo-aggregation is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    resp = await app_client.get(
        "/aggregate?aggregations=geometry_geohash_grid_frequency&geometry_geohash_grid_frequency_precision=5&collections=test-collection"
    )

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"][0]["buckets"]) > 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "r6hhb"


@pytest.mark.asyncio
async def test_post_aggregate_geometry_geohash_frequency(app_client, ctx):

    # geo-aggregation is only available on a commercial license of ES
    if os.getenv("BACKEND", "elasticsearch").lower() == "elasticsearch":
        pytest.skip()

    params = {
        "aggregations": ["geometry_geohash_grid_frequency"],
        "geometry_geohash_grid_frequency_precision": 5,
        "collections": ["test-collection"],
    }

    resp = await app_client.post("/aggregate", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"][0]["buckets"]) > 1
    assert resp.json()["aggregations"][0]["buckets"][0]["key"] == "r6hhb"
