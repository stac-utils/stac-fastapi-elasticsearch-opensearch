import json
import logging
import os
import uuid
from os import listdir
from os.path import isfile, join
from typing import Callable, Dict

import pytest
from httpx import AsyncClient

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.asyncio
async def test_filter_extension_landing_page_link(app_client, ctx):
    resp = await app_client.get("/")
    assert resp.status_code == 200

    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]

    assert "queryables" in keys


@pytest.mark.asyncio
async def test_filter_extension_collection_link(app_client, load_test_data):
    """Test creation and deletion of a collection"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{test_collection['id']}")
    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]
    assert "queryables" in keys

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_search_filters_post(app_client, ctx):
    filters = []
    pwd = f"{THIS_DIR}/cql2"
    for fn in [fn for f in listdir(pwd) if isfile(fn := join(pwd, f))]:
        with open(fn) as f:
            filters.append(json.loads(f.read()))

    for _filter in filters:
        resp = await app_client.post("/search", json={"filter": _filter})
        if resp.status_code != 200:
            logging.error(f"Failed with status {resp.status_code}")
            logging.error(f"Response body: {resp.json()}")
            logging.error({"filter": _filter})
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
    params = {
        "filter": {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]},
    }
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
    filter = f"eo:cloud_cover={cloud_cover} AND collection='{collection}'"
    resp = await app_client.get(f"/search?filter-lang=cql2-text&filter={filter}")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_ext_and_get_cql2text_cloud_cover_no_results(
    app_client, ctx
):
    collection = ctx.item["collection"]
    cloud_cover = ctx.item["properties"]["eo:cloud_cover"] + 1
    filter = f"eo:cloud_cover={cloud_cover} AND collection='{collection}'"
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


@pytest.mark.asyncio
async def test_search_filter_extension_isnull_post(app_client, ctx):
    # Test for a property that is not null
    params = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "isNull",
            "args": [{"property": "properties.view:sun_elevation"}],
        },
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0

    # Test for the property that is null
    params = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "isNull",
            "args": [{"property": "properties.thispropertyisnull"}],
        },
    }
    resp = await app_client.post("/search", json=params)

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_isnull_get(app_client, ctx):
    # Test for a property that is not null

    resp = await app_client.get("/search?filter=properties.view:sun_elevation IS NULL")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 0

    # Test for the property that is null
    resp = await app_client.get("/search?filter=properties.thispropertyisnull IS NULL")

    assert resp.status_code == 200
    assert len(resp.json()["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_s_intersects_property(app_client, ctx):
    intersecting_geom = {
        "coordinates": [150.04, -33.14],
        "type": "Point",
    }
    params = {
        "filter": {
            "op": "s_intersects",
            "args": [
                {"property": "geometry"},
                intersecting_geom,
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_s_contains_property(app_client, ctx):
    contains_geom = {
        "coordinates": [150.04, -33.14],
        "type": "Point",
    }
    params = {
        "filter": {
            "op": "s_contains",
            "args": [
                {"property": "geometry"},
                contains_geom,
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_s_within_property(app_client, ctx):
    within_geom = {
        "coordinates": [
            [
                [148.5776607193635, -35.257132625788756],
                [153.15052873427666, -35.257132625788756],
                [153.15052873427666, -31.080816742218623],
                [148.5776607193635, -31.080816742218623],
                [148.5776607193635, -35.257132625788756],
            ]
        ],
        "type": "Polygon",
    }
    params = {
        "filter": {
            "op": "s_within",
            "args": [
                {"property": "geometry"},
                within_geom,
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_s_disjoint_property(app_client, ctx):
    intersecting_geom = {
        "coordinates": [0, 0],
        "type": "Point",
    }
    params = {
        "filter": {
            "op": "s_disjoint",
            "args": [
                {"property": "geometry"},
                intersecting_geom,
            ],
        },
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_cql2text_s_intersects_property(app_client, ctx):
    filter = 'S_INTERSECTS("geometry",POINT(150.04 -33.14))'
    params = {
        "filter": filter,
        "filter_lang": "cql2-text",
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_cql2text_s_contains_property(app_client, ctx):
    filter = 'S_CONTAINS("geometry",POINT(150.04 -33.14))'
    params = {
        "filter": filter,
        "filter_lang": "cql2-text",
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_cql2text_s_within_property(app_client, ctx):
    filter = 'S_WITHIN("geometry",POLYGON((148.5776607193635 -35.257132625788756, 153.15052873427666 -35.257132625788756, 153.15052873427666 -31.080816742218623, 148.5776607193635 -31.080816742218623, 148.5776607193635 -35.257132625788756)))'
    params = {
        "filter": filter,
        "filter_lang": "cql2-text",
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_search_filter_extension_cql2text_s_disjoint_property(app_client, ctx):
    filter = 'S_DISJOINT("geometry",POINT(0 0))'
    params = {
        "filter": filter,
        "filter_lang": "cql2-text",
    }
    resp = await app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.asyncio
async def test_queryables_enum_platform(
    app_client: AsyncClient,
    load_test_data: Callable[[str], Dict],
    monkeypatch: pytest.MonkeyPatch,
):
    # Arrange
    # Enforce instant database refresh
    # TODO: Is there a better way to do this?
    monkeypatch.setenv("DATABASE_REFRESH", "true")

    # Create collection
    collection_data = load_test_data("test_collection.json")
    collection_id = collection_data["id"] = f"enum-test-collection-{uuid.uuid4()}"
    r = await app_client.post("/collections", json=collection_data)
    r.raise_for_status()

    # Create items with different platform values
    NUM_ITEMS = 3
    for i in range(1, NUM_ITEMS + 1):
        item_data = load_test_data("test_item.json")
        item_data["id"] = f"enum-test-item-{i}"
        item_data["collection"] = collection_id
        item_data["properties"]["platform"] = "landsat-8" if i % 2 else "sentinel-2"
        r = await app_client.post(f"/collections/{collection_id}/items", json=item_data)
        r.raise_for_status()

    # Act
    # Test queryables endpoint
    queryables = (
        (await app_client.get(f"/collections/{collection_data['id']}/queryables"))
        .raise_for_status()
        .json()
    )

    # Assert
    # Verify distinct values (should only have 2 unique values despite 3 items)
    properties = queryables["properties"]
    platform_info = properties["platform"]
    platform_values = platform_info["enum"]
    assert set(platform_values) == {"landsat-8", "sentinel-2"}

    # Clean up
    r = await app_client.delete(f"/collections/{collection_id}")
    r.raise_for_status()


@pytest.mark.asyncio
async def test_search_filter_ext_or_with_must_condition(app_client, load_test_data):
    """
    Test that OR conditions require at least one match when combined with MUST.
    This test will fail if minimum_should_match=1 is not set in the ES/OS query.
    """
    # Arrange: Create a unique collection for this test
    collection_data = load_test_data("test_collection.json")
    collection_id = collection_data["id"] = f"or-test-collection-{uuid.uuid4()}"
    r = await app_client.post("/collections", json=collection_data)
    r.raise_for_status()

    # Add three items:
    # 1. Matches both must and should
    # 2. Matches must but not should
    # 3. Matches neither
    items = [
        {
            "eo:cloud_cover": 0,
            "proj:epsg": 32756,
        },  # Should be returned when should matches
        {
            "eo:cloud_cover": 5,
            "proj:epsg": 88888,
        },  # Should NOT be returned if min_should_match=1
        {"eo:cloud_cover": -5, "proj:epsg": 99999},  # Should not be returned at all
    ]
    for idx, props in enumerate(items):
        item_data = load_test_data("test_item.json")
        item_data["id"] = f"or-test-item-{idx}"
        item_data["collection"] = collection_id
        item_data["properties"]["eo:cloud_cover"] = props["eo:cloud_cover"]
        item_data["properties"]["proj:epsg"] = props["proj:epsg"]
        r = await app_client.post(f"/collections/{collection_id}/items", json=item_data)
        r.raise_for_status()

    # Case 1: At least one OR condition matches (should return only the first item)
    params = {
        "filter": {
            "op": "and",
            "args": [
                {"op": ">=", "args": [{"property": "eo:cloud_cover"}, 0]},
                {
                    "op": "or",
                    "args": [
                        {
                            "op": "<",
                            "args": [{"property": "eo:cloud_cover"}, 1],
                        },  # Only first item matches
                        {
                            "op": "=",
                            "args": [{"property": "proj:epsg"}, 32756],
                        },  # Only first item matches
                    ],
                },
            ],
        }
    }
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert any(
        f["properties"].get("eo:cloud_cover") == 0 for f in features
    ), "Should return the item where at least one OR condition matches"
    assert all(
        f["properties"].get("eo:cloud_cover") == 0 for f in features
    ), "Should only return the item matching the should clause"

    # Case 2: No OR conditions match (should NOT return the second item if minimum_should_match=1 is set)
    params["filter"]["args"][1]["args"][0]["args"][
        1
    ] = -10  # cloud_cover < -10 (false for all)
    params["filter"]["args"][1]["args"][1]["args"][
        1
    ] = 12345  # proj:epsg == 12345 (false for all)
    resp = await app_client.post("/search", json=params)
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert len(features) == 0, (
        "Should NOT return items that match only the must clause when no OR conditions match "
        "(requires minimum_should_match=1)"
    )

    # Clean up
    r = await app_client.delete(f"/collections/{collection_id}")
    r.raise_for_status()
