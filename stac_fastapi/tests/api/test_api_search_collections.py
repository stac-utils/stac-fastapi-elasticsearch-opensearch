import base64
import copy
import json
import uuid
from urllib.parse import parse_qs, urlparse

import pytest

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from ..conftest import create_collection, refresh_indices


@pytest.mark.asyncio
async def test_collections_sort_id_asc(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor ascending sort on id."""
    # Create multiple collections with different ids
    base_collection = ctx.collection

    # Create collections with ids in a specific order to test sorting
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"asc-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Define endpoints to test
    endpoints = [
        {"method": "GET", "path": "/collections", "params": [("sortby", "+id")]},
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("sortby", "+id")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"sortby": [{"field": "id", "direction": "asc"}]},
        },
    ]

    for endpoint in endpoints:
        # Test ascending sort by id
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]

        # Collections should be sorted alphabetically by id
        sorted_ids = sorted(collection_ids)
        assert len(test_collections) == len(collection_ids)
        for i, expected_id in enumerate(sorted_ids):
            assert test_collections[i]["id"] == expected_id


@pytest.mark.asyncio
async def test_collections_sort_id_desc(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor descending sort on id."""
    # Create multiple collections with different ids
    base_collection = ctx.collection

    # Create collections with ids in a specific order to test sorting
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"desc-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Define endpoints to test
    endpoints = [
        {"method": "GET", "path": "/collections", "params": [("sortby", "-id")]},
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("sortby", "-id")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"sortby": [{"field": "id", "direction": "desc"}]},
        },
    ]

    for endpoint in endpoints:
        # Test descending sort by id
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]

        # Collections should be sorted in reverse alphabetical order by id
        sorted_ids = sorted(collection_ids, reverse=True)
        assert len(test_collections) == len(collection_ids)
        for i, expected_id in enumerate(sorted_ids):
            assert test_collections[i]["id"] == expected_id


@pytest.mark.asyncio
async def test_collections_fields_all_endpoints(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor the fields parameter."""
    # Create multiple collections with different ids
    base_collection = ctx.collection

    # Create collections with ids in a specific order to test fields
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"fields-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-a", f"{test_prefix}-b", f"{test_prefix}-c"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        test_collection["description"] = f"Description for collection {i}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Define endpoints to test
    endpoints = [
        {"method": "GET", "path": "/collections", "params": [("fields", "id,title")]},
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("fields", "id,title")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"fields": {"include": ["id", "title"]}},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert resp.status_code == 200
        resp_json = resp.json()

        collections_list = resp_json["collections"]

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in collections_list if c["id"].startswith(test_prefix)
        ]

        # Collections should only have id and title fields
        for collection in test_collections:
            assert "id" in collection
            assert "title" in collection
            assert "description" not in collection

    # Test exclude fields parameter
    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("fields", "-description")],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("fields", "-description")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"fields": {"exclude": ["description"]}},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert resp.status_code == 200
        resp_json = resp.json()

        collections_list = resp_json["collections"]

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in collections_list if c["id"].startswith(test_prefix)
        ]

        # Collections should have all fields except description
        for collection in test_collections:
            assert "id" in collection
            assert "title" in collection
            assert "description" not in collection
            assert "links" in collection  # links are always included


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "include,exclude",
    [(["doesnotexist"], []), (["nope*"], []), (["doesnotexist"], ["links"])],
)
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/collections"),
        ("GET", "/collections-search"),
        ("POST", "/collections-search"),
    ],
)
async def test_collections_fields_matching_nothing_return_id_only(
    app_client, txn_client, ctx, method, path, include, exclude
):
    test_prefix = f"fields-nomatch-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-a", f"{test_prefix}-b"]
    for coll_id in collection_ids:
        await create_collection(txn_client, {**ctx.collection, "id": coll_id})
    await refresh_indices(txn_client)

    if method == "GET":
        fields = ",".join(include + [f"-{field}" for field in exclude])
        resp = await app_client.get(path, params={"fields": fields})
    else:
        resp = await app_client.post(
            path, json={"fields": {"include": include, "exclude": exclude}}
        )

    assert resp.status_code == 200
    test_collections = [
        c for c in resp.json()["collections"] if c["id"].startswith(test_prefix)
    ]
    assert test_collections == [{"id": coll_id} for coll_id in collection_ids]


@pytest.mark.asyncio
async def test_collections_free_text_all_endpoints(
    app_client, txn_client, ctx, monkeypatch
):
    """Test free text search across all collection endpoints."""
    # Create test data
    test_prefix = f"free-text-{uuid.uuid4().hex[:8]}"
    base_collection = ctx.collection
    search_term = "SEARCHABLETERM"

    monkeypatch.setenv("ENABLE_COLLECTIONS_SEARCH_ROUTE", "true")

    # Create test collections
    target_collection = base_collection.copy()
    target_collection["id"] = f"{test_prefix}-target"
    target_collection["title"] = f"Collection with {search_term} in title"
    await create_collection(txn_client, target_collection)

    decoy_collection = base_collection.copy()
    decoy_collection["id"] = f"{test_prefix}-decoy"
    decoy_collection["title"] = "Collection without the term"
    await create_collection(txn_client, decoy_collection)

    await refresh_indices(txn_client)

    # Define endpoints to test
    endpoints = [
        {"method": "GET", "path": "/collections", "param": "q"},
        {"method": "GET", "path": "/collections-search", "param": "q"},
        {"method": "POST", "path": "/collections-search", "body_key": "q"},
    ]

    for endpoint in endpoints:
        print(f"Testing free text search on {endpoint['method']} {endpoint['path']}")

        if endpoint["method"] == "GET":
            params = [(endpoint["param"], search_term)]
            resp = await app_client.get(endpoint["path"], params=params)
        else:  # POST
            body = {endpoint["body_key"]: search_term}
            resp = await app_client.post(endpoint["path"], json=body)

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']} with status {resp.status_code}"
        resp_json = resp.json()

        collections = resp_json["collections"]

        # Filter to our test collections
        found = [c for c in collections if c["id"].startswith(test_prefix)]
        assert (
            len(found) == 1
        ), f"Expected 1 collection, found {len(found)} for {endpoint['method']} {endpoint['path']}"
        assert (
            found[0]["id"] == target_collection["id"]
        ), f"Expected {target_collection['id']}, found {found[0]['id']} for {endpoint['method']} {endpoint['path']}"


@pytest.mark.asyncio
async def test_collections_filter_search(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor the filter parameter for structured search."""
    # Create multiple collections with different content
    base_collection = ctx.collection

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"filter-{uuid.uuid4().hex[:8]}"

    # Create collections with different content to test structured filter
    test_collections = [
        {
            "id": f"{test_prefix}-sentinel",
            "title": "Sentinel-2 Collection",
            "description": "Collection of Sentinel-2 data",
            "summaries": {"platform": ["sentinel-2a", "sentinel-2b"]},
        },
        {
            "id": f"{test_prefix}-landsat",
            "title": "Landsat Collection",
            "description": "Collection of Landsat data",
            "summaries": {"platform": ["landsat-8", "landsat-9"]},
        },
        {
            "id": f"{test_prefix}-modis",
            "title": "MODIS Collection",
            "description": "Collection of MODIS data",
            "summaries": {"platform": ["terra", "aqua"]},
        },
    ]

    for i, coll in enumerate(test_collections):
        test_collection = base_collection.copy()
        test_collection["id"] = coll["id"]
        test_collection["title"] = coll["title"]
        test_collection["description"] = coll["description"]
        test_collection["summaries"] = coll["summaries"]
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Use the ID of the first test collection for the filter
    test_collection_id = test_collections[0]["id"]

    # Test 1: CQL2-JSON format
    # Create a simple filter for exact ID match using CQL2-JSON
    filter_expr = {"op": "=", "args": [{"property": "id"}, test_collection_id]}

    # Convert to JSON string for URL parameter
    filter_json = json.dumps(filter_expr)

    # Define endpoints to test
    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("filter", filter_json), ("filter-lang", "cql2-json")],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("filter", filter_json), ("filter-lang", "cql2-json")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"filter": filter_expr, "filter-lang": "cql2-json"},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Should find exactly one collection with the specified ID
        found_collections = [
            c for c in resp_json["collections"] if c["id"] == test_collection_id
        ]

        assert (
            len(found_collections) == 1
        ), f"Expected 1 collection with ID {test_collection_id}, found {len(found_collections)} for {endpoint['method']} {endpoint['path']}"
        assert found_collections[0]["id"] == test_collection_id

    # Test 2: CQL2-text format with LIKE operator
    filter_text = f"id LIKE '%{test_collection_id.split('-')[-1]}%'"

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("filter", filter_text), ("filter-lang", "cql2-text")],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("filter", filter_text), ("filter-lang", "cql2-text")],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"filter": filter_text, "filter-lang": "cql2-text"},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Should find the test collection we created
        found_collections = [
            c for c in resp_json["collections"] if c["id"] == test_collection_id
        ]
        assert (
            len(found_collections) >= 1
        ), f"Expected at least 1 collection with ID {test_collection_id} using LIKE filter for {endpoint['method']} {endpoint['path']}"


@pytest.mark.asyncio
async def test_collections_filter_search_in_ids_post_only(app_client, txn_client, ctx):
    """Verify POST /collections-search honors CQL2-json `in` filter on id for multiple collections."""
    base_collection = ctx.collection

    test_prefix = f"filter-in-{uuid.uuid4().hex[:8]}"
    target_ids = [f"{test_prefix}-1", f"{test_prefix}-2"]
    other_id = f"{test_prefix}-other"

    # Create two target collections and one extra collection
    for coll_id in target_ids + [other_id]:
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {coll_id}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # CQL2-json filter: id IN [target_ids]
    filter_expr = {
        "op": "in",
        "args": [
            {"property": "id"},
            target_ids,
        ],
    }

    resp = await app_client.post(
        "/collections-search",
        json={
            "filter": filter_expr,
            "filter_lang": "cql2-json",
            "limit": 10,
        },
    )

    assert resp.status_code == 200, resp.text
    resp_json = resp.json()

    returned_ids = {
        c["id"] for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    }

    # Only the two target collections should be returned, not the other one
    assert returned_ids == set(target_ids)


@pytest.mark.asyncio
async def test_collections_query_extension(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor the query extension."""
    # Create multiple collections with different content
    base_collection = ctx.collection
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"query-ext-{uuid.uuid4().hex[:8]}"

    # Create collections with different content to test query extension
    test_collections = [
        {
            "id": f"{test_prefix}-sentinel",
            "title": "Sentinel-2 Collection",
            "description": "Collection of Sentinel-2 data",
            "summaries": {"platform": ["sentinel-2a", "sentinel-2b"]},
        },
        {
            "id": f"{test_prefix}-landsat",
            "title": "Landsat Collection",
            "description": "Collection of Landsat data",
            "summaries": {"platform": ["landsat-8", "landsat-9"]},
        },
        {
            "id": f"{test_prefix}-modis",
            "title": "MODIS Collection",
            "description": "Collection of MODIS data",
            "summaries": {"platform": ["terra", "aqua"]},
        },
    ]

    for i, coll in enumerate(test_collections):
        test_collection = base_collection.copy()
        test_collection["id"] = coll["id"]
        test_collection["title"] = coll["title"]
        test_collection["description"] = coll["description"]
        test_collection["summaries"] = coll["summaries"]
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Test 1: Query with equal operator
    sentinel_id = f"{test_prefix}-sentinel"
    query = {"id": {"eq": sentinel_id}}

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"query": json.dumps(query)},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        found_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]

        # Should only find the sentinel collection
        assert (
            len(found_collections) == 1
        ), f"Expected 1 collection for {endpoint['method']} {endpoint['path']}"
        assert found_collections[0]["id"] == sentinel_id

    # Test 2: Query with not-equal operator
    query = {"id": {"neq": f"{test_prefix}-sentinel"}}

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"query": json.dumps(query)},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        found_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]
        found_ids = [c["id"] for c in found_collections]

        # Should find landsat and modis collections but not sentinel
        assert (
            len(found_collections) == 2
        ), f"Expected 2 collections for {endpoint['method']} {endpoint['path']}"
        assert f"{test_prefix}-sentinel" not in found_ids
        assert f"{test_prefix}-landsat" in found_ids
        assert f"{test_prefix}-modis" in found_ids


@pytest.mark.asyncio
async def test_collections_datetime_filter(app_client, load_test_data, txn_client):
    """Test filtering collections by datetime across all endpoints."""
    # Create a test collection with a specific temporal extent

    base_collection = load_test_data("test_collection.json")
    base_collection["extent"]["temporal"]["interval"] = [
        ["2020-01-01T00:00:00Z", "2020-12-31T23:59:59Z"]
    ]
    test_collection_id = base_collection["id"]

    await create_collection(txn_client, base_collection)
    await refresh_indices(txn_client)

    # Test scenarios with different datetime ranges
    test_scenarios = [
        {
            "name": "overlapping range",
            "datetime": "2020-06-01T00:00:00Z/2021-01-01T00:00:00Z",
            "expected_count": 1,
        },
        {
            "name": "before range",
            "datetime": "2019-01-01T00:00:00Z/2019-12-31T23:59:59Z",
            "expected_count": 0,
        },
        {
            "name": "after range",
            "datetime": "2021-01-01T00:00:00Z/2021-12-31T23:59:59Z",
            "expected_count": 0,
        },
        {
            "name": "single datetime within range",
            "datetime": "2020-06-15T12:00:00Z",
            "expected_count": 1,
        },
        {
            "name": "open-ended future range",
            "datetime": "2020-06-01T00:00:00Z/..",
            "expected_count": 1,
        },
    ]

    for scenario in test_scenarios:
        endpoints = [
            {
                "method": "GET",
                "path": "/collections",
                "params": [("datetime", scenario["datetime"])],
            },
            {
                "method": "GET",
                "path": "/collections-search",
                "params": [("datetime", scenario["datetime"])],
            },
            {
                "method": "POST",
                "path": "/collections-search",
                "body": {"datetime": scenario["datetime"]},
            },
        ]

        for endpoint in endpoints:
            if endpoint["method"] == "GET":
                resp = await app_client.get(endpoint["path"], params=endpoint["params"])
            else:  # POST
                resp = await app_client.post(endpoint["path"], json=endpoint["body"])

            assert (
                resp.status_code == 200
            ), f"Failed for {endpoint['method']} {endpoint['path']} with {scenario['name']}"
            resp_json = resp.json()
            found_collections = [
                c for c in resp_json["collections"] if c["id"] == test_collection_id
            ]
            assert len(found_collections) == scenario["expected_count"], (
                f"Expected {scenario['expected_count']} collection(s) for {scenario['name']} "
                f"on {endpoint['method']} {endpoint['path']}, found {len(found_collections)}"
            )

    # Test 6: Open-ended range (from the past to a date within the collection's range)
    # TODO: This test is currently skipped due to an unresolved issue with open-ended past range queries.
    # The query works correctly in Postman but fails in the test environment.
    # Further investigation is needed to understand why this specific query pattern fails.
    """
    resp = await app_client.get(
        "/collections?datetime=../2025-02-01T00:00:00Z"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    found_collections = [c for c in resp_json["collections"] if c["id"] == test_collection_id]
    assert len(found_collections) == 1, f"Expected to find collection {test_collection_id} with open-ended past range to a date within its range"
    """


@pytest.mark.asyncio
async def test_collections_number_matched_returned(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search return correct numberMatched and numberReturned values."""
    # Create multiple collections with different ids
    base_collection = ctx.collection

    # Create collections with ids in a specific order to test pagination
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"count-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-{i}" for i in range(10)]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Test 1: With limit=5
    endpoints = [
        {"method": "GET", "path": "/collections", "params": [("limit", "5")]},
        {"method": "GET", "path": "/collections-search", "params": [("limit", "5")]},
        {"method": "POST", "path": "/collections-search", "body": {"limit": 5}},
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]

        # Should return 5 collections
        assert (
            len(test_collections) == 5
        ), f"Expected 5 test collections for {endpoint['method']} {endpoint['path']}"

        # Check that numberReturned matches the number of collections returned
        assert resp_json["numberReturned"] == len(resp_json["collections"])

        # Check that numberMatched is greater than or equal to numberReturned
        assert resp_json["numberMatched"] >= resp_json["numberReturned"]

        # Check that numberMatched includes at least all our test collections
        assert resp_json["numberMatched"] >= len(collection_ids)

    # Test 2: With a query that should match only one collection
    query = {"id": {"eq": f"{test_prefix}-1"}}

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("query", json.dumps(query))],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"query": json.dumps(query)},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
        ]

        # Should return only 1 collection
        assert (
            len(test_collections) == 1
        ), f"Expected 1 test collection for {endpoint['method']} {endpoint['path']}"
        assert test_collections[0]["id"] == f"{test_prefix}-1"

        # Check that numberReturned matches the number of collections returned
        assert resp_json["numberReturned"] == len(resp_json["collections"])

        # Check that numberMatched matches the number of collections that match the query
        assert resp_json["numberMatched"] >= 1


@pytest.mark.asyncio
async def test_collections_post(app_client, txn_client, ctx):
    """Verify POST /collections-search endpoint works."""

    # Create multiple collections with different ids
    base_collection = ctx.collection

    # Create collections with ids in a specific order to test search
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"post-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-{i}" for i in range(10)]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Test basic POST search
    resp = await app_client.post(
        "/collections-search",
        json={"limit": 5},
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    test_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Should return 5 collections
    assert len(test_collections) == 5

    # Check that numberReturned matches the number of collections returned
    assert resp_json["numberReturned"] == len(resp_json["collections"])

    # Check that numberMatched is greater than or equal to numberReturned
    assert resp_json["numberMatched"] >= resp_json["numberReturned"]

    # Test POST search with sortby
    resp = await app_client.post(
        "/collections-search",
        json={"sortby": [{"field": "id", "direction": "desc"}]},
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    test_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Check that collections are sorted by id in descending order
    if len(test_collections) >= 2:
        assert test_collections[0]["id"] > test_collections[1]["id"]

    # Check that numberReturned matches the number of collections returned
    assert resp_json["numberReturned"] == len(resp_json["collections"])

    # Test POST search with fields
    resp = await app_client.post(
        "/collections-search",
        json={"fields": {"exclude": ["stac_version"]}},
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    test_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Check that stac_version is excluded from the collections
    for collection in test_collections:
        assert "stac_version" not in collection


@pytest.mark.asyncio
async def test_collections_search_cql2_text(app_client, txn_client, ctx):
    """Test collections search with CQL2-text filter."""
    # Create a unique prefix for test collections
    test_prefix = f"test-{uuid.uuid4().hex[:8]}"

    # Create test collections
    collection_data = ctx.collection.copy()
    collection_data["id"] = f"{test_prefix}-collection"
    await create_collection(txn_client, collection_data)
    await refresh_indices(txn_client)

    # Test GET search with CQL2-text filter
    collection_id = collection_data["id"]
    resp = await app_client.get(
        f"/collections-search?filter-lang=cql2-text&filter=id='{collection_id}'"
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones with our test prefix
    filtered_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Check that only the filtered collection is returned
    assert len(filtered_collections) == 1
    assert filtered_collections[0]["id"] == collection_id

    # Test GET search with more complex CQL2-text filter (LIKE operator)
    resp = await app_client.get(
        f"/collections-search?filter-lang=cql2-text&filter=id LIKE '{test_prefix}%'"
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones with our test prefix
    filtered_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Check that all test collections are returned
    assert (
        len(filtered_collections) == 1
    )  # We only created one collection with this prefix
    assert filtered_collections[0]["id"] == collection_id


@pytest.mark.asyncio
async def test_collections_pagination_all_endpoints(app_client, txn_client, ctx):
    """Test pagination works correctly across all collection endpoints."""
    # Create test data
    test_prefix = f"pagination-{uuid.uuid4().hex[:8]}"
    base_collection = ctx.collection

    # Create 10 test collections with predictable IDs for sorting
    test_collections = []
    for i in range(10):
        test_coll = base_collection.copy()
        test_coll["id"] = f"{test_prefix}-{i:02d}"
        test_coll["title"] = f"Test Collection {i}"
        test_collections.append(test_coll)
        await create_collection(txn_client, test_coll)

    await refresh_indices(txn_client)

    # Define endpoints to test
    endpoints = [
        {"method": "GET", "path": "/collections", "param": "limit"},
        {"method": "GET", "path": "/collections-search", "param": "limit"},
        {"method": "POST", "path": "/collections-search", "body_key": "limit"},
    ]

    # Test pagination for each endpoint
    for endpoint in endpoints:
        # Test first page with limit=3
        limit = 3

        # Make the request
        if endpoint["method"] == "GET":
            params = [(endpoint["param"], str(limit))]
            resp = await app_client.get(endpoint["path"], params=params)
        else:  # POST
            body = {endpoint["body_key"]: limit}
            resp = await app_client.post(endpoint["path"], json=body)

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}"
        resp_json = resp.json()

        # # Filter to our test collections
        # if endpoint["path"] == "/collections":
        #     found_collections = resp_json
        # else:  # For collection-search endpoints
        found_collections = resp_json["collections"]

        test_found = [c for c in found_collections if c["id"].startswith(test_prefix)]

        # Should return exactly limit collections
        assert (
            len(test_found) == limit
        ), f"Expected {limit} collections, got {len(test_found)}"

        # Verify collections are in correct order (ascending by ID)
        expected_ids = [f"{test_prefix}-{i:02d}" for i in range(limit)]
        for i, expected_id in enumerate(expected_ids):
            assert test_found[i]["id"] == expected_id

        # Test second page using the token from the next link
        next_link = None
        for link in resp_json.get("links", []):
            if link.get("rel") == "next":
                next_link = link
                break

        if next_link:
            # Extract token based on method
            if endpoint["method"] == "GET":
                # For GET, token is in the URL query params
                from urllib.parse import parse_qs, urlparse

                parsed_url = urlparse(next_link["href"])
                query_params = parse_qs(parsed_url.query)
                token = query_params.get("token", [None])[0]

                if token:
                    params = [(endpoint["param"], str(limit)), ("token", token)]
                    resp = await app_client.get(endpoint["path"], params=params)
                else:
                    continue  # Skip if no token found
            else:  # POST
                # For POST, token is in the body
                body = next_link.get("body", {})
                if "token" in body:
                    resp = await app_client.post(endpoint["path"], json=body)
                else:
                    continue  # Skip if no token found

            assert (
                resp.status_code == 200
            ), f"Failed for {endpoint['method']} {endpoint['path']} with token"
            resp_json = resp.json()

            # Filter to our test collections
            found_collections = resp_json["collections"]

            test_found = [
                c for c in found_collections if c["id"].startswith(test_prefix)
            ]

            # Should return next set of collections
            expected_ids = [f"{test_prefix}-{i:02d}" for i in range(limit, limit * 2)]
            assert len(test_found) == min(
                limit, len(expected_ids)
            ), f"Expected {min(limit, len(expected_ids))} collections, got {len(test_found)}"

            # Verify collections are in correct order
            for i, expected_id in enumerate(expected_ids[: len(test_found)]):
                assert test_found[i]["id"] == expected_id

        # Test with sortby parameter to ensure token works with sorting
        if endpoint["method"] == "GET":
            params = [("sortby", "-id"), (endpoint["param"], str(limit))]
            resp = await app_client.get(endpoint["path"], params=params)
        else:  # POST
            body = {
                "sortby": [{"field": "id", "direction": "desc"}],
                endpoint["body_key"]: limit,
            }
            resp = await app_client.post(endpoint["path"], json=body)

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']} with sortby"
        resp_json = resp.json()

        found_collections = resp_json["collections"]

        test_found = [c for c in found_collections if c["id"].startswith(test_prefix)]

        # Verify collections are sorted in descending order
        # We expect the highest IDs first (09, 08, 07, etc.)
        expected_ids = sorted(
            [f"{test_prefix}-{i:02d}" for i in range(10)], reverse=True
        )[:limit]

        # Filter expected_ids to only include collections that actually exist in the response
        expected_ids = [
            id for id in expected_ids if any(c["id"] == id for c in found_collections)
        ]

        for i, expected_id in enumerate(expected_ids):
            assert test_found[i]["id"] == expected_id


@pytest.mark.asyncio
async def test_collections_bbox_all_endpoints(app_client, txn_client, ctx):
    """Verify GET /collections, GET /collections-search, and POST /collections-search honor the bbox parameter."""
    # Create multiple collections with different spatial extents
    base_collection = ctx.collection

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"bbox-{uuid.uuid4().hex[:8]}"

    # Create collections with different bboxes
    # Collection 1: Europe bbox
    collection_europe = base_collection.copy()
    collection_europe["id"] = f"{test_prefix}-europe"
    collection_europe["title"] = "Europe Collection"
    collection_europe["extent"] = {
        "spatial": {"bbox": [[-10.0, 35.0, 40.0, 70.0]]},
        "temporal": {"interval": [[None, None]]},
    }
    await create_collection(txn_client, collection_europe)

    # Collection 2: North America bbox
    collection_na = base_collection.copy()
    collection_na["id"] = f"{test_prefix}-north-america"
    collection_na["title"] = "North America Collection"
    collection_na["extent"] = {
        "spatial": {"bbox": [[-170.0, 15.0, -50.0, 75.0]]},
        "temporal": {"interval": [[None, None]]},
    }
    await create_collection(txn_client, collection_na)

    # Collection 3: Asia bbox
    collection_asia = base_collection.copy()
    collection_asia["id"] = f"{test_prefix}-asia"
    collection_asia["title"] = "Asia Collection"
    collection_asia["extent"] = {
        "spatial": {"bbox": [[60.0, -10.0, 150.0, 55.0]]},
        "temporal": {"interval": [[None, None]]},
    }
    await create_collection(txn_client, collection_asia)

    # Collection 4: Global bbox (should match any query)
    collection_global = base_collection.copy()
    collection_global["id"] = f"{test_prefix}-global"
    collection_global["title"] = "Global Collection"
    collection_global["extent"] = {
        "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
        "temporal": {"interval": [[None, None]]},
    }
    await create_collection(txn_client, collection_global)

    # Collection 5: 3D bbox (with altitude) - should still work for 2D queries
    collection_3d = base_collection.copy()
    collection_3d["id"] = f"{test_prefix}-3d-europe"
    collection_3d["title"] = "3D Europe Collection"
    collection_3d["extent"] = {
        "spatial": {"bbox": [[-10.0, 35.0, 0.0, 40.0, 70.0, 5000.0]]},  # 3D bbox
        "temporal": {"interval": [[None, None]]},
    }
    await create_collection(txn_client, collection_3d)

    await refresh_indices(txn_client)

    # Test 1: Query for Europe region - should match Europe, Global, and 3D Europe collections
    europe_bbox = [0.0, 40.0, 20.0, 60.0]  # Central Europe

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("bbox", ",".join(map(str, europe_bbox)))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("bbox", ",".join(map(str, europe_bbox)))],
        },
        {
            "method": "POST",
            "path": "/collections-search",
            "body": {"bbox": europe_bbox},
        },
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}: {resp.text}"
        resp_json = resp.json()

        collections_list = resp_json["collections"]

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in collections_list if c["id"].startswith(test_prefix)
        ]

        # Should find Europe, Global, and 3D Europe collections
        found_ids = {c["id"] for c in test_collections}
        assert (
            f"{test_prefix}-europe" in found_ids
        ), f"Europe collection not found {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-global" in found_ids
        ), f"Global collection not found {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-3d-europe" in found_ids
        ), f"3D Europe collection not found {endpoint['method']} {endpoint['path']}"
        # Should NOT find North America or Asia
        assert (
            f"{test_prefix}-north-america" not in found_ids
        ), f"North America should not match Europe bbox in {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-asia" not in found_ids
        ), f"Asia should not match Europe bbox in {endpoint['method']} {endpoint['path']}"

    # Test 2: Query for North America region - should match North America and Global collections
    na_bbox = [-120.0, 30.0, -80.0, 50.0]  # Central North America

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("bbox", ",".join(map(str, na_bbox)))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("bbox", ",".join(map(str, na_bbox)))],
        },
        {"method": "POST", "path": "/collections-search", "body": {"bbox": na_bbox}},
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}: {resp.text}"
        resp_json = resp.json()

        collections_list = resp_json["collections"]

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in collections_list if c["id"].startswith(test_prefix)
        ]

        # Should find North America and Global collections
        found_ids = {c["id"] for c in test_collections}
        assert (
            f"{test_prefix}-north-america" in found_ids
        ), f"North America collection not found {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-global" in found_ids
        ), f"Global collection not found {endpoint['method']} {endpoint['path']}"
        # Should NOT find Europe, Asia, or 3D Europe
        assert (
            f"{test_prefix}-europe" not in found_ids
        ), f"Europe should not match North America bbox in {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-asia" not in found_ids
        ), f"Asia should not match North America bbox in {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-3d-europe" not in found_ids
        ), f"3D Europe should not match North America bbox in {endpoint['method']} {endpoint['path']}"

    # Test 3: Query for Asia region - should match Asia and Global collections
    asia_bbox = [100.0, 20.0, 130.0, 45.0]  # East Asia

    endpoints = [
        {
            "method": "GET",
            "path": "/collections",
            "params": [("bbox", ",".join(map(str, asia_bbox)))],
        },
        {
            "method": "GET",
            "path": "/collections-search",
            "params": [("bbox", ",".join(map(str, asia_bbox)))],
        },
        {"method": "POST", "path": "/collections-search", "body": {"bbox": asia_bbox}},
    ]

    for endpoint in endpoints:
        if endpoint["method"] == "GET":
            resp = await app_client.get(endpoint["path"], params=endpoint["params"])
        else:  # POST
            resp = await app_client.post(endpoint["path"], json=endpoint["body"])

        assert (
            resp.status_code == 200
        ), f"Failed for {endpoint['method']} {endpoint['path']}: {resp.text}"
        resp_json = resp.json()

        collections_list = resp_json["collections"]

        # Filter collections to only include the ones we created for this test
        test_collections = [
            c for c in collections_list if c["id"].startswith(test_prefix)
        ]

        # Should find Asia and Global collections
        found_ids = {c["id"] for c in test_collections}
        assert (
            f"{test_prefix}-asia" in found_ids
        ), f"Asia collection not found {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-global" in found_ids
        ), f"Global collection not found {endpoint['method']} {endpoint['path']}"
        # Should NOT find Europe, North America, or 3D Europe
        assert (
            f"{test_prefix}-europe" not in found_ids
        ), f"Europe should not match Asia bbox in {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-north-america" not in found_ids
        ), f"North America should not match Asia bbox in {endpoint['method']} {endpoint['path']}"
        assert (
            f"{test_prefix}-3d-europe" not in found_ids
        ), f"3D Europe should not match Asia bbox in {endpoint['method']} {endpoint['path']}"


COLLECTION_LISTINGS = [
    ("GET", "/collections"),
    ("GET", "/collections-search"),
    ("POST", "/collections-search"),
]


def _prefix_filter(method: str, prefix: str) -> dict:
    if method == "GET":
        return {"filter": f"id LIKE '{prefix}%'", "filter-lang": "cql2-text"}
    return {
        "filter": {"op": "like", "args": [{"property": "id"}, f"{prefix}%"]},
        "filter-lang": "cql2-json",
    }


async def _walk_collections(app_client, method, path, query, max_pages):
    """Follow rel=next from the first page and return the ids in page order."""
    seen: list[str] = []
    tokens_seen: set[str] = set()
    url, params, body = path, query, query
    for _ in range(max_pages):  # bounded, so a looping token fails instead of hanging
        if method == "GET":
            resp = await app_client.get(url, params=params)
        else:
            resp = await app_client.post(path, json=body)
        assert resp.status_code == 200, resp.text
        page = resp.json()
        seen += [c["id"] for c in page["collections"]]
        nxt = next((link for link in page["links"] if link["rel"] == "next"), None)
        if nxt is None:
            return seen
        if method == "GET":
            token = parse_qs(urlparse(nxt["href"]).query)["token"][0]
            url, params = nxt["href"], None
        else:
            token = nxt["body"]["token"]
            body = nxt["body"]
        assert token not in tokens_seen, "next repeated a token: paging is looping"
        tokens_seen.add(token)
    raise AssertionError("paging did not terminate")


async def _create_collections(txn_client, test_collection, ids, titles=None):
    for i, coll_id in enumerate(ids):
        collection = {**test_collection, "id": coll_id}
        collection.pop("title", None)
        if titles and titles[i] is not None:
            collection["title"] = titles[i]
        await create_collection(txn_client, collection)


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", COLLECTION_LISTINGS)
async def test_collections_next_link_pages_through_tricky_ids(
    app_client, txn_client, test_collection, method, path
):
    """Paging one at a time terminates and returns each id once.

    The old raw token split on ``|``, so an id containing ``|`` sent ``next``
    back to page 1 forever. ``+`` and ``=`` check that the link escapes the token.
    """
    prefix = f"ctok-{uuid.uuid4().hex[:8]}"
    ids = [f"{prefix}-a|b", f"{prefix}-c+d", f"{prefix}-e=f"]
    try:
        await _create_collections(txn_client, test_collection, ids)
        query = {**_prefix_filter(method, prefix), "limit": 1}
        seen = await _walk_collections(app_client, method, path, query, len(ids) + 3)
        assert seen == sorted(ids)
    finally:
        for coll_id in ids:
            await txn_client.delete_collection(coll_id)


@pytest.mark.asyncio
async def test_collections_paging_sorted_by_title_with_missing_and_empty_titles(
    app_client, txn_client, test_collection
):
    """Tokens carrying a ``""`` or ``null`` sort value resume at the right place.

    Empty titles sort first and missing titles last, so the walk follows both.
    """
    prefix = f"ctok-{uuid.uuid4().hex[:8]}"
    ids = [f"{prefix}-{i}" for i in range(5)]
    titles = ["a|b", "", "", None, None]
    try:
        await _create_collections(txn_client, test_collection, ids, titles)
        query = {**_prefix_filter("GET", prefix), "sortby": "title", "limit": 1}
        seen = await _walk_collections(app_client, "GET", "/collections", query, 8)
        assert seen == [ids[1], ids[2], ids[0], ids[3], ids[4]]
    finally:
        for coll_id in ids:
            await txn_client.delete_collection(coll_id)


@pytest.mark.asyncio
async def test_collections_old_or_invalid_token_returns_first_page(
    app_client, txn_client, test_collection
):
    """A raw token from the old format, or any invalid token, returns page 1."""
    prefix = f"ctok-{uuid.uuid4().hex[:8]}"
    ids = [f"{prefix}-0", f"{prefix}-1"]
    try:
        await _create_collections(txn_client, test_collection, ids)
        query = {**_prefix_filter("GET", prefix), "limit": 1}
        first = await app_client.get("/collections", params=query)
        assert [c["id"] for c in first.json()["collections"]] == [ids[0]]

        non_list = base64.urlsafe_b64encode(b'{"x": 1}').decode()
        for token in ("zzz", "a|b", non_list):
            resp = await app_client.get(
                "/collections", params={**query, "token": token}
            )
            assert resp.status_code == 200, resp.text
            assert [c["id"] for c in resp.json()["collections"]] == [ids[0]], token
    finally:
        for coll_id in ids:
            await txn_client.delete_collection(coll_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", COLLECTION_LISTINGS)
async def test_collections_full_last_page_has_no_next_link(
    app_client, txn_client, test_collection, method, path
):
    """A last page holding exactly ``limit`` collections has no next link."""
    prefix = f"ctok-{uuid.uuid4().hex[:8]}"
    ids = [f"{prefix}-0", f"{prefix}-1"]
    try:
        await _create_collections(txn_client, test_collection, ids)
        query = {**_prefix_filter(method, prefix), "limit": 2}
        if method == "GET":
            resp = await app_client.get(path, params=query)
        else:
            resp = await app_client.post(path, json=query)
        assert resp.status_code == 200, resp.text
        page = resp.json()
        assert [c["id"] for c in page["collections"]] == ids
        assert "next" not in [link["rel"] for link in page["links"]]
    finally:
        for coll_id in ids:
            await txn_client.delete_collection(coll_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "injected",
    [
        {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        None,
    ],
)
async def test_merge_patch_ignores_bbox_shape(
    app_client, txn_client, ctx, monkeypatch, validator, injected
):
    """bbox_shape is backend-owned and derived from the extent, so a merge patch
    must not replace or delete it. The test collection's extent is global: with
    the injected 1-degree shape at the origin (or the field removed), a bbox
    search inside the real extent would stop listing it."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    collection_id = ctx.collection["id"]

    resp = await app_client.patch(
        f"/collections/{collection_id}",
        json={"bbox_shape": injected},
        headers={"Content-Type": "application/merge-patch+json"},
    )
    assert resp.status_code == 200, resp.text

    await refresh_indices(txn_client)
    resp = await app_client.get("/collections", params={"bbox": "100,10,110,20"})
    assert resp.status_code == 200, resp.text
    assert collection_id in {c["id"] for c in resp.json()["collections"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "shape", ["add", "replace", "remove", "copy-from", "move-to", "test", "root"]
)
async def test_json_patch_rejects_bbox_shape_operations(
    app_client, txn_client, ctx, monkeypatch, validator, shape
):
    """JSON Patch may not read or write the derived bbox_shape (#1017)."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    collection_id = ctx.collection["id"]
    tiny = {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
    op = {
        "add": {"op": "add", "path": "/bbox_shape", "value": tiny},
        "replace": {"op": "replace", "path": "/bbox_shape", "value": tiny},
        "remove": {"op": "remove", "path": "/bbox_shape"},
        "copy-from": {"op": "copy", "from": "/bbox_shape", "path": "/keywords"},
        "move-to": {"op": "move", "from": "/keywords", "path": "/bbox_shape"},
        "test": {"op": "test", "path": "/bbox_shape", "value": tiny},
        "root": {
            "op": "replace",
            "path": "",
            "value": {**ctx.collection, "bbox_shape": tiny},
        },
    }[shape]
    document = dict(index=COLLECTIONS_INDEX, id=collection_id)
    before = await txn_client.database.client.get(**document)

    resp = await app_client.patch(
        f"/collections/{collection_id}",
        json=[op],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert resp.status_code == 400, resp.text
    assert "A patch may not address bbox_shape" in resp.json()["detail"]
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]

    await refresh_indices(txn_client)
    resp = await app_client.get("/collections", params={"bbox": "100,10,110,20"})
    assert resp.status_code == 200, resp.text
    assert collection_id in {c["id"] for c in resp.json()["collections"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["post", "put"])
async def test_collection_body_bbox_shape_is_ignored(
    app_client, txn_client, ctx, load_test_data, method
):
    """Create and update derive bbox_shape from the extent and drop a body value (#1017)."""
    collection = load_test_data("test_collection.json")
    collection["id"] = f"bbox-shape-body-{uuid.uuid4()}"
    body = {
        **collection,
        "bbox_shape": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
    }

    if method == "post":
        resp = await app_client.post("/collections", json=body)
        assert resp.status_code == 201, resp.text
    else:
        resp = await app_client.post("/collections", json=collection)
        assert resp.status_code == 201, resp.text
        resp = await app_client.put(f"/collections/{collection['id']}", json=body)
        assert resp.status_code == 200, resp.text

    stored = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection["id"]
    )
    assert stored["_source"]["bbox_shape"]["coordinates"] == [
        [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    ]
    await refresh_indices(txn_client)
    resp = await app_client.get("/collections", params={"bbox": "100,10,110,20"})
    assert resp.status_code == 200, resp.text
    assert collection["id"] in {c["id"] for c in resp.json()["collections"]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type, patch",
    [
        (
            "application/json-patch+json",
            [
                {
                    "op": "replace",
                    "path": "/extent/spatial/bbox",
                    "value": [[0, 0, 1, 1]],
                }
            ],
        ),
        (
            "application/merge-patch+json",
            {"extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}}},
        ),
        (
            "application/json-patch+json",
            [
                {
                    "op": "replace",
                    "path": "/extent/spatial/bbox",
                    "value": [[0, 0, -10, 1, 1, 10]],
                }
            ],
        ),
    ],
    ids=["json-patch", "merge-patch", "json-patch-3d"],
)
async def test_patch_without_validator_rebuilds_bbox_shape(
    app_client, txn_client, ctx, monkeypatch, content_type, patch
):
    """A validator-off patch derives bbox_shape from the patched extent (#1017)."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    collection_id = ctx.collection["id"]

    resp = await app_client.patch(
        f"/collections/{collection_id}",
        json=patch,
        headers={"Content-Type": content_type},
    )
    assert resp.status_code == 200, resp.text

    stored = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection_id
    )
    assert stored["_source"]["bbox_shape"] == {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }
    await refresh_indices(txn_client)
    for bbox, is_listed in (("100,10,110,20", False), ("0.2,0.2,0.8,0.8", True)):
        resp = await app_client.get("/collections", params={"bbox": bbox})
        assert resp.status_code == 200, resp.text
        assert (
            collection_id in {c["id"] for c in resp.json()["collections"]}
        ) is is_listed


@pytest.mark.asyncio
async def test_patch_without_validator_drops_bbox_shape_with_extent_bbox(
    app_client, txn_client, ctx, monkeypatch
):
    """Removing the extent bbox removes the derived shape instead of keeping it stale (#1017)."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    collection_id = ctx.collection["id"]

    resp = await app_client.patch(
        f"/collections/{collection_id}",
        json=[{"op": "remove", "path": "/extent/spatial/bbox"}],
        headers={"Content-Type": "application/json-patch+json"},
    )
    assert resp.status_code == 200, resp.text

    stored = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection_id
    )
    assert "bbox_shape" not in stored["_source"]


@pytest.mark.asyncio
async def test_patch_without_validator_rejects_extent_the_index_rejects(
    app_client, txn_client, ctx, monkeypatch
):
    """A derived shape the index rejects fails the patch as a whole (#1017)."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    collection_id = ctx.collection["id"]
    document = dict(index=COLLECTIONS_INDEX, id=collection_id)
    before = await txn_client.database.client.get(**document)

    resp = await app_client.patch(
        f"/collections/{collection_id}",
        json=[
            {"op": "replace", "path": "/extent/spatial/bbox", "value": [[5, 5, 5, 5]]}
        ],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert resp.status_code == 400, resp.text
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]


@pytest.mark.asyncio
async def test_create_collection_stores_bbox_shape_by_default(
    app_client, txn_client, ctx, load_test_data, monkeypatch
):
    """Collection search is on by default, so bbox_shape is derived without either flag (#1017)."""
    monkeypatch.delenv("ENABLE_COLLECTIONS_SEARCH", raising=False)
    monkeypatch.delenv("ENABLE_COLLECTIONS_SEARCH_ROUTE", raising=False)
    collection = load_test_data("test_collection.json")
    collection["id"] = f"bbox-shape-default-{uuid.uuid4()}"

    resp = await app_client.post("/collections", json=collection)
    assert resp.status_code == 201, resp.text

    stored = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection["id"]
    )
    assert "bbox_shape" in stored["_source"]


@pytest.mark.asyncio
async def test_collections_3d_bbox_filters_by_horizontal_bounds(
    app_client, txn_client, ctx
):
    """A 6-value bbox filters collections by its horizontal bounds."""
    test_prefix = f"bbox3d-{uuid.uuid4().hex[:8]}"
    for suffix, bbox in (("keep", [10, 10, 20, 20]), ("drop", [-50, -50, -40, -40])):
        collection = copy.deepcopy(ctx.collection)
        collection["id"] = f"{test_prefix}-{suffix}"
        collection["extent"] = {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [[None, None]]},
        }
        await create_collection(txn_client, collection)
    await refresh_indices(txn_client)

    bbox = [12.0, 12.0, 0.0, 14.0, 14.0, 100.0]
    for method, path, kwargs in (
        ("GET", "/collections", {"params": {"bbox": ",".join(map(str, bbox))}}),
        ("GET", "/collections-search", {"params": {"bbox": ",".join(map(str, bbox))}}),
        ("POST", "/collections-search", {"json": {"bbox": bbox}}),
    ):
        resp = await app_client.request(method, path, **kwargs)
        assert resp.status_code == 200, (method, path, resp.text)
        found = [
            c["id"]
            for c in resp.json()["collections"]
            if c["id"].startswith(test_prefix)
        ]
        assert found == [f"{test_prefix}-keep"], (method, path)
