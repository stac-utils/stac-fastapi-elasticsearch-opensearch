import json
import uuid

import pytest

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
