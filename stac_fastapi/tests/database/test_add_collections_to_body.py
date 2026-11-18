"""`add_collections_to_body` keeps the caller's query required (#1029)."""

import pytest

from stac_fastapi.sfeos_helpers.database.query import add_collections_to_body

IDS = ["A", "B"]
COLLECTION_FILTER = {"terms": {"collection": IDS}}
OR_QUERY = {
    "bool": {
        "should": [
            {"term": {"collection": "A"}},
            {"term": {"collection": "B"}},
        ]
    }
}
LEAF_QUERY = {"term": {"collection": "A"}}


@pytest.mark.parametrize("query", [OR_QUERY, LEAF_QUERY], ids=["should", "leaf"])
def test_wraps_query_as_required_clause(query):
    assert add_collections_to_body(IDS, query) == {
        "bool": {"must": [query], "filter": [COLLECTION_FILTER]}
    }


def test_filters_only_collections_without_query():
    assert add_collections_to_body(IDS, None) == {
        "bool": {"filter": [COLLECTION_FILTER]}
    }


@pytest.mark.parametrize("collection_ids", [None, []])
def test_returns_query_unchanged_without_collection_ids(collection_ids):
    assert add_collections_to_body(collection_ids, OR_QUERY) is OR_QUERY
