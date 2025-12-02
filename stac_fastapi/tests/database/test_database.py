import os
import uuid

import pytest
from stac_pydantic import api

from stac_fastapi.sfeos_helpers.database import (
    filter_indexes_by_datetime,
    index_alias_by_collection_id,
)
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
)

from ..conftest import MockRequest, database


@pytest.mark.asyncio
async def test_index_mapping_collections(ctx):
    response = await database.client.indices.get_mapping(index=COLLECTIONS_INDEX)
    if not isinstance(response, dict):
        response = response.body
    actual_mappings = next(iter(response.values()))["mappings"]
    assert (
        actual_mappings["dynamic_templates"]
        == ES_COLLECTIONS_MAPPINGS["dynamic_templates"]
    )


@pytest.mark.asyncio
async def test_index_mapping_items(txn_client, load_test_data):
    if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    collection = load_test_data("test_collection.json")
    collection["id"] = str(uuid.uuid4())
    await txn_client.create_collection(
        api.Collection(**collection), request=MockRequest
    )
    response = await database.client.indices.get_mapping(
        index=index_alias_by_collection_id(collection["id"])
    )
    if not isinstance(response, dict):
        response = response.body
    actual_mappings = next(iter(response.values()))["mappings"]
    assert (
        actual_mappings["dynamic_templates"] == ES_ITEMS_MAPPINGS["dynamic_templates"]
    )
    await txn_client.delete_collection(collection["id"])


@pytest.mark.datetime_filtering
def test_filter_datetime_field_outside_range():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": "2021-01-01T00:00:00Z", "lte": "2021-12-31T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_start_datetime_field_with_gte():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": "2020-02-01T00:00:00Z", "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1


@pytest.mark.datetime_filtering
def test_filter_end_datetime_field_with_lte():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": "2020-02-28T23:59:59Z"},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1


@pytest.mark.datetime_filtering
def test_filter_all_criteria_matching():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": "2020-02-01T00:00:00Z", "lte": "2020-02-28T23:59:59Z"},
        "start_datetime": {"gte": "2020-02-01T00:00:00Z", "lte": None},
        "end_datetime": {"gte": None, "lte": "2020-02-28T23:59:59Z"},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1


@pytest.mark.datetime_filtering
def test_filter_datetime_field_fails_gte():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": "2020-02-15T00:00:00Z", "lte": "2020-02-28T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_datetime_field_fails_lte():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": "2020-01-01T00:00:00Z", "lte": "2020-02-10T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_start_datetime_range_format(mock_datetime_env):
    collection_indexes = [
        (
            {
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08-2022-04-05",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": "2020-02-01T00:00:00Z", "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1
    assert result[0] == "items_start_datetime_new-collection_2020-02-08-2022-04-05"


@pytest.mark.datetime_filtering
def test_filter_start_datetime_range_fails_gte():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08-2022-04-05",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": "2022-05-01T00:00:00Z", "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_multiple_indexes_mixed_results():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
            },
        ),
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-15",
            },
        ),
        (
            {
                "datetime": "items_datetime_new-collection_2021-03-15",
            },
        ),
    ]
    datetime_search = {
        "datetime": {"gte": "2020-02-01T00:00:00Z", "lte": "2020-02-28T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, True)

    assert len(result) == 2
    assert "items_datetime_new-collection_2020-02-12" in result
    assert "items_datetime_new-collection_2020-02-15" in result


@pytest.mark.datetime_filtering
def test_filter_empty_collection():
    collection_indexes = []
    datetime_search = {
        "datetime": {"gte": "2020-02-01T00:00:00Z", "lte": "2020-02-28T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_all_criteria_none():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1


@pytest.mark.datetime_filtering
def test_filter_end_datetime_outside_range():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": "2020-02-10T23:59:59Z"},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_filter_complex_mixed_criteria():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-16",
                "start_datetime": "items_start_datetime_new-collection_2020-02-08",
            },
        ),
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-14",
                "end_datetime": "items_end_datetime_new-collection_2020-02-18",
                "start_datetime": "items_start_datetime_new-collection_2020-02-10",
            },
        ),
    ]
    datetime_search = {
        "datetime": {"gte": "2020-02-12T00:00:00Z", "lte": "2020-02-28T23:59:59Z"},
        "start_datetime": {"gte": "2020-02-01T00:00:00Z", "lte": None},
        "end_datetime": {"gte": None, "lte": "2020-02-20T23:59:59Z"},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 2


@pytest.mark.datetime_filtering
def test_filter_with_single_date_range():
    collection_indexes = [
        (
            {
                "datetime": "items_datetime_new-collection_2020-02-12",
                "end_datetime": "items_end_datetime_new-collection_2020-02-12",
                "start_datetime": "items_start_datetime_new-collection_2020-02-12",
            },
        )
    ]
    datetime_search = {
        "datetime": {"gte": "2020-02-12T00:00:00Z", "lte": "2020-02-12T23:59:59Z"},
        "start_datetime": {"gte": None, "lte": None},
        "end_datetime": {"gte": None, "lte": None},
    }

    result = filter_indexes_by_datetime(collection_indexes, datetime_search, False)

    assert len(result) == 1
