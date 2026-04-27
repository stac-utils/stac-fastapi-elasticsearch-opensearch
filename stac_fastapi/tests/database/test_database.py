import importlib
import os
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from stac_pydantic import api

import stac_fastapi.sfeos_helpers.database.index as database_index_module
import stac_fastapi.sfeos_helpers.mappings as mappings_module
import stac_fastapi.sfeos_helpers.search_engine.index_operations as index_operations_module

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from opensearchpy.exceptions import RequestError as IndexingError

    from stac_fastapi.opensearch import database_logic as backend_database_logic_module

else:
    from elasticsearch.exceptions import BadRequestError as IndexingError
    from stac_fastapi.elasticsearch import (
        database_logic as backend_database_logic_module,
    )

from stac_fastapi.sfeos_helpers.database import (
    filter_indexes_by_datetime,
    filter_indexes_by_datetime_range,
    index_alias_by_collection_id,
)
from stac_fastapi.sfeos_helpers.filter.cql2 import resolve_cql2_indexes
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ITEM_INDICES,
)
from stac_fastapi.sfeos_helpers.search_engine.selection.selectors import (
    DatetimeBasedIndexSelector,
)

from ..conftest import (
    MockRequest,
    create_collection_index,
    create_index_templates,
    database,
)


@pytest_asyncio.fixture(scope="module", autouse=True)
async def setup_database_indexes():
    await create_collection_index()
    await create_index_templates()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cql2_filter", "collection_ids", "expected_metadata"),
    [
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "between",
                        "args": [
                            {"property": "datetime"},
                            {"timestamp": "2024-01-10T00:00:000000Z"},
                            {"timestamp": "2024-01-20T00:00:000000Z"},
                        ],
                    }
                ],
            },
            [],
            [
                ([], "../{'timestamp': '2024-01-10T00:00:000000Z'}"),
                ([], "{'timestamp': '2024-01-20T00:00:000000Z'}/.."),
            ],
            id="not-between-datetime",
        ),
        pytest.param(
            {
                "op": "or",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [{"property": "collection"}, "collection-1"],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    {"timestamp": "2024-01-01T00:00:000000Z"},
                                    {"timestamp": "2024-01-10T00:00:000000Z"},
                                ],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    {"timestamp": "2024-01-05T00:00:000000Z"},
                                    {"timestamp": "2024-01-15T00:00:000000Z"},
                                ],
                            },
                        ],
                    },
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [{"property": "collection"}, "collection-2"],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    {"timestamp": "2024-02-01T00:00:000000Z"},
                                    {"timestamp": "2024-02-03T00:00:000000Z"},
                                ],
                            },
                        ],
                    },
                ],
            },
            ["collection-1", "collection-2", "collection-3"],
            [
                (
                    ["collection-1"],
                    "{'timestamp': '2024-01-05T00:00:000000Z'}/{'timestamp': '2024-01-10T00:00:000000Z'}",
                ),
                (
                    ["collection-2"],
                    "{'timestamp': '2024-02-01T00:00:000000Z'}/{'timestamp': '2024-02-03T00:00:000000Z'}",
                ),
            ],
            id="or-nested-overlapping-datetime-ranges",
        ),
        pytest.param(
            {
                "op": "or",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-indexes-1",
                                ],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-08T23:59:59.999000Z",
                                    "2025-11-10T23:59:59.999000Z",
                                ],
                            },
                        ],
                    },
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-indexes-2",
                                ],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-11T23:59:59.998000Z",
                                    "2025-11-11T23:59:59.999900Z",
                                ],
                            },
                        ],
                    },
                ],
            },
            [
                "test-indexes-1",
                "test-indexes-2",
                "test-indexes-3",
            ],
            [
                (
                    ["test-indexes-1"],
                    "2025-11-08T23:59:59.999000Z/2025-11-10T23:59:59.999000Z",
                ),
                (
                    ["test-indexes-2"],
                    "2025-11-11T23:59:59.998000Z/2025-11-11T23:59:59.999900Z",
                ),
            ],
            id="or-for-different-collections-datetime",
        ),
        pytest.param(
            {
                "op": "and",
                "args": [
                    {
                        "op": "=",
                        "args": [
                            {"property": "collection"},
                            "collection-1",
                        ],
                    },
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-03-01T00:00:00Z",
                                    "2025-03-31T23:59:59Z",
                                ],
                            }
                        ],
                    },
                ],
            },
            ["collection-1", "collection-2", "collection-3"],
            [
                (["collection-1"], "../2025-03-01T00:00:00Z"),
                (["collection-1"], "2025-03-31T23:59:59Z/.."),
            ],
            id="not-between-collection-datetime",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": ">=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-03-01T00:00:00Z",
                                ],
                            },
                            {
                                "op": "<=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-03-31T23:59:59Z",
                                ],
                            },
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "collection-1",
                                ],
                            },
                        ],
                    }
                ],
            },
            ["collection-1", "collection-2", "collection-3"],
            [
                (["collection-2", "collection-3"], ""),
                (
                    ["collection-1", "collection-2", "collection-3"],
                    "../2025-03-01T00:00:00Z",
                ),
                (
                    ["collection-1", "collection-2", "collection-3"],
                    "2025-03-31T23:59:59Z/..",
                ),
            ],
            id="not-and-two-datetime-bounds-and-collection",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-03-01T00:00:00Z",
                                    "2025-03-31T23:59:59Z",
                                ],
                            },
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "collection-1",
                                ],
                            },
                        ],
                    }
                ],
            },
            ["collection-1", "collection-2", "collection-3"],
            [
                (["collection-2", "collection-3"], ""),
                (
                    ["collection-1", "collection-2", "collection-3"],
                    "../2025-03-01T00:00:00Z",
                ),
                (
                    ["collection-1", "collection-2", "collection-3"],
                    "2025-03-31T23:59:59Z/..",
                ),
            ],
            id="not-and-between-datetime-and-collection",
        ),
        pytest.param(
            {
                "op": "and",
                "args": [
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": "in",
                                "args": [
                                    {"property": "collection"},
                                    ["collection-a"],
                                ],
                            }
                        ],
                    },
                    {
                        "op": "between",
                        "args": [
                            {"property": "datetime"},
                            "2025-04-10T00:00:00Z",
                            "2025-04-20T00:00:00Z",
                        ],
                    },
                ],
            },
            ["collection-a", "collection-b"],
            [(["collection-b"], "2025-04-10T00:00:00Z/2025-04-20T00:00:00Z")],
            id="not-in-datetime-with-collection",
        ),
        pytest.param(
            {
                "op": "or",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "collection-1",
                                ],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-05-01T00:00:00Z",
                                    "2025-05-05T23:59:59Z",
                                ],
                            },
                        ],
                    },
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "collection-1",
                                ],
                            },
                            {
                                "op": "between",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-06-01T00:00:00Z",
                                    "2025-06-05T23:59:59Z",
                                ],
                            },
                        ],
                    },
                ],
            },
            ["collection-1", "collection-2"],
            [
                (["collection-1"], "2025-05-01T00:00:00Z/2025-05-05T23:59:59Z"),
                (["collection-1"], "2025-06-01T00:00:00Z/2025-06-05T23:59:59Z"),
            ],
            id="same-collection-disjoint-or-ranges",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "and",
                        "args": [
                            {
                                "op": ">=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-03T23:59:59.999000Z",
                                ],
                            },
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-collection-indexes-1",
                                ],
                            },
                        ],
                    }
                ],
            },
            [
                "test-collection-indexes-1",
                "test-collection-indexes-3",
                "test-collection-indexes-5",
            ],
            [
                (
                    [
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "",
                ),
                (
                    [
                        "test-collection-indexes-1",
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "../2025-11-03T23:59:59.999000Z",
                ),
            ],
            id="not-and-datetime-collection",
        ),
        pytest.param(
            {
                "op": "or",
                "args": [
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": ">=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-03T23:59:59.999000Z",
                                ],
                            }
                        ],
                    },
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-collection-indexes-1",
                                ],
                            }
                        ],
                    },
                ],
            },
            [
                "test-collection-indexes-1",
                "test-collection-indexes-3",
                "test-collection-indexes-5",
            ],
            [
                (
                    [
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "",
                ),
                (
                    [
                        "test-collection-indexes-1",
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "../2025-11-03T23:59:59.999000Z",
                ),
            ],
            id="or-not-datetime-not-collection",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "or",
                        "args": [
                            {
                                "op": ">=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-03T23:59:59.999000Z",
                                ],
                            },
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-collection-indexes-1",
                                ],
                            },
                        ],
                    }
                ],
            },
            [
                "test-collection-indexes-1",
                "test-collection-indexes-3",
                "test-collection-indexes-5",
            ],
            [
                (
                    [
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "../2025-11-03T23:59:59.999000Z",
                )
            ],
            id="not-or-datetime-collection",
        ),
        pytest.param(
            {
                "op": "and",
                "args": [
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": ">=",
                                "args": [
                                    {"property": "datetime"},
                                    "2025-11-03T23:59:59.999000Z",
                                ],
                            }
                        ],
                    },
                    {
                        "op": "not",
                        "args": [
                            {
                                "op": "=",
                                "args": [
                                    {"property": "collection"},
                                    "test-collection-indexes-1",
                                ],
                            }
                        ],
                    },
                ],
            },
            [
                "test-collection-indexes-1",
                "test-collection-indexes-3",
                "test-collection-indexes-5",
            ],
            [
                (
                    [
                        "test-collection-indexes-3",
                        "test-collection-indexes-5",
                    ],
                    "../2025-11-03T23:59:59.999000Z",
                )
            ],
            id="and-not-datetime-and-not-collection",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "in",
                        "args": [
                            {"property": "collection"},
                            ["collection-1"],
                        ],
                    }
                ],
            },
            ["collection-1", "collection-2", "collection-3"],
            [(["collection-2", "collection-3"], "")],
            id="not-in-collection-empty-result",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "in",
                        "args": [
                            {"property": "collection"},
                            ["collection-a"],
                        ],
                    }
                ],
            },
            ["collection-a"],
            [],
            id="empty-collections",
        ),
        pytest.param(
            {
                "op": ">=",
                "args": [
                    {"property": "datetime"},
                    "2022-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "2022-01-01T00:00:00Z/..")],
            id="datetime-gte",
        ),
        pytest.param(
            {
                "op": "<=",
                "args": [
                    {"property": "datetime"},
                    "2022-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "../2022-01-01T00:00:00Z")],
            id="datetime-lte",
        ),
        pytest.param(
            {
                "op": ">",
                "args": [
                    {"property": "datetime"},
                    "2022-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "2022-01-01T00:00:00Z/..")],
            id="datetime-gt",
        ),
        pytest.param(
            {
                "op": "<",
                "args": [
                    {"property": "datetime"},
                    "2022-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "../2022-01-01T00:00:00Z")],
            id="datetime-lt",
        ),
        pytest.param(
            {
                "op": "<",
                "args": [
                    {"property": "datetime"},
                    "2022-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "../2022-01-01T00:00:00Z")],
            id="datetime-open-lt",
        ),
        pytest.param(
            {
                "op": ">",
                "args": [
                    {"property": "datetime"},
                    "2020-01-01T00:00:00Z",
                ],
            },
            [],
            [([], "2020-01-01T00:00:00Z/..")],
            id="datetime-open-gt",
        ),
        pytest.param(
            {
                "op": "<>",
                "args": [{"property": "datetime"}, "2024-01-15T00:00:00Z"],
            },
            [],
            [([], "../2024-01-15T00:00:00Z"), ([], "2024-01-15T00:00:00Z/..")],
            id="datetime-not-equal",
        ),
        pytest.param(
            {
                "op": "isNull",
                "args": [{"property": "datetime"}],
            },
            [],
            [],
            id="isnull-datetime",
        ),
        pytest.param(
            {
                "op": "not",
                "args": [
                    {
                        "op": "in",
                        "args": [
                            {"property": "datetime"},
                            ["2024-01-01T00:00:00Z", "2024-01-15T00:00:00Z"],
                        ],
                    }
                ],
            },
            [],
            [
                ([], "../2024-01-01T00:00:00Z"),
                ([], "2024-01-01T00:00:00Z/2024-01-15T00:00:00Z"),
                ([], "2024-01-15T00:00:00Z/.."),
            ],
            id="datetime-not-in",
        ),
        pytest.param(
            {
                "op": ">=",
                "args": [{"property": "start_datetime"}, "2023-01-01T00:00:00Z"],
            },
            [],
            [([], "2023-01-01T00:00:00Z/..")],
            id="start_datetime-gte",
        ),
        pytest.param(
            {
                "op": "<=",
                "args": [{"property": "start_datetime"}, "2023-12-31T23:59:59Z"],
            },
            [],
            [([], "../2023-12-31T23:59:59Z")],
            id="start_datetime-lte",
        ),
        pytest.param(
            {
                "op": "between",
                "args": [
                    {"property": "start_datetime"},
                    "2023-06-01T00:00:00Z",
                    "2023-06-30T23:59:59Z",
                ],
            },
            [],
            [([], "2023-06-01T00:00:00Z/2023-06-30T23:59:59Z")],
            id="start_datetime-between",
        ),
        pytest.param(
            {
                "op": ">=",
                "args": [{"property": "end_datetime"}, "2023-01-01T00:00:00Z"],
            },
            [],
            [([], "2023-01-01T00:00:00Z/..")],
            id="end_datetime-gte",
        ),
        pytest.param(
            {
                "op": "<=",
                "args": [{"property": "end_datetime"}, "2023-12-31T23:59:59Z"],
            },
            [],
            [([], "../2023-12-31T23:59:59Z")],
            id="end_datetime-lte",
        ),
        pytest.param(
            {
                "op": "between",
                "args": [
                    {"property": "end_datetime"},
                    "2023-06-01T00:00:00Z",
                    "2023-06-30T23:59:59Z",
                ],
            },
            [],
            [([], "2023-06-01T00:00:00Z/2023-06-30T23:59:59Z")],
            id="end_datetime-between",
        ),
        pytest.param(
            {
                "op": "and",
                "args": [
                    {
                        "op": ">=",
                        "args": [
                            {"property": "start_datetime"},
                            "2023-01-01T00:00:00Z",
                        ],
                    },
                    {
                        "op": "<=",
                        "args": [{"property": "end_datetime"}, "2023-12-31T23:59:59Z"],
                    },
                ],
            },
            [],
            [([], "2023-01-01T00:00:00Z/.."), ([], "../2023-12-31T23:59:59Z")],
            id="start-and-end-datetime-combined",
        ),
    ],
)
async def test_apply_cql2_filter_checks_search_and_metadata(
    monkeypatch,
    cql2_filter,
    collection_ids,
    expected_metadata,
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    queryables_mapping = {
        "collection": ["collection", "collection.keyword"],
        "datetime": "properties.datetime",
        "start_datetime": "properties.start_datetime",
        "end_datetime": "properties.end_datetime",
    }
    queryables_mapping_mock = AsyncMock(return_value=queryables_mapping)
    collection_ids_mock = AsyncMock(return_value=collection_ids)
    monkeypatch.setattr(database, "get_queryables_mapping", queryables_mapping_mock)
    monkeypatch.setattr(
        database.async_index_selector,
        "get_all_collection_ids",
        collection_ids_mock,
    )
    search, metadata = await database.apply_cql2_filter(
        database.make_search(),
        cql2_filter,
    )
    assert search.to_dict().get("query") is not None
    assert metadata == expected_metadata
    queryables_mapping_mock.assert_awaited_once()
    collection_ids_mock.assert_awaited_once()


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata", "expected_index_param", "expected_collection_ids"),
    [
        pytest.param(
            [
                (["col-a"], "2020-02-01T00:00:00Z/2020-02-28T23:59:59Z"),
                (["col-b"], "2020-02-01T00:00:00Z/2020-02-28T23:59:59Z"),
                (["col-a"], "2020-02-01T00:00:00Z/2020-02-28T23:59:59Z"),
            ],
            "items_start_datetime_col-a_2020-02-08-2020-02-09,items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-b_2020-02-15",
            {"col-a", "col-b"},
            id="test-collections-and-datetime",
        ),
        pytest.param(
            [(["col-a"], "")],
            "items_start_datetime_col-a_2020-02-08-2020-02-09,items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-a_2020-06-10",
            {"col-a"},
            id="test-only-collection",
        ),
        pytest.param(
            [([], "2020-02-15T00:00:00Z")],
            "items_start_datetime_col-a_2020-02-08-2020-02-09,items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-b_2020-02-15",
            set(),
            id="test-only-datetime",
        ),
        pytest.param(
            [([], "../2020-02-20T23:59:59Z")],
            "items_start_datetime_col-a_2020-02-08-2020-02-09,items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-b_2020-02-15",
            set(),
            id="test-open-start-datetime",
        ),
        pytest.param(
            [([], "2020-06-01T00:00:00Z/..")],
            "items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-a_2020-06-10",
            set(),
            id="test-open-end-datetime",
        ),
        pytest.param(
            [([], "2020-02-01T00:00:00Z/2020-02-28T23:59:59Z")],
            "items_start_datetime_col-a_2020-02-08-2020-02-09,items_start_datetime_col-a_2020-02-10-2020-06-09,items_start_datetime_col-b_2020-02-15",
            set(),
            id="test-bounded-datetime",
        ),
        pytest.param(
            [([], "2019-01-01T00:00:00Z/2019-12-31T23:59:59Z")],
            "",
            set(),
            id="test-disjoint-range-before-all-indexes",
        ),
        pytest.param(
            [([], "2021-01-01T00:00:00Z/2021-12-31T23:59:59Z")],
            "",
            set(),
            id="test-disjoint-range-after-all-indexes",
        ),
    ],
)
async def test_resolve_cql2_indexes_with_collections_datetime(
    monkeypatch,
    metadata,
    expected_index_param,
    expected_collection_ids,
):
    if not os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip()

    index_selector = _make_selector(monkeypatch)
    search = database.make_search()

    index_param, collection_ids = await resolve_cql2_indexes(
        metadata,
        index_selector,
        database.apply_datetime_filter,
        search,
    )
    assert index_param == expected_index_param
    assert set(collection_ids) == expected_collection_ids


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


@pytest.mark.asyncio
async def test_item_add_rejects_coerce_false(txn_client, load_test_data, monkeypatch):
    """Test that item with type mismatch is rejected when coerce is disabled."""

    collection = load_test_data("test_collection.json")
    collection["id"] = str(uuid.uuid4())

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["collection"] = collection["id"]
    item["properties"]["sat:absolute_orbit"] = "12345"

    try:
        with monkeypatch.context() as context:
            context.setenv("STAC_FASTAPI_ES_COERCE_GLOBAL", "false")

            importlib.reload(mappings_module)
            importlib.reload(database_index_module)
            importlib.reload(index_operations_module)

            await backend_database_logic_module.create_index_templates()

            await txn_client.create_collection(
                api.Collection(**collection), request=MockRequest
            )
            await database.async_index_inserter.create_simple_index(
                database.client, collection["id"]
            )

            index_kwargs = {
                "index": index_alias_by_collection_id(collection["id"]),
                "id": f"{item['id']}|{collection['id']}",
                "refresh": True,
            }

            if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
                index_kwargs["body"] = item
            else:
                index_kwargs["document"] = item

            with pytest.raises(IndexingError):
                await database.client.index(**index_kwargs)

            await txn_client.delete_collection(collection["id"])
    finally:
        importlib.reload(mappings_module)
        importlib.reload(database_index_module)
        importlib.reload(index_operations_module)


@pytest.mark.asyncio
async def test_item_add_accepted_coerce_true(txn_client, load_test_data, monkeypatch):
    """Test that item with type mismatch is accepted coerce is enabled."""

    collection = load_test_data("test_collection.json")
    collection["id"] = str(uuid.uuid4())

    item = load_test_data("test_item.json")
    item["id"] = str(uuid.uuid4())
    item["collection"] = collection["id"]
    item["properties"]["sat:absolute_orbit"] = "12345"

    try:
        with monkeypatch.context() as context:
            context.setenv("STAC_FASTAPI_ES_COERCE_GLOBAL", "true")

            importlib.reload(mappings_module)
            importlib.reload(database_index_module)
            importlib.reload(index_operations_module)

            await backend_database_logic_module.create_index_templates()

            await txn_client.create_collection(
                api.Collection(**collection), request=MockRequest
            )
            await database.async_index_inserter.create_simple_index(
                database.client, collection["id"]
            )

            index_kwargs = {
                "index": index_alias_by_collection_id(collection["id"]),
                "id": f"{item['id']}|{collection['id']}",
                "refresh": True,
            }

            if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
                index_kwargs["body"] = item
            else:
                index_kwargs["document"] = item

            await database.client.index(**index_kwargs)

            get_response = await database.client.get(
                index=index_alias_by_collection_id(collection["id"]),
                id=f"{item['id']}|{collection['id']}",
            )
            if hasattr(get_response, "body"):
                doc = get_response.body["_source"]
            else:
                doc = get_response["_source"]

            assert doc["properties"]["sat:absolute_orbit"] == "12345"

            await txn_client.delete_collection(collection["id"])

    finally:
        importlib.reload(mappings_module)
        importlib.reload(database_index_module)
        importlib.reload(index_operations_module)


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
        "end_datetime": {"gte": None, "lte": "2020-02-07T23:59:59Z"},
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


THREE_INDEXES = [
    (
        {
            "end_datetime": "items_end_datetime_col_2025-11-07",
            "start_datetime": "items_start_datetime_col_2025-11-01-2025-11-05",
        },
    ),
    (
        {
            "end_datetime": "items_end_datetime_col_2025-11-12",
            "start_datetime": "items_start_datetime_col_2025-11-06-2025-11-10",
        },
    ),
    (
        {
            "end_datetime": "items_end_datetime_col_2025-11-16",
            "start_datetime": "items_start_datetime_col_2025-11-11",
        },
    ),
]


def _range_search(gte=None, lte=None):
    return {
        "datetime": {"gte": None, "lte": None},
        "start_datetime": {"gte": gte, "lte": None},
        "end_datetime": {"gte": None, "lte": lte},
    }


@pytest.mark.datetime_filtering
def test_range_intersects_first_two_indexes():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-11-07T23:59:59.999Z", "2025-11-07T23:59:59.999Z"),
    )
    assert len(result) == 2
    assert "items_start_datetime_col_2025-11-01-2025-11-05" in result
    assert "items_start_datetime_col_2025-11-06-2025-11-10" in result


@pytest.mark.datetime_filtering
def test_range_intersects_all_indexes():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-11-01T00:00:00Z", "2025-11-30T23:59:59Z"),
    )
    assert len(result) == 3


@pytest.mark.datetime_filtering
def test_range_intersects_none():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-10-01T00:00:00Z", "2025-10-15T23:59:59Z"),
    )
    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_range_intersects_last_index_only():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-11-15T00:00:00Z", "2025-11-20T23:59:59Z"),
    )
    assert len(result) == 1
    assert "items_start_datetime_col_2025-11-11" in result


@pytest.mark.datetime_filtering
def test_range_after_all_indexes():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-12-01T00:00:00Z", "2025-12-31T23:59:59Z"),
    )
    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_range_no_filters_returns_all():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search(),
    )
    assert len(result) == 3


@pytest.mark.datetime_filtering
def test_range_only_gte():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-11-10T00:00:00Z", None),
    )
    assert len(result) == 2
    assert "items_start_datetime_col_2025-11-06-2025-11-10" in result
    assert "items_start_datetime_col_2025-11-11" in result


@pytest.mark.datetime_filtering
def test_range_only_lte():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search(None, "2025-11-08T23:59:59Z"),
    )
    assert len(result) == 2
    assert "items_start_datetime_col_2025-11-01-2025-11-05" in result
    assert "items_start_datetime_col_2025-11-06-2025-11-10" in result


@pytest.mark.datetime_filtering
def test_range_exact_boundary_same_day():
    result = filter_indexes_by_datetime_range(
        THREE_INDEXES,
        _range_search("2025-11-07T23:59:59.999Z", "2025-11-07T23:59:59.999Z"),
    )
    assert "items_start_datetime_col_2025-11-01-2025-11-05" in result


@pytest.mark.datetime_filtering
def test_range_empty_collection():
    result = filter_indexes_by_datetime_range(
        [],
        _range_search("2025-11-07T00:00:00Z", "2025-11-07T23:59:59Z"),
    )
    assert len(result) == 0


@pytest.mark.datetime_filtering
def test_range_index_without_end_datetime():
    collection_indexes = [
        (
            {
                "start_datetime": "items_start_datetime_col_2025-11-05",
            },
        )
    ]
    result = filter_indexes_by_datetime_range(
        collection_indexes,
        _range_search("2025-11-10T00:00:00Z", "2025-11-15T23:59:59Z"),
    )
    assert len(result) == 1


@pytest.mark.datetime_filtering
def test_range_index_without_start_datetime_skipped():
    collection_indexes = [
        (
            {
                "end_datetime": "items_end_datetime_col_2025-11-07",
            },
        )
    ]
    result = filter_indexes_by_datetime_range(
        collection_indexes,
        _range_search("2025-11-01T00:00:00Z", "2025-11-10T23:59:59Z"),
    )
    assert len(result) == 0


SELECTOR_ALIASES = {
    "items_col-a": [
        (
            {
                "start_datetime": "items_start_datetime_col-a_2020-02-08-2020-02-09",
                "end_datetime": "items_end_datetime_col-a_2020-02-16",
            },
        ),
        (
            {
                "start_datetime": "items_start_datetime_col-a_2020-02-10-2020-06-09",
                "end_datetime": "items_end_datetime_col-a_2020-06-18",
            },
        ),
        (
            {
                "start_datetime": "items_start_datetime_col-a_2020-06-10",
                "end_datetime": "items_end_datetime_col-a_2020-06-20",
            },
        ),
    ],
    "items_col-b": [
        (
            {
                "start_datetime": "items_start_datetime_col-b_2020-02-15",
                "end_datetime": "items_end_datetime_col-b_2020-02-25",
            },
        ),
    ],
}


def _make_selector(monkeypatch):
    """Create a DatetimeBasedIndexSelector with mocked alias_loader."""
    monkeypatch.setenv("USE_DATETIME", "false")
    DatetimeBasedIndexSelector._instance = None
    with patch.object(DatetimeBasedIndexSelector, "__init__", lambda self, c: None):
        sel = DatetimeBasedIndexSelector.__new__(DatetimeBasedIndexSelector, None)
    sel.alias_loader = AsyncMock()
    sel.alias_loader.get_aliases = AsyncMock(return_value=SELECTOR_ALIASES)
    sel.alias_loader.get_collection_indexes = AsyncMock(
        side_effect=lambda cid, **kw: SELECTOR_ALIASES.get(f"items_{cid}", [])
    )
    return sel


@pytest.mark.datetime_filtering
def test_has_datetime_values_with_gte():
    assert DatetimeBasedIndexSelector._has_datetime_values(
        {"gte": "2020-01-01T00:00:00Z", "lte": None}
    )


@pytest.mark.datetime_filtering
def test_has_datetime_values_with_lte():
    assert DatetimeBasedIndexSelector._has_datetime_values(
        {"gte": None, "lte": "2020-12-31T00:00:00Z"}
    )


@pytest.mark.datetime_filtering
def test_has_datetime_values_both_none():
    assert not DatetimeBasedIndexSelector._has_datetime_values(
        {"gte": None, "lte": None}
    )


@pytest.mark.datetime_filtering
def test_has_datetime_values_empty_dict():
    assert not DatetimeBasedIndexSelector._has_datetime_values({})


@pytest.mark.datetime_filtering
def test_has_datetime_values_none():
    assert not DatetimeBasedIndexSelector._has_datetime_values(None)


@pytest.mark.datetime_filtering
def test_has_datetime_values_string():
    assert DatetimeBasedIndexSelector._has_datetime_values("2020-01-01T00:00:00Z")


@pytest.mark.datetime_filtering
def test_has_datetime_values_empty_string():
    assert not DatetimeBasedIndexSelector._has_datetime_values("")


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_with_collections_and_datetime(monkeypatch):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        ["col-a"],
        {"gte": "2020-02-08T00:00:00Z", "lte": "2020-02-09T23:59:59Z"},
    )

    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-06-10" not in result
    sel.alias_loader.get_aliases.assert_not_awaited()


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_with_datetime(monkeypatch):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2020-02-10T00:00:00Z", "lte": "2020-02-14T23:59:59Z"},
    )

    sel.alias_loader.get_aliases.assert_awaited_once()
    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-a_2020-06-10" not in result
    assert "items_start_datetime_col-b_2020-02-15" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_empty_collections_with_datetime(monkeypatch):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        [],
        {"gte": "2020-02-10T00:00:00Z", "lte": "2020-02-14T23:59:59Z"},
    )

    sel.alias_loader.get_aliases.assert_awaited_once()
    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-b_2020-02-15" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_no_datetime(monkeypatch):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": None, "lte": None},
    )

    assert result == ITEM_INDICES
    sel.alias_loader.get_aliases.assert_not_awaited()


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_matches_returns_empty(monkeypatch):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        ["col-a"],
        {"gte": "2026-01-01T00:00:00Z", "lte": "2026-01-31T23:59:59Z"},
    )

    assert result == ""


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_collections_no_datetime_returns_all_for_collection(
    monkeypatch,
):
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        ["col-a"],
        {"gte": None, "lte": None},
    )

    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-a_2020-06-10" in result
    assert "col-b" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_only_gte(monkeypatch):
    """No collections + only gte → returns all indexes starting from that date."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2020-06-01T00:00:00Z", "lte": None},
    )

    assert "items_start_datetime_col-a_2020-06-10" in result
    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" not in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-b_2020-02-15" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_only_lte(monkeypatch):
    """No collections + only lte → returns all indexes up to that date."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": None, "lte": "2020-02-14T23:59:59Z"},
    )

    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-b_2020-02-15" not in result
    assert "items_start_datetime_col-a_2020-06-10" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_wide_range_returns_all(monkeypatch):
    """No collections + wide datetime range → returns all indexes."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2020-01-01T00:00:00Z", "lte": "2020-12-31T23:59:59Z"},
    )

    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-a_2020-06-10" in result
    assert "items_start_datetime_col-b_2020-02-15" in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_narrow_range_single_match(monkeypatch):
    """No collections + narrow range → only one index matches."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2020-06-10T00:00:00Z", "lte": "2020-06-15T23:59:59Z"},
    )

    assert "items_start_datetime_col-a_2020-06-10" in result
    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" not in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-b_2020-02-15" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_no_collections_no_matches(monkeypatch):
    """No collections + datetime outside all ranges → empty string."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2026-01-01T00:00:00Z", "lte": "2026-12-31T23:59:59Z"},
    )

    assert result == ""


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_multiple_collections_datetime(monkeypatch):
    """Multiple collections + datetime → filters each collection independently."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        ["col-a", "col-b"],
        {"gte": "2020-02-20T00:00:00Z", "lte": "2020-02-22T23:59:59Z"},
    )

    assert "items_start_datetime_col-b_2020-02-15" in result
    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" not in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" in result
    assert "items_start_datetime_col-a_2020-06-10" not in result


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_select_indexes_boundary_date_match(monkeypatch):
    """Datetime range touching index boundary should match."""
    sel = _make_selector(monkeypatch)
    result = await sel.select_indexes(
        None,
        {"gte": "2020-02-09T00:00:00Z", "lte": "2020-02-09T23:59:59Z"},
    )

    assert "items_start_datetime_col-a_2020-02-08-2020-02-09" in result
    assert "items_start_datetime_col-a_2020-02-10-2020-06-09" not in result
    assert "items_start_datetime_col-b_2020-02-15" not in result
