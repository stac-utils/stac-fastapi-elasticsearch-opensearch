"""Items dated before 1970 or after 2262 with USE_DATETIME_NANOS=false (#967, #968)."""

import uuid
from copy import deepcopy

import pytest
import pytest_asyncio

from stac_fastapi.core.utilities import get_bool_env

from ..conftest import use_datetime_nanos

ALL_IDS = {
    "dt-1900",
    "dt-1950",
    "dt-1969",
    "dt-1970",
    "dt-2300",
    "range-1961-1990",
    "dt-2000-only",
}


def _dated_item(base: dict, item_id: str, dt: str | None, start=None, end=None):
    item = deepcopy(base)
    item["id"] = item_id
    props = item["properties"]
    props["datetime"] = dt
    props.pop("start_datetime", None)
    props.pop("end_datetime", None)
    if start:
        props["start_datetime"] = start
        props["end_datetime"] = end
    return item


@pytest.fixture(params=["true", "false"], ids=["use_datetime", "start_end_only"])
def use_datetime(request, monkeypatch):
    monkeypatch.setenv("USE_DATETIME", request.param)
    return request.param == "true"


async def _seed(app_client, load_test_data, exclude: str | None = None) -> str:
    collection = load_test_data("test_collection.json")
    collection["id"] = f"dt-range-{uuid.uuid4().hex[:8]}"
    resp = await app_client.post("/collections", json=collection)
    assert resp.status_code == 201, resp.text

    base = load_test_data("test_item.json")
    base["collection"] = collection["id"]
    # Instant items carry start = end = datetime so both USE_DATETIME modes match them.
    instants = {
        "dt-1900": "1900-01-01T00:00:00Z",
        "dt-1950": "1950-06-15T00:00:00Z",
        "dt-1969": "1969-12-31T23:59:59.999Z",
        "dt-1970": "1970-01-01T00:00:00Z",
        "dt-2300": "2300-01-01T00:00:00Z",
    }
    items = [_dated_item(base, i, dt, dt, dt) for i, dt in instants.items()]
    items.append(
        _dated_item(
            base,
            "range-1961-1990",
            None,
            "1961-01-01T00:00:00Z",
            "1990-12-31T23:59:59Z",
        )
    )
    items.append(_dated_item(base, "dt-2000-only", "2000-01-01T00:00:00Z"))
    for item in items:
        if item["id"] == exclude:
            continue
        resp = await app_client.post(
            f"/collections/{collection['id']}/items", json=item
        )
        assert resp.status_code == 201, resp.text
    return collection["id"]


@pytest_asyncio.fixture()
async def seeded(datetime_nanos_off, use_datetime, app_client, load_test_data):
    if get_bool_env("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("seeds item shapes that index filtering rejects in one mode")
    return await _seed(app_client, load_test_data)


@pytest_asyncio.fixture()
async def seeded_with_index_filtering(
    datetime_nanos_off, use_datetime, datetime_filtering_env, app_client, load_test_data
):
    """Seed through datetime index filtering, which needs a datetime with USE_DATETIME=true and start/end otherwise."""
    unsupported = "range-1961-1990" if use_datetime else "dt-2000-only"
    return await _seed(app_client, load_test_data, exclude=unsupported), unsupported


def _ids(resp) -> set[str]:
    assert resp.status_code == 200, resp.text
    return {f["id"] for f in resp.json()["features"]}


async def _all_pages(app_client, url: str) -> list[str]:
    ids = []
    while url:
        resp = await app_client.get(url)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        ids += [f["id"] for f in body["features"]]
        url = next((li["href"] for li in body["links"] if li["rel"] == "next"), None)
    return ids


@pytest.mark.asyncio
async def test_search_without_datetime_returns_every_item(app_client, seeded):
    cid = seeded
    assert (
        _ids(await app_client.post("/search", json={"collections": [cid]})) == ALL_IDS
    )
    assert _ids(await app_client.get(f"/search?collections={cid}")) == ALL_IDS
    assert _ids(await app_client.get(f"/collections/{cid}/items")) == ALL_IDS


INTERVALS = [
    ("1950-06-15T00:00:00Z", {"dt-1950"}),
    ("1950-01-01T00:00:00Z/1965-01-01T00:00:00Z", {"dt-1950", "range-1961-1990"}),
    ("../1960-01-01T00:00:00Z", {"dt-1900", "dt-1950"}),
    (
        "1960-01-01T00:00:00Z/..",
        {"dt-1969", "dt-1970", "dt-2300", "range-1961-1990", "dt-2000-only"},
    ),
    ("2200-01-01T00:00:00Z/2400-01-01T00:00:00Z", {"dt-2300"}),
]


async def _assert_datetime_filter(app_client, cid, interval, expected):
    post = await app_client.post(
        "/search", json={"collections": [cid], "datetime": interval}
    )
    assert _ids(post) == expected
    get = await app_client.get(
        "/search", params={"collections": cid, "datetime": interval}
    )
    assert _ids(get) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("interval,expected", INTERVALS)
async def test_datetime_filter_matches_out_of_nanos_range(
    app_client, seeded, use_datetime, interval, expected
):
    if not use_datetime:
        expected = expected - {"dt-2000-only"}
    await _assert_datetime_filter(app_client, seeded, interval, expected)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("interval,expected", INTERVALS)
async def test_datetime_filter_with_index_filtering_matches_out_of_nanos_range(
    app_client, seeded_with_index_filtering, use_datetime, interval, expected
):
    cid, unsupported = seeded_with_index_filtering
    expected = expected - {unsupported}
    if not use_datetime:
        expected = expected - {"dt-2000-only"}
    await _assert_datetime_filter(app_client, cid, interval, expected)
    assert _ids(await app_client.get(f"/search?collections={cid}")) == ALL_IDS - {
        unsupported
    }


@pytest.mark.asyncio
async def test_double_open_interval_is_rejected(app_client, seeded):
    resp = await app_client.post(
        "/search", json={"collections": [seeded], "datetime": "../.."}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "<",
                "args": [
                    {"property": "datetime"},
                    {"timestamp": "1960-01-01T00:00:00Z"},
                ],
            },
        },
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "between",
                "args": [
                    {"property": "datetime"},
                    "1800-01-01T00:00:00Z",
                    "1960-01-01T00:00:00Z",
                ],
            },
        },
        {"query": {"datetime": {"lt": "1960-01-01T00:00:00Z"}}},
    ],
    ids=["cql2-lt-timestamp", "cql2-between", "query-lt"],
)
async def test_property_filters_match_pre_epoch_items(app_client, seeded, body):
    resp = await app_client.post("/search", json={"collections": [seeded], **body})
    assert _ids(resp) == {"dt-1900", "dt-1950"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sortby,expected",
    [
        (
            "+properties.datetime",
            ["dt-1900", "dt-1950", "dt-1969", "dt-1970", "dt-2000-only", "dt-2300"]
            + ["range-1961-1990"],
        ),
        (
            "-properties.datetime",
            ["dt-2300", "dt-2000-only", "dt-1970", "dt-1969", "dt-1950", "dt-1900"]
            + ["range-1961-1990"],
        ),
        (
            "+properties.start_datetime",
            ["dt-1900", "dt-1950", "range-1961-1990", "dt-1969", "dt-1970", "dt-2300"]
            + ["dt-2000-only"],
        ),
        (
            "-properties.start_datetime",
            ["dt-2300", "dt-1970", "dt-1969", "range-1961-1990", "dt-1950", "dt-1900"]
            + ["dt-2000-only"],
        ),
        (
            None,
            ["dt-2300", "dt-2000-only", "dt-1970", "dt-1969", "dt-1950", "dt-1900"]
            + ["range-1961-1990"],
        ),
    ],
)
async def test_sort_orders_all_dates_and_puts_missing_last(
    app_client, seeded, sortby, expected
):
    url = f"/search?collections={seeded}&limit=2"
    if sortby:
        url += f"&sortby={sortby.replace('+', '%2B')}"
    assert await _all_pages(app_client, url) == expected


@pytest.mark.asyncio
async def test_datetime_aggregations_cover_all_dates(app_client, seeded):
    resp = await app_client.post(
        "/aggregate",
        json={
            "collections": [seeded],
            "aggregations": ["datetime_min", "datetime_max", "datetime_frequency"],
        },
    )
    assert resp.status_code == 200, resp.text
    aggs = {a["name"]: a for a in resp.json()["aggregations"]}
    assert aggs["datetime_min"]["value"].startswith("1900-01-01T00:00:00")
    assert aggs["datetime_max"]["value"].startswith("2300-01-01T00:00:00")
    buckets = [b for b in aggs["datetime_frequency"]["buckets"] if b["frequency"]]
    assert buckets[0]["key"].startswith("1900-01-01")
    assert buckets[-1]["key"].startswith("2300-01-01")
    assert sum(b["frequency"] for b in buckets) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["+", "-"])
async def test_sort_spans_date_and_date_nanos_indexes(
    app_client, seeded, load_test_data, monkeypatch, direction
):
    """A deployment mid-reindex holds both mappings; sorts must order them together."""
    with monkeypatch.context() as m:
        m.setenv("USE_DATETIME_NANOS", "true")
        await use_datetime_nanos(True)
        collection = load_test_data("test_collection.json")
        collection["id"] = f"{seeded}-nanos"
        resp = await app_client.post("/collections", json=collection)
        assert resp.status_code == 201, resp.text
        base = load_test_data("test_item.json")
        base["collection"] = collection["id"]
        for item in (
            _dated_item(base, "nanos-1990", "1990-01-01T00:00:00.000000001Z"),
            _dated_item(
                base, "nanos-null", None, "1980-01-01T00:00:00Z", "1981-01-01T00:00:00Z"
            ),
        ):
            resp = await app_client.post(
                f"/collections/{collection['id']}/items", json=item
            )
            assert resp.status_code == 201, resp.text
    await use_datetime_nanos(False)

    dated = ["dt-1900", "dt-1950", "dt-1969", "dt-1970", "nanos-1990"]
    dated += ["dt-2000-only", "dt-2300"]
    if direction == "-":
        dated.reverse()
    url = (
        f"/search?collections={seeded},{collection['id']}&limit=2"
        f"&sortby={'%2B' if direction == '+' else '-'}properties.datetime"
    )
    assert await _all_pages(app_client, url) == dated + [
        "nanos-null",
        "range-1961-1990",
    ]
