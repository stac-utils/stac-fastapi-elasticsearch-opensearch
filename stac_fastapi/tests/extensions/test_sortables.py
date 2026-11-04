import os
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from stac_fastapi.core.models.links import SORTABLES_REL
from stac_fastapi.extensions.sort import SortConformanceClasses
from stac_fastapi.sfeos_helpers.database.query import (
    populate_sort_shared,
    validate_sort_fields_shared,
)
from stac_fastapi.sfeos_helpers.sort.client import (
    DEFAULT_SORTABLES,
    SortablesCache,
    get_sortable_field_types_shared,
    get_sortable_properties_shared,
)
from stac_fastapi.types.errors import NotFoundError


@pytest.mark.asyncio
async def test_sortables(app_client: AsyncClient):
    """GET /sortables returns the default sortable fields."""
    resp = await app_client.get("/sortables")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/schema+json")
    sortables = resp.json()
    assert sortables["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert sortables["$id"] == "http://test-server/sortables"
    assert sortables["type"] == "object"
    assert sortables["title"] == "Sortables"
    assert sortables["properties"] == DEFAULT_SORTABLES
    assert sortables["additionalProperties"] is True


@pytest.mark.asyncio
async def test_collection_sortables(app_client: AsyncClient, ctx):
    """GET /collections/{collection_id}/sortables reflects the items index mapping."""
    resp = await app_client.get(f"/collections/{ctx.collection['id']}/sortables")
    assert resp.status_code == 200
    sortables = resp.json()
    assert sortables["$id"] == (
        f"http://test-server/collections/{ctx.collection['id']}/sortables"
    )
    assert sortables["title"] == "Sortables"
    properties = sortables["properties"]

    # Sortable mapped fields
    assert properties["id"]["type"] == "string"
    assert properties["collection"]["type"] == "string"
    assert properties["properties.datetime"]["format"] == "date-time"
    assert properties["properties.eo:cloud_cover"]["type"] == "number"
    assert properties["properties.platform"]["type"] == "string"

    # Non-sortable mapped fields are not advertised
    assert "geometry" not in properties  # geo_shape
    assert "properties.proj:centroid" not in properties  # geo_point
    assert "links" not in properties  # enabled: false

    assert sortables["additionalProperties"] is False


@pytest.mark.asyncio
async def test_collection_sortables_not_found(app_client: AsyncClient):
    """GET /collections/{collection_id}/sortables 404s for unknown collections."""
    resp = await app_client.get("/collections/does-not-exist/sortables")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_collections_sortables(app_client: AsyncClient):
    """GET /collections-sortables reflects the collections index mapping."""
    resp = await app_client.get("/collections-sortables")
    assert resp.status_code == 200
    sortables = resp.json()
    assert sortables["$id"] == "http://test-server/collections-sortables"
    assert sortables["title"] == "Sortables"
    properties = sortables["properties"]

    # Sortable mapped fields
    assert properties["id"]["type"] == "string"
    assert properties["title"]["type"] == "string"  # text with keyword sub-field
    assert properties["extent.temporal.interval"]["format"] == "date-time"
    # Alias to a sortable field is advertised with the target's schema
    assert properties["temporal"]["type"] == "string"
    assert properties["temporal"]["format"] == "date-time"

    # Non-sortable mapped fields are not advertised
    assert "bbox_shape" not in properties  # geo_shape
    assert "providers" not in properties  # enabled: false
    assert "links" not in properties  # enabled: false

    assert sortables["additionalProperties"] is False


def test_collections_sortables_route_gated_on_collection_search():
    """`/collections-sortables` is not registered when collection search is off."""
    if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
        from stac_fastapi.opensearch.app import instantiate_api
        from stac_fastapi.opensearch.config import (
            AsyncOpensearchSettings as AsyncSettings,
        )
        from stac_fastapi.opensearch.database_logic import DatabaseLogic
    else:
        from stac_fastapi.elasticsearch.app import instantiate_api
        from stac_fastapi.elasticsearch.config import (
            AsyncElasticsearchSettings as AsyncSettings,
        )
        from stac_fastapi.elasticsearch.database_logic import DatabaseLogic

    api = instantiate_api(
        settings=AsyncSettings(
            enable_collections_search=False,
            enable_collections_search_route=False,
        ),
        database_logic=DatabaseLogic(),
    )
    routes = set()
    for route in api.app.routes:
        router = getattr(route, "original_router", None)
        if router is not None:
            routes.update(getattr(sub, "path", "") for sub in router.routes)
        else:
            routes.add(getattr(route, "path", ""))
    assert "/sortables" in routes
    assert "/collections/{collection_id}/sortables" in routes
    assert "/collections-sortables" not in routes


@pytest.mark.asyncio
async def test_sortables_conformance(app_client: AsyncClient):
    """The sortables conformance URIs are advertised on /conformance."""
    resp = await app_client.get("/conformance")
    assert resp.status_code == 200
    conforms_to = resp.json()["conformsTo"]
    assert SortConformanceClasses.ITEM_SEARCH_SORTABLES in conforms_to
    assert SortConformanceClasses.FEATURES_SORTABLES in conforms_to
    assert SortConformanceClasses.COLLECTION_SEARCH_SORTABLES in conforms_to


@pytest.mark.asyncio
async def test_sortables_links(app_client: AsyncClient, ctx):
    """Resources link to their sortables endpoint with the OGC sortables rel."""
    landing = (await app_client.get("/")).json()
    assert any(
        link["rel"] == SORTABLES_REL
        and link["href"] == "http://test-server/sortables"
        and link["type"] == "application/schema+json"
        for link in landing["links"]
    )

    collection = (await app_client.get(f"/collections/{ctx.collection['id']}")).json()
    assert any(
        link["rel"] == SORTABLES_REL
        and link["href"]
        == f"http://test-server/collections/{ctx.collection['id']}/sortables"
        and link["type"] == "application/schema+json"
        for link in collection["links"]
    )

    collections = (await app_client.get("/collections")).json()
    assert any(
        link["rel"] == SORTABLES_REL
        and link["href"] == "http://test-server/collections-sortables"
        and link["type"] == "application/schema+json"
        for link in collections["links"]
    )


@pytest.mark.asyncio
async def test_sortby_undeclared_field(app_client: AsyncClient, ctx):
    """Unknown sort fields sort as null on /search and 400 on /items."""
    # /sortables advertises additionalProperties: true by default, so an
    # unmapped name is a valid sort field whose values evaluate to null.
    resp = await app_client.post(
        "/search",
        json={"sortby": [{"field": "definitely_not_sortable", "direction": "asc"}]},
    )
    assert resp.status_code == 200

    # /collections/{id}/sortables advertises false, so undeclared fields 400.
    resp = await app_client.get(
        f"/collections/{ctx.collection['id']}/items?sortby=properties.undeclared"
    )
    assert resp.status_code == 400

    # Mapped but unsortable fields are not in sortables either.
    resp = await app_client.get(
        f"/collections/{ctx.collection['id']}/items?sortby=geometry"
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_sortby_union_membership(app_client: AsyncClient, ctx, monkeypatch):
    """With ROOT_QUERYABLES_UNION, sortby fields must be in the union schema."""
    monkeypatch.setenv("ROOT_QUERYABLES_UNION", "true")

    resp = await app_client.post(
        "/search",
        json={
            "sortby": [
                {"field": "properties.eo:cloud_cover", "direction": "desc"},
                {"field": "id"},
            ]
        },
    )
    assert resp.status_code == 200

    resp = await app_client.post(
        "/search", json={"sortby": [{"field": "definitely_not_sortable"}]}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_sortby_field_mapped_in_some_collections(
    app_client: AsyncClient, ctx, load_test_data, txn_client
):
    """A field mapped in only some collections sorts all items, missing last."""
    from ..conftest import create_collection, create_item

    # A second collection whose index does not map properties.eo:cloud_cover.
    collection_b = load_test_data("test_collection.json")
    collection_b["id"] = "collection-b"
    await create_collection(txn_client, collection_b)

    item_b = load_test_data("test_item.json")
    item_b["id"] = "item-b"
    item_b["collection"] = "collection-b"
    item_b["properties"].pop("eo:cloud_cover", None)
    await create_item(txn_client, item_b)

    # Ascending: the mapped item sorts first, the unmapped one last. A keyword
    # unmapped_type would 400 with incompatible sort types across shards.
    resp = await app_client.post(
        "/search",
        json={"sortby": [{"field": "properties.eo:cloud_cover", "direction": "asc"}]},
    )
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert len(features) == 2
    assert features[0]["id"] == ctx.item["id"]
    assert features[1]["id"] == "item-b"


def test_get_sortable_properties_shared():
    """Only fields the engine can sort on are returned."""
    properties = get_sortable_properties_shared(
        {
            "id": {"type": "keyword"},
            "count": {"type": "long"},
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description": {"type": "text"},
            "geometry": {"type": "geo_shape"},
            "hidden": {"type": "keyword", "enabled": False},
            "wrapper": {
                "type": "object",
                "properties": {"inner": {"type": "date"}},
            },
            "grouped": {
                "properties": {"scores": {"type": "float"}},
            },
            "nested_obj": {
                "type": "nested",
                "properties": {"leaf": {"type": "keyword"}},
            },
        }
    )
    assert set(properties) == {
        "id",
        "count",
        "title",
        "wrapper.inner",
        "grouped.scores",
    }
    assert properties["title"]["type"] == "string"
    assert properties["wrapper.inner"]["format"] == "date-time"


def _fake_database(calls: list[str]) -> SimpleNamespace:
    """A database stub that records get_items_mapping calls.

    Returns a two-index response, as get_mapping over items_* would, with
    nested `properties` objects like real item index mappings.
    """

    async def get_items_mapping(collection_id: str) -> dict:
        calls.append(collection_id)
        return {
            "items_c1_000001": {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "properties": {
                            "type": "object",
                            "properties": {
                                "datetime": {"type": "date_nanos"},
                                "score": {"type": "long"},
                            },
                        },
                    }
                }
            },
            "items_c2_000001": {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "properties": {
                            "type": "object",
                            "properties": {
                                "rating": {"type": "integer"},
                                "platform": {"type": "keyword"},
                            },
                        },
                    }
                }
            },
        }

    return SimpleNamespace(get_items_mapping=get_items_mapping)


@pytest.mark.asyncio
async def test_sortables_cache_reuses_union():
    """The union of collection sortables is computed once per TTL window."""
    calls: list[str] = []
    cache = SortablesCache(_fake_database(calls))

    properties = await cache.get_union_properties()
    assert properties["id"]["type"] == "string"
    assert properties["properties.datetime"]["format"] == "date-time"
    # Fields nested under `properties` union across indexes, not first-wins
    assert properties["properties.score"]["type"] == "number"
    assert properties["properties.rating"]["type"] == "integer"
    assert properties["properties.platform"]["type"] == "string"

    assert await cache.get_union_properties() == properties
    assert calls == ["*"]


@pytest.mark.asyncio
async def test_sortables_cache_collection_properties():
    """Per-collection sortables are cached and used to validate sortby."""
    calls: list[str] = []
    cache = SortablesCache(_fake_database(calls))

    properties = await cache.get_collection_properties("c1")
    assert properties["properties.score"]["type"] == "number"
    assert await cache.get_collection_properties("c1") == properties
    assert calls == ["c1"]

    await cache.validate(["properties.score"], collection_id="c1")
    with pytest.raises(HTTPException) as exc:
        await cache.validate(["not.declared"], collection_id="c1")
    assert exc.value.status_code == 400

    # Union validation admits core fields plus unioned collection fields
    await cache.validate(["id", "properties.rating"])
    with pytest.raises(HTTPException):
        await cache.validate(["not_declared"])


@pytest.mark.asyncio
async def test_sortables_cache_expires():
    """The union is recomputed once the TTL has elapsed."""
    calls: list[str] = []
    cache = SortablesCache(_fake_database(calls))
    await cache.get_union_properties()

    cache.cache_ttl = 0
    await cache.get_union_properties()
    assert calls == ["*", "*"]


@pytest.mark.asyncio
async def test_sortables_cache_empty_union_cached():
    """An empty union is still cached for the TTL window."""
    walks: list[str] = []

    async def get_items_mapping(collection_id: str) -> dict:
        walks.append("walk")
        raise NotFoundError("no item indexes")

    database = SimpleNamespace(get_items_mapping=get_items_mapping)
    cache = SortablesCache(database)
    assert await cache.get_union_properties() == {}
    assert await cache.get_union_properties() == {}
    assert walks == ["walk"]


def test_get_sortable_properties_shared_aliases():
    """Alias fields are advertised with their target field's schema."""
    properties = get_sortable_properties_shared(
        {
            "temporal": {"type": "alias", "path": "extent.temporal.interval"},
            "broken": {"type": "alias", "path": "missing.path"},
            "shadow": {"type": "alias", "path": "geometry"},
            "extent": {
                "type": "object",
                "properties": {
                    "temporal": {
                        "type": "object",
                        "properties": {"interval": {"type": "date"}},
                    }
                },
            },
            "geometry": {"type": "geo_shape"},
        }
    )
    assert properties["temporal"]["type"] == "string"
    assert properties["temporal"]["format"] == "date-time"
    assert "broken" not in properties  # unresolvable target
    assert "shadow" not in properties  # target is not sortable


def test_sortby_excluded_fields_bad_request(monkeypatch):
    """Excluded sortby fields are a 400, per additionalProperties: false."""
    monkeypatch.setenv("EXCLUDED_FROM_QUERYABLES", "properties.secret")

    # Bare, prefixed, and child names are all rejected
    for field in ("secret", "properties.secret", "properties.secret.level"):
        with pytest.raises(HTTPException) as exc:
            validate_sort_fields_shared([field])
        assert exc.value.status_code == 400

    # Non-excluded fields pass
    validate_sort_fields_shared(["id", "properties.datetime"])

    # populate_sort_shared rejects them before building sort config
    sort_field = SimpleNamespace(field="properties.secret", direction="asc")
    with pytest.raises(HTTPException):
        populate_sort_shared([sort_field])


def test_get_sortable_field_types_shared():
    """Field types map each sortable field to the engine type sorted on."""
    types = get_sortable_field_types_shared(
        {
            "id": {"type": "keyword"},
            "count": {"type": "long"},
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "geometry": {"type": "geo_shape"},
            "wrapper": {
                "type": "object",
                "properties": {"inner": {"type": "date_nanos"}},
            },
            "temporal": {"type": "alias", "path": "wrapper.inner"},
        }
    )
    assert types == {
        "id": "keyword",
        "count": "long",
        "title": "keyword",
        "wrapper.inner": "date_nanos",
        "temporal": "date_nanos",
    }


def test_populate_sort_shared_unmapped_type():
    """Sort clauses null-fill documents in indexes that do not map the field."""
    sort = populate_sort_shared(
        [SimpleNamespace(field="properties.score", direction="desc")]
    )
    assert sort["properties.score"] == {
        "order": "desc",
        "unmapped_type": "keyword",
        "missing": "_last",
    }
    assert sort["id"] == {"order": "asc"}
    assert list(sort) == ["properties.score", "id", "collection"]

    # The field's mapped type is used when known: a double field sorted with
    # unmapped_type keyword still fails with incompatible sort types.
    sort = populate_sort_shared(
        [SimpleNamespace(field="properties.score", direction="desc")],
        field_types={"properties.score": "double"},
    )
    assert sort["properties.score"]["unmapped_type"] == "double"

    # Unknown names still fall back to keyword so they evaluate to null.
    sort = populate_sort_shared(
        [SimpleNamespace(field="not.mapped", direction="asc")],
        field_types={"properties.score": "double"},
    )
    assert sort["not.mapped"]["unmapped_type"] == "keyword"


def test_populate_sort_shared_item_datetime_compares_in_milliseconds():
    """Item datetime sorts use numeric_type date; collection sorts don't."""
    sort = populate_sort_shared(
        [SimpleNamespace(field="properties.datetime", direction="desc")],
        field_types={"properties.datetime": "date_nanos"},
    )
    assert sort["properties.datetime"] == {
        "order": "desc",
        "numeric_type": "date",
        "unmapped_type": "date",
        "missing": "_last",
    }
    sort = populate_sort_shared(
        [SimpleNamespace(field="properties.datetime", direction="asc")],
        is_collection=True,
    )
    assert "numeric_type" not in sort["properties.datetime"]


def test_sort_post_request_spec_conformance():
    """POST sortby requires only `field`; direction defaults to asc."""
    from pydantic import ValidationError

    from stac_fastapi.sfeos_helpers.sort.request import SortExtensionPostRequest

    model = SortExtensionPostRequest(sortby=[{"field": "id"}])
    assert model.sortby[0].direction == "asc"

    with pytest.raises(ValidationError):
        SortExtensionPostRequest(sortby=[])


def test_get_sortable_properties_shared_exclusions():
    """Excluded fields and their children are not returned."""
    properties = get_sortable_properties_shared(
        {
            "id": {"type": "keyword"},
            "properties": {
                "type": "object",
                "properties": {
                    "secret": {"type": "keyword"},
                    "secret_child": {"type": "keyword"},
                },
            },
        },
        excluded={"properties.secret"},
    )
    assert set(properties) == {"id", "properties.secret_child"}
