"""ETag and If-Match on single items and collections (optimistic locking)."""

import re
import sys
import uuid
from copy import deepcopy

import pytest
from stac_pydantic import api

from stac_fastapi.core.etag import if_match_holds
from stac_fastapi.core.utilities import get_bool_env

from ..conftest import MockRequest, create_collection, create_item, database

pytestmark = [pytest.mark.datetime_filtering, pytest.mark.asyncio]

RESOURCES = ["item", "collection"]


def _url(ctx, resource):
    if resource == "collection":
        return f"/collections/{ctx.collection['id']}"
    return f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"


def _replacement(ctx, resource, title):
    doc = deepcopy(ctx.item if resource == "item" else ctx.collection)
    if resource == "item":
        doc["properties"]["title"] = title
    else:
        doc["title"] = title
    return doc


def _title(doc, resource):
    return doc["properties"].get("title") if resource == "item" else doc.get("title")


def _merge_patch(resource, title):
    return {"properties": {"title": title}} if resource == "item" else {"title": title}


async def _etag(app_client, url):
    response = await app_client.get(url)
    assert response.status_code == 200, response.text
    return response.headers["etag"]


async def _write(app_client, method, url, ctx, resource, title, headers):
    if method == "PUT":
        return await app_client.put(
            url, json=_replacement(ctx, resource, title), headers=headers
        )
    if method == "PATCH":
        return await app_client.patch(
            url,
            json=_merge_patch(resource, title),
            headers={"Content-Type": "application/merge-patch+json", **headers},
        )
    return await app_client.delete(url, headers=headers)


@pytest.mark.parametrize("resource", RESOURCES)
async def test_get_returns_strong_etag(app_client, ctx, resource):
    response = await app_client.get(_url(ctx, resource))

    assert response.status_code == 200
    etag = response.headers.get("etag")
    assert etag is not None
    # Primary term, sequence number, a hash of the index that holds the document,
    # and the content coding of a compressed response.
    assert re.fullmatch(r'"\d+-\d+-[0-9a-f]{8}(-[a-z][a-z0-9.]*)?"', etag), etag
    assert await _etag(app_client, _url(ctx, resource)) == etag


@pytest.mark.parametrize("coding", ["identity", "gzip", "br"])
async def test_each_content_coding_has_its_own_strong_etag(app_client, ctx, coding):
    """RFC 9110, section 8.8.1: a tag shared by gzip and plain bodies would be weak."""
    url = _url(ctx, "item")
    plain = await app_client.get(url, headers={"Accept-Encoding": "identity"})
    response = await app_client.get(url, headers={"Accept-Encoding": coding})

    etag = response.headers["etag"]
    assert response.headers.get("content-encoding", "identity") == coding
    assert not etag.startswith("W/")
    assert (etag == plain.headers["etag"]) == (coding == "identity")
    response = await app_client.put(
        url, json=_replacement(ctx, "item", coding), headers={"If-Match": etag}
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize("resource", RESOURCES)
async def test_create_returns_the_etag_of_the_new_version(
    app_client, ctx, load_test_data, resource
):
    if resource == "collection":
        doc = load_test_data("test_collection.json")
        doc["id"] = f"conditional-{uuid.uuid4()}"
        target, url = "/collections", f"/collections/{doc['id']}"
    else:
        doc = load_test_data("test_item.json")
        doc["id"] = f"conditional-{uuid.uuid4()}"
        target = f"/collections/{ctx.collection['id']}/items"
        url = f"{target}/{doc['id']}"

    response = await app_client.post(target, json=doc)

    assert response.status_code == 201, response.text
    assert response.headers["etag"] == await _etag(app_client, url)


async def test_lists_carry_no_etag(app_client, ctx):
    for url in ("/collections", f"/collections/{ctx.collection['id']}/items"):
        response = await app_client.get(url)
        assert response.status_code == 200
        assert "etag" not in response.headers


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("method", ["PUT", "PATCH"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_update_with_current_etag_returns_the_new_etag(
    app_client, ctx, monkeypatch, resource, method, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    response = await _write(
        app_client, method, url, ctx, resource, "first", {"If-Match": etag}
    )

    assert response.status_code == 200, response.text
    new_etag = response.headers.get("etag")
    assert new_etag is not None and new_etag != etag
    assert await _etag(app_client, url) == new_etag
    stored = (await app_client.get(url)).json()
    assert _title(stored, resource) == "first"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_stale_etag_returns_412_and_changes_nothing(
    app_client, ctx, monkeypatch, resource, method, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = _url(ctx, resource)
    stale = await _etag(app_client, url)
    response = await app_client.put(
        url, json=_replacement(ctx, resource, "first"), headers={"If-Match": stale}
    )
    assert response.status_code == 200, response.text
    current = response.headers["etag"]

    response = await _write(
        app_client, method, url, ctx, resource, "second", {"If-Match": stale}
    )

    assert response.status_code == 412, response.text
    stored = await app_client.get(url)
    assert stored.status_code == 200
    assert _title(stored.json(), resource) == "first"
    assert stored.headers["etag"] == current


@pytest.mark.parametrize("resource", RESOURCES)
async def test_delete_with_current_etag(app_client, ctx, resource):
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    response = await app_client.delete(url, headers={"If-Match": etag})

    assert response.status_code in (200, 204)
    assert (await app_client.get(url)).status_code == 404


@pytest.mark.parametrize(
    "if_match",
    ["*", '"not-the-etag", {etag}', '{etag}, "not-the-etag"', " , {etag}, , "],
    ids=["star", "list-last", "list-first", "empty-elements"],
)
@pytest.mark.parametrize("resource", RESOURCES)
async def test_if_match_star_and_lists(app_client, ctx, resource, if_match):
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    response = await app_client.put(
        url,
        json=_replacement(ctx, resource, "matched"),
        headers={"If-Match": if_match.format(etag=etag)},
    )

    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "if_match", ["W/{etag}", "{bare}", '"not-the-etag"', ""], ids=repr
)
@pytest.mark.parametrize("resource", RESOURCES)
async def test_if_match_that_does_not_match_returns_412(
    app_client, ctx, resource, if_match
):
    """Weak tags never match (RFC 9110, section 13.1.1); nor do malformed values."""
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    response = await app_client.put(
        url,
        json=_replacement(ctx, resource, "must not persist"),
        headers={"If-Match": if_match.format(etag=etag, bare=etag.strip('"'))},
    )

    assert response.status_code == 412, response.text
    assert _title((await app_client.get(url)).json(), resource) != "must not persist"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "headers", [{"If-Match": "*"}, {}], ids=["if-match", "no-if-match"]
)
@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_a_missing_resource_returns_404_before_the_precondition(
    app_client, ctx, monkeypatch, resource, method, headers, validator
):
    """RFC 9110, section 13.2.1: a 404 wins over 412 and 428 on every route."""
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    if resource == "collection":
        ctx.collection["id"] = "missing-collection"
    else:
        ctx.item["id"] = "missing-item"

    response = await _write(
        app_client, method, _url(ctx, resource), ctx, resource, "x", headers
    )

    assert response.status_code == 404, response.text


@pytest.mark.parametrize("method", ["PATCH", "DELETE"])
async def test_a_collection_write_after_an_alias_move_returns_412(
    app_client, ctx, monkeypatch, method
):
    """A write bound to the version's index must not land in a detached index."""
    # With the validator on, a PATCH goes through update_collection, which reads
    # through the alias and never calls index_behind_alias.
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    url = _url(ctx, "collection")
    etag = await _etag(app_client, url)

    async def moved(*args):
        return False

    module = sys.modules[type(database).__module__]
    monkeypatch.setattr(module, "index_behind_alias", moved)
    response = await _write(
        app_client, method, url, ctx, "collection", "detached", {"If-Match": etag}
    )

    assert response.status_code == 412, response.text
    assert _title((await app_client.get(url)).json(), "collection") != "detached"


async def test_if_match_allows_only_space_and_tab_around_tags():
    """RFC 9110, section 5.6.3: optional whitespace is space or tab."""
    assert if_match_holds(' \t"1-2-ab12cd34" ,\t', '"1-2-ab12cd34"')
    assert not if_match_holds('"1-2-ab12cd34"\r\n', '"1-2-ab12cd34"')


@pytest.mark.parametrize("validator", ["false", "true"])
async def test_a_client_error_comes_before_the_precondition(
    app_client, ctx, monkeypatch, validator
):
    """RFC 9110, section 13.2.1: a 400 found before the write wins over 412."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    doc = _replacement(ctx, "collection", "renamed")
    doc["id"] = "another-collection"

    response = await app_client.put(
        _url(ctx, "collection"), json=doc, headers={"If-Match": '"0-0"'}
    )

    assert response.status_code == 400, response.text

    for resource in RESOURCES:  # a JSON Patch media type with a merge-patch body
        response = await app_client.patch(
            _url(ctx, resource),
            json=_merge_patch(resource, "mismatched"),
            headers={
                "Content-Type": "application/json-patch+json",
                "If-Match": '"0-0"',
            },
        )
        assert response.status_code == 400, response.text


async def test_require_if_match_does_not_queue_creates(app_client, ctx, monkeypatch):
    """The queue writes with upsert semantics, so a queued POST could overwrite."""
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")
    title = _title(ctx.item, "item")

    response = await app_client.post(
        f"/collections/{ctx.collection['id']}/items",
        json=_replacement(ctx, "item", "overwritten"),
    )

    assert response.status_code == 409, response.text
    stored = await app_client.get(_url(ctx, "item"))
    assert _title(stored.json(), "item") == title


async def test_a_recreated_collection_does_not_reuse_old_item_etags(
    app_client, ctx, load_test_data
):
    """Sequence numbers restart in a new index, so the tag must name the index."""
    url = _url(ctx, "item")
    old_etag = await _etag(app_client, url)
    response = await app_client.delete(_url(ctx, "collection"))
    assert response.status_code in (200, 204), response.text
    response = await app_client.post("/collections", json=ctx.collection)
    assert response.status_code == 201, response.text
    response = await app_client.post(
        f"/collections/{ctx.collection['id']}/items", json=ctx.item
    )
    assert response.status_code == 201, response.text

    response = await app_client.delete(url, headers={"If-Match": old_etag})

    assert response.status_code == 412, response.text
    assert (await app_client.get(url)).status_code == 200


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_writes_without_if_match_still_work_by_default(
    app_client, ctx, resource, method
):
    response = await _write(
        app_client, method, _url(ctx, resource), ctx, resource, "unconditional", {}
    )

    assert response.status_code in (200, 204), response.text


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_require_if_match_returns_428_without_it(
    app_client, ctx, monkeypatch, resource, method
):
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    response = await _write(app_client, method, url, ctx, resource, "no header", {})

    assert response.status_code == 428, response.text
    assert "If-Match" in response.json()["detail"]  # RFC 6585: say how to resubmit
    assert await _etag(app_client, url) == etag

    response = await _write(
        app_client, method, url, ctx, resource, "with header", {"If-Match": etag}
    )
    assert response.status_code in (200, 204), response.text


async def test_require_if_match_leaves_creates_alone(
    app_client, ctx, monkeypatch, load_test_data
):
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    collection = load_test_data("test_collection.json")
    collection["id"] = f"conditional-{uuid.uuid4()}"
    item = load_test_data("test_item.json")
    item["id"] = f"conditional-{uuid.uuid4()}"

    response = await app_client.post("/collections", json=collection)
    assert response.status_code == 201, response.text
    response = await app_client.post(
        f"/collections/{ctx.collection['id']}/items", json=item
    )
    assert response.status_code == 201, response.text


async def test_conditional_put_writes_to_the_index_that_holds_the_item(
    app_client, ctx, monkeypatch
):
    """The inserter can pick another index for a new item; a replace stays put."""
    url = _url(ctx, "item")
    etag = await _etag(app_client, url)

    async def another_index(self, *args, **kwargs):
        return "items_somewhere-else"

    monkeypatch.setattr(
        type(database.async_index_inserter), "get_target_index", another_index
    )
    response = await _write(
        app_client, "PUT", url, ctx, "item", "replaced", {"If-Match": etag}
    )
    assert response.status_code == 200, response.text
    stored = await app_client.get(url)
    assert _title(stored.json(), "item") == "replaced"


@pytest.mark.parametrize("validator", ["false", "true"])
async def test_a_patch_whose_item_was_recreated_elsewhere_returns_412(
    app_client, ctx, monkeypatch, validator
):
    """Between the check and the write, the collection is deleted and created
    again with the same item: the replacement lives in another index and must
    not be patched with the old tag."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = _url(ctx, "item")
    etag = await _etag(app_client, url)
    client_cls = type(database.client)
    # The write's own lookup is a search, or a real-time mget under datetime
    # index filtering; the write itself is index or a scripted update with a
    # precondition. The mget resolves the item's index before a hook could
    # fire, so the recreation is triggered on the lookup or the write only.
    originals = {
        name: getattr(client_cls, name) for name in ("search", "index", "update")
    }
    recreated = []

    async def recreate():
        recreated.append(True)
        response = await app_client.delete(_url(ctx, "collection"))
        assert response.status_code in (200, 204), response.text
        response = await app_client.post("/collections", json=ctx.collection)
        assert response.status_code == 201, response.text
        response = await app_client.post(
            f"/collections/{ctx.collection['id']}/items", json=ctx.item
        )
        assert response.status_code == 201, response.text

    def hooked(name):
        async def call(self, *args, **kwargs):
            body = kwargs.get("body") or {}
            # The write's own lookup (no version asked for), or the write itself.
            trigger = (
                name == "search"
                and not body.get("seq_no_primary_term")
                and ctx.item["id"] in str(body.get("query", {}))
            ) or (name in ("index", "update") and "if_seq_no" in kwargs)
            if trigger and not recreated:
                await recreate()
            return await originals[name](self, *args, **kwargs)

        return call

    for name in originals:
        monkeypatch.setattr(client_cls, name, hooked(name))
    response = await _write(
        app_client, "PATCH", url, ctx, "item", "stale", {"If-Match": etag}
    )
    for name, original in originals.items():
        monkeypatch.setattr(client_cls, name, original)

    assert recreated, "the collection was not recreated during the write"
    assert response.status_code == 412, response.text
    stored = await app_client.get(url)
    assert stored.status_code == 200
    assert _title(stored.json(), "item") == _title(ctx.item, "item")
    # A recreation in the moment between the existence check and the write can
    # still leave an empty index under the old name; that window is accepted.


async def test_an_item_hidden_since_the_last_refresh_is_not_returned(
    app_client, ctx, monkeypatch
):
    """The search sees the last refreshed version; the real-time read must
    recheck the hidden field on the current document."""
    monkeypatch.setenv("HIDE_ITEM_PATH", "properties._private.hidden")
    url = _url(ctx, "item")
    assert (await app_client.get(url)).status_code == 200
    from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id

    await database.client.update(
        index=index_alias_by_collection_id(ctx.item["collection"]),
        id=f"{ctx.item['id']}|{ctx.item['collection']}",
        body={"doc": {"properties": {"_private": {"hidden": True}}}},
        refresh=False,
    )

    assert (await app_client.get(url)).status_code == 404


HIDDEN_SHAPES = [
    {"hidden": True},
    {"hidden": False},
    {"hidden": "false"},
    {"hidden": [True]},
    {"hidden": [True, False]},
    {"hidden": [[False]]},
    {"hidden": []},
    {"hidden": None},
    [{"hidden": True}],
    [{"hidden": True}, {"hidden": False}],
]


@pytest.mark.parametrize("private", HIDDEN_SHAPES, ids=[str(s) for s in HIDDEN_SHAPES])
async def test_is_hidden_agrees_with_the_search_filter(ctx, private):
    """The real-time read must decide as the engine's filter does, for every shape."""
    from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
    from stac_fastapi.sfeos_helpers.database.utils import add_hidden_filter, is_hidden

    path = "properties._private.hidden"
    index = index_alias_by_collection_id(ctx.item["collection"])
    doc_id = f"parity-{uuid.uuid4()}"
    source = {**deepcopy(ctx.item), "id": doc_id}
    source["properties"]["_private"] = private
    await database.client.index(index=index, id=doc_id, body=source, refresh=True)

    response = await database.client.search(
        index=index,
        body={"query": add_hidden_filter({"term": {"_id": doc_id}}, path), "size": 0},
    )
    visible_to_search = response["hits"]["total"]["value"] == 1

    assert is_hidden(source, path) is (not visible_to_search)


async def test_require_if_match_refuses_bulk_upsert(
    app_client, ctx, monkeypatch, load_test_data
):
    """A bulk upsert overwrites items with no If-Match; insert still works."""
    if get_bool_env("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("bulk_items returns 400 under datetime index filtering")
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    url = f"/collections/{ctx.collection['id']}/bulk_items"
    item = load_test_data("test_item.json")
    item["id"] = f"conditional-{uuid.uuid4()}"
    item["collection"] = ctx.collection["id"]
    response = await app_client.post(
        url, json={"items": {item["id"]: item}, "method": "upsert"}
    )
    assert response.status_code == 428, response.text
    assert "insert" in response.json()["detail"]
    response = await app_client.post(
        url, json={"items": {item["id"]: item}, "method": "insert"}
    )
    assert response.status_code == 200, response.text


async def test_openapi_documents_etag_and_if_match(app_client):
    response = await app_client.get("/api")
    assert response.status_code == 200
    paths = response.json()["paths"]
    collection = paths["/collections/{collection_id}"]
    item = paths["/collections/{collection_id}/items/{item_id}"]
    for operation in (collection["get"], collection["put"], item["get"], item["patch"]):
        assert "ETag" in operation["responses"]["200"]["headers"]
    for operation in (collection["put"], collection["delete"], item["patch"]):
        assert any(p["name"] == "If-Match" for p in operation["parameters"])
        assert {"412", "428"} <= operation["responses"].keys()
    assert (
        "428" in paths["/collections/{collection_id}/bulk_items"]["post"]["responses"]
    )
    assert "If-Match" not in str(paths["/search"])


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize("resource", RESOURCES)
async def test_a_write_between_check_and_write_returns_412(
    app_client, ctx, monkeypatch, resource, method, validator
):
    """The ETag check and the write are one atomic step in the database."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = _url(ctx, resource)
    etag = await _etag(app_client, url)

    client_cls = type(database.client)
    originals = {name: getattr(client_cls, name) for name in ("index", "update")}
    originals["delete"] = client_cls.delete
    raced = []

    def racing(name):
        async def call(self, *args, **kwargs):
            if "if_seq_no" in kwargs and not raced:
                raced.append(name)
                concurrent = await app_client.patch(
                    url,
                    json=_merge_patch(resource, "concurrent"),
                    headers={"Content-Type": "application/merge-patch+json"},
                )
                assert concurrent.status_code == 200, concurrent.text
            return await originals[name](self, *args, **kwargs)

        return call

    for name in originals:
        monkeypatch.setattr(client_cls, name, racing(name))
    response = await _write(
        app_client, method, url, ctx, resource, "lost", {"If-Match": etag}
    )
    for name, original in originals.items():
        monkeypatch.setattr(client_cls, name, original)

    assert raced, "the write was not conditional"
    assert response.status_code == 412, response.text
    stored = await app_client.get(url)
    assert stored.status_code == 200
    assert _title(stored.json(), resource) == "concurrent"


async def test_catalog_scoped_collection_update_checks_if_match(
    catalogs_app_client, load_test_data
):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"catalog-{uuid.uuid4()}"
    collection = load_test_data("test_collection.json")
    collection["id"] = f"collection-{uuid.uuid4()}"
    assert (
        await catalogs_app_client.post("/catalogs", json=catalog)
    ).status_code == 201
    response = await catalogs_app_client.post(
        f"/catalogs/{catalog['id']}/collections", json=collection
    )
    assert response.status_code == 201, response.text
    url = f"/catalogs/{catalog['id']}/collections/{collection['id']}"
    etag = await _etag(catalogs_app_client, f"/collections/{collection['id']}")

    response = await catalogs_app_client.put(
        url, json={**collection, "title": "first"}, headers={"If-Match": etag}
    )
    assert response.status_code == 200, response.text
    assert response.headers["etag"] == await _etag(
        catalogs_app_client, f"/collections/{collection['id']}"
    )

    response = await catalogs_app_client.put(
        url, json={**collection, "title": "second"}, headers={"If-Match": etag}
    )
    assert response.status_code == 412, response.text
    stored = await catalogs_app_client.get(f"/collections/{collection['id']}")
    assert stored.json()["title"] == "first"


async def test_conditional_put_is_not_queued(txn_client, load_test_data, monkeypatch):
    """A queued write could not be checked atomically, so a conditional PUT is not queued."""
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")
    collection = load_test_data("test_collection.json")
    collection["id"] = f"conditional-queue-{uuid.uuid4()}"
    await create_collection(txn_client, collection)
    item = load_test_data("test_item.json")
    item["collection"] = collection["id"]
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    await create_item(txn_client, item)
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")
    _, version = await database.get_one_item_with_version(collection["id"], item["id"])
    item["properties"]["title"] = "written directly"

    await txn_client.update_item(
        collection_id=collection["id"],
        item_id=item["id"],
        item=api.Item(**item),
        request=MockRequest(
            method="PUT",
            headers={"content-type": "application/json", "if-match": version.etag},
        ),
    )

    stored = await database.get_one_item(collection["id"], item["id"])
    assert stored["properties"]["title"] == "written directly"
    await txn_client.delete_collection(collection["id"], request=MockRequest)
