"""ETag and If-Match on single catalogs (optimistic locking)."""

import re
import uuid

import pytest

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from ..conftest import database

STRONG_ETAG = re.compile(r'^"\d+-\d+-[0-9a-f]{8}"$')


async def _create_catalog(client, load_test_data, parent_id=None, title=None):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"catalog-{uuid.uuid4()}"
    if title is not None:
        catalog["title"] = title
    url = f"/catalogs/{parent_id}/catalogs" if parent_id else "/catalogs"
    response = await client.post(url, json=catalog)
    assert response.status_code == 201, response.text
    return catalog, response


async def _etag(client, catalog_id):
    # The plain representation's tag; a compressed one has the coding appended.
    response = await client.get(
        f"/catalogs/{catalog_id}", headers={"Accept-Encoding": "identity"}
    )
    assert response.status_code == 200, response.text
    return response.headers["etag"]


async def _title(client, catalog_id):
    response = await client.get(f"/catalogs/{catalog_id}")
    assert response.status_code == 200, response.text
    return response.json().get("title")


async def _child_ids(client, parent_id):
    response = await client.get(f"/catalogs/{parent_id}/catalogs")
    assert response.status_code == 200, response.text
    return {catalog["id"] for catalog in response.json()["catalogs"]}


def _replacement(catalog, title):
    return {**catalog, "title": title}


@pytest.mark.asyncio
async def test_get_catalog_returns_strong_etag(catalogs_app_client, load_test_data):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)

    etag = await _etag(catalogs_app_client, catalog["id"])

    assert STRONG_ETAG.match(etag), etag


@pytest.mark.asyncio
async def test_a_compressed_catalog_has_its_own_tag_that_if_match_accepts(
    catalogs_app_client, load_test_data
):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    plain = await _etag(catalogs_app_client, catalog["id"])

    response = await catalogs_app_client.get(
        f"/catalogs/{catalog['id']}", headers={"Accept-Encoding": "br"}
    )
    assert response.headers.get("content-encoding") == "br"
    coded = response.headers["etag"]
    assert coded == f'{plain[:-1]}-br"'

    response = await catalogs_app_client.put(
        f"/catalogs/{catalog['id']}",
        json=_replacement(catalog, "coded"),
        headers={"If-Match": coded},
    )
    assert response.status_code == 200, response.text
    assert await _title(catalogs_app_client, catalog["id"]) == "coded"


@pytest.mark.asyncio
async def test_create_catalog_returns_the_etag_of_the_new_version(
    catalogs_app_client, load_test_data
):
    catalog, response = await _create_catalog(catalogs_app_client, load_test_data)

    assert response.headers["etag"] == await _etag(catalogs_app_client, catalog["id"])


@pytest.mark.asyncio
async def test_catalog_lists_carry_no_etag(catalogs_app_client, load_test_data):
    await _create_catalog(catalogs_app_client, load_test_data)

    response = await catalogs_app_client.get("/catalogs")

    assert response.status_code == 200
    assert "etag" not in response.headers


@pytest.mark.asyncio
async def test_update_with_current_etag_returns_the_new_etag(
    catalogs_app_client, load_test_data
):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    etag = await _etag(catalogs_app_client, catalog["id"])

    response = await catalogs_app_client.put(
        f"/catalogs/{catalog['id']}",
        json=_replacement(catalog, "updated"),
        headers={"If-Match": etag},
    )

    assert response.status_code == 200, response.text
    assert response.json()["title"] == "updated"
    assert STRONG_ETAG.match(response.headers["etag"])
    assert response.headers["etag"] != etag
    assert response.headers["etag"] == await _etag(catalogs_app_client, catalog["id"])


@pytest.mark.asyncio
async def test_update_with_stale_etag_returns_412_and_changes_nothing(
    catalogs_app_client, load_test_data
):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    etag = await _etag(catalogs_app_client, catalog["id"])
    first = await catalogs_app_client.put(
        f"/catalogs/{catalog['id']}",
        json=_replacement(catalog, "first"),
        headers={"If-Match": etag},
    )
    assert first.status_code == 200, first.text

    response = await catalogs_app_client.put(
        f"/catalogs/{catalog['id']}",
        json=_replacement(catalog, "second"),
        headers={"If-Match": etag},
    )

    assert response.status_code == 412, response.text
    assert await _title(catalogs_app_client, catalog["id"]) == "first"


@pytest.mark.asyncio
@pytest.mark.parametrize("if_match", ["*", "W/{etag}", '"0-0", {etag}'])
async def test_if_match_star_weak_and_lists(
    catalogs_app_client, load_test_data, if_match
):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    etag = await _etag(catalogs_app_client, catalog["id"])

    response = await catalogs_app_client.put(
        f"/catalogs/{catalog['id']}",
        json=_replacement(catalog, "changed"),
        headers={"If-Match": if_match.format(etag=etag)},
    )

    if if_match.startswith("W/"):
        # If-Match compares strongly, so a weak tag never matches.
        assert response.status_code == 412, response.text
        assert await _title(catalogs_app_client, catalog["id"]) != "changed"
    else:
        assert response.status_code == 200, response.text
        assert await _title(catalogs_app_client, catalog["id"]) == "changed"


@pytest.mark.asyncio
async def test_conditional_update_keeps_the_parent(catalogs_app_client, load_test_data):
    parent, _ = await _create_catalog(catalogs_app_client, load_test_data)
    child, _ = await _create_catalog(
        catalogs_app_client, load_test_data, parent_id=parent["id"]
    )
    etag = await _etag(catalogs_app_client, child["id"])

    response = await catalogs_app_client.put(
        f"/catalogs/{child['id']}",
        json=_replacement(child, "renamed"),
        headers={"If-Match": etag},
    )

    assert response.status_code == 200, response.text
    assert child["id"] in await _child_ids(catalogs_app_client, parent["id"])


@pytest.mark.asyncio
async def test_delete_with_current_etag(catalogs_app_client, load_test_data):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    etag = await _etag(catalogs_app_client, catalog["id"])

    response = await catalogs_app_client.delete(
        f"/catalogs/{catalog['id']}", headers={"If-Match": etag}
    )

    assert response.status_code == 204, response.text
    missing = await catalogs_app_client.get(f"/catalogs/{catalog['id']}")
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_delete_with_stale_etag_keeps_the_catalog_and_its_children(
    catalogs_app_client, load_test_data
):
    parent, _ = await _create_catalog(catalogs_app_client, load_test_data)
    child, _ = await _create_catalog(
        catalogs_app_client, load_test_data, parent_id=parent["id"]
    )
    etag = await _etag(catalogs_app_client, parent["id"])
    changed = await catalogs_app_client.put(
        f"/catalogs/{parent['id']}",
        json=_replacement(parent, "changed"),
        headers={"If-Match": etag},
    )
    assert changed.status_code == 200, changed.text

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}", headers={"If-Match": etag}
    )

    assert response.status_code == 412, response.text
    assert await _title(catalogs_app_client, parent["id"]) == "changed"
    # A refused delete must not have unlinked the children first.
    assert child["id"] in await _child_ids(catalogs_app_client, parent["id"])


@pytest.mark.asyncio
async def test_conditional_delete_still_unlinks_the_children(
    catalogs_app_client, load_test_data
):
    parent, _ = await _create_catalog(catalogs_app_client, load_test_data)
    child, _ = await _create_catalog(
        catalogs_app_client, load_test_data, parent_id=parent["id"]
    )
    etag = await _etag(catalogs_app_client, parent["id"])

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}", headers={"If-Match": etag}
    )

    assert response.status_code == 204, response.text
    # Deleting a catalog never deletes its children; they lose the parent.
    stored = await database.client.get(index=COLLECTIONS_INDEX, id=child["id"])
    assert parent["id"] not in stored["_source"].get("parent_ids", [])


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "DELETE"])
async def test_if_match_on_a_missing_catalog_returns_404(
    catalogs_app_client, load_test_data, method
):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"catalog-{uuid.uuid4()}"
    url = f"/catalogs/{catalog['id']}"

    if method == "PUT":
        response = await catalogs_app_client.put(
            url, json=catalog, headers={"If-Match": "*"}
        )
    else:
        response = await catalogs_app_client.delete(url, headers={"If-Match": "*"})

    assert response.status_code == 404, response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "DELETE"])
async def test_require_if_match_returns_428_without_it(
    catalogs_app_client, load_test_data, monkeypatch, method
):
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    url = f"/catalogs/{catalog['id']}"

    if method == "PUT":
        response = await catalogs_app_client.put(
            url, json=_replacement(catalog, "unconditional")
        )
    else:
        response = await catalogs_app_client.delete(url)

    assert response.status_code == 428, response.text
    monkeypatch.delenv("REQUIRE_IF_MATCH")
    assert await _title(catalogs_app_client, catalog["id"]) != "unconditional"


@pytest.mark.asyncio
async def test_require_if_match_leaves_creates_links_and_unlinks_alone(
    catalogs_app_client, load_test_data, monkeypatch
):
    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")
    parent, _ = await _create_catalog(catalogs_app_client, load_test_data)
    child, _ = await _create_catalog(catalogs_app_client, load_test_data)

    response = await catalogs_app_client.post(
        f"/catalogs/{parent['id']}/catalogs", json={"id": child["id"]}
    )

    assert response.status_code == 200, response.text
    assert child["id"] in await _child_ids(catalogs_app_client, parent["id"])

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}/catalogs/{child['id']}"
    )

    assert response.status_code in (200, 204), response.text
    assert child["id"] not in await _child_ids(catalogs_app_client, parent["id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "DELETE"])
async def test_a_write_between_check_and_write_returns_412(
    catalogs_app_client, load_test_data, monkeypatch, method
):
    """The ETag check and the write are one atomic step in the database."""
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    url = f"/catalogs/{catalog['id']}"
    etag = await _etag(catalogs_app_client, catalog["id"])

    client_cls = type(database.client)
    originals = {name: getattr(client_cls, name) for name in ("index", "delete")}
    raced = []

    def racing(name):
        async def call(self, *args, **kwargs):
            if "if_seq_no" in kwargs and not raced:
                raced.append(name)
                concurrent = await catalogs_app_client.put(
                    url, json=_replacement(catalog, "concurrent")
                )
                assert concurrent.status_code == 200, concurrent.text
            return await originals[name](self, *args, **kwargs)

        return call

    for name in originals:
        monkeypatch.setattr(client_cls, name, racing(name))
    if method == "PUT":
        response = await catalogs_app_client.put(
            url, json=_replacement(catalog, "lost"), headers={"If-Match": etag}
        )
    else:
        response = await catalogs_app_client.delete(url, headers={"If-Match": etag})
    for name, original in originals.items():
        monkeypatch.setattr(client_cls, name, original)

    assert raced, "the write was not conditional"
    assert response.status_code == 412, response.text
    assert await _title(catalogs_app_client, catalog["id"]) == "concurrent"


@pytest.mark.asyncio
async def test_catalog_scoped_reads_return_the_same_etag(
    catalogs_app_client, load_test_data
):
    """A collection or item read through its catalog has the same tag as read directly."""
    catalog, _ = await _create_catalog(catalogs_app_client, load_test_data)
    collection = load_test_data("test_collection.json")
    collection["id"] = f"collection-{uuid.uuid4()}"
    response = await catalogs_app_client.post(
        f"/catalogs/{catalog['id']}/collections",
        json=collection,
        headers={"Accept-Encoding": "identity"},
    )
    assert response.status_code == 201, response.text
    assert STRONG_ETAG.match(response.headers["etag"]), response.headers
    item = load_test_data("test_item.json")
    item["id"] = f"item-{uuid.uuid4()}"
    item["collection"] = collection["id"]
    response = await catalogs_app_client.post(
        f"/collections/{collection['id']}/items", json=item
    )
    assert response.status_code == 201, response.text

    for direct, scoped in (
        (
            f"/collections/{collection['id']}",
            f"/catalogs/{catalog['id']}/collections/{collection['id']}",
        ),
        (
            f"/collections/{collection['id']}/items/{item['id']}",
            f"/catalogs/{catalog['id']}/collections/{collection['id']}/items/{item['id']}",
        ),
    ):
        headers = {"Accept-Encoding": "identity"}
        first = await catalogs_app_client.get(direct, headers=headers)
        second = await catalogs_app_client.get(scoped, headers=headers)
        assert first.status_code == second.status_code == 200, second.text
        assert STRONG_ETAG.match(first.headers["etag"]), first.headers
        assert first.headers["etag"] == second.headers["etag"]


@pytest.mark.asyncio
async def test_a_delete_that_fails_after_the_catalog_is_gone_can_be_retried(
    catalogs_app_client, load_test_data, monkeypatch
):
    """The children stay linked to a removed catalog; a retry unlinks them."""
    import sys

    monkeypatch.setenv("REQUIRE_IF_MATCH", "true")  # the retry must carry If-Match
    parent, _ = await _create_catalog(catalogs_app_client, load_test_data)
    child, _ = await _create_catalog(
        catalogs_app_client, load_test_data, parent_id=parent["id"]
    )
    etag = await _etag(catalogs_app_client, parent["id"])
    module = sys.modules[type(database).__module__]
    original = module.unlink_catalog_children_shared
    failed = []

    async def fail_once(client, catalog_id):
        if not failed:
            failed.append(catalog_id)
            raise RuntimeError("cleanup lost its connection")
        return await original(client, catalog_id)

    monkeypatch.setattr(module, "unlink_catalog_children_shared", fail_once)
    with pytest.raises(RuntimeError):  # the test transport re-raises the 500
        await catalogs_app_client.delete(
            f"/catalogs/{parent['id']}", headers={"If-Match": etag}
        )
    assert failed
    assert parent["id"] in (await database.find_catalog(child["id"]))["parent_ids"]

    response = await catalogs_app_client.delete(
        f"/catalogs/{parent['id']}", headers={"If-Match": etag}
    )

    assert response.status_code == 404, response.text
    assert parent["id"] not in (await database.find_catalog(child["id"])).get(
        "parent_ids", []
    )
