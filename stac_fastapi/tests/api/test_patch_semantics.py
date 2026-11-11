"""PATCH media types, recursive merges and atomic invalid-operation regressions."""

from copy import deepcopy
from unittest.mock import AsyncMock

import jsonpatch
import pytest

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

pytestmark = [pytest.mark.datetime_filtering, pytest.mark.asyncio]


def _target(ctx, resource):
    if resource == "collection":
        return f"/collections/{ctx.collection['id']}", dict(
            index=COLLECTIONS_INDEX, id=ctx.collection["id"]
        )
    return (
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        dict(
            index=index_alias_by_collection_id(ctx.item["collection"]),
            id=mk_item_id(ctx.item["id"], ctx.item["collection"]),
        ),
    )


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("format", ["json-patch", "merge-patch"])
@pytest.mark.parametrize(
    "header",
    [
        "application/{format}+json; charset=utf-8",
        "Application/{format}+JSON; charset=UTF-8",
    ],
)
async def test_patch_accepts_case_insensitive_parameterised_media_type(
    app_client, ctx, txn_client, monkeypatch, validator, resource, format, header
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    path = "/properties/title" if resource == "item" else "/title"
    patch = (
        [{"op": "add", "path": path, "value": "patched"}]
        if format == "json-patch"
        else (
            {"properties": {"title": "patched"}}
            if resource == "item"
            else {"title": "patched"}
        )
    )
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": header.format(format=format.upper())}
    )
    assert response.status_code == 200, response.text
    stored = (await txn_client.database.client.get(**document))["_source"]
    for result in (response.json(), stored):
        assert (result["properties"] if resource == "item" else result)[
            "title"
        ] == "patched"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("initial", ["missing", "scalar", "object"])
@pytest.mark.parametrize("mode", ["nested", "empty", "null-only"])
async def test_merge_patch_recurses_preserves_siblings_and_removes_null(
    app_client, ctx, txn_client, monkeypatch, validator, resource, initial, mode
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    # This extension field stores arbitrary JSON; do not constrain its shape
    # through dynamic search mappings while exercising scalar-to-object merges.
    mapping = {"properties": {"custom": {"type": "object", "enabled": False}}}
    field = "properties" if resource == "item" else "summaries"
    mapping = {"properties": {field: mapping}}
    await txn_client.database.client.indices.put_mapping(
        index=document["index"], body=mapping
    )
    seed = deepcopy(getattr(ctx, resource))
    container = seed[field]
    container["custom_sibling"] = ["untouched"]
    container["custom_remove"] = ["remove me"]
    if initial != "missing":
        container["custom"] = (
            ("scalar" if resource == "item" else ["scalar"])
            if initial == "scalar"
            else {
                "keep": "sibling",
                "remove": "old",
                "nested": {"keep": True, "change": "old"},
            }
        )
    assert (await app_client.put(url, json=seed)).status_code == 200
    changes = {
        "custom": {
            "remove": None,
            "absent": None,
            "nested": {"change": "new", "absent": None},
        },
        "custom_remove": None,
    }
    if mode != "nested":
        changes["custom"] = {} if mode == "empty" else {"remove": None}
    patch = {field: changes}
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": "application/merge-patch+json"}
    )
    assert response.status_code == 200, response.text
    expected = deepcopy(container["custom"]) if initial == "object" else {}
    if mode != "empty":
        expected.pop("remove", None)
    if mode == "nested":
        expected.setdefault("nested", {})["change"] = "new"
    stored = (await txn_client.database.client.get(**document))["_source"]
    for result in (response.json(), stored):
        merged = result[field]
        assert merged["custom"] == expected
        assert merged["custom_sibling"] == ["untouched"]
        assert "custom_remove" not in merged
        if resource == "item":
            assert merged["datetime"] == seed["properties"]["datetime"]
        else:
            assert result["extent"] == seed["extent"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
async def test_merge_patch_preserves_literal_member_names(
    app_client, ctx, txn_client, monkeypatch, validator, resource
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    field = "properties" if resource == "item" else "summaries"
    await txn_client.database.client.indices.put_mapping(
        index=document["index"],
        body={
            "properties": {
                field: {"properties": {"custom": {"type": "object", "enabled": False}}}
            }
        },
    )
    seed = deepcopy(getattr(ctx, resource))
    sibling = {"sibling": ["untouched"]}
    seed[field]["custom"] = deepcopy(sibling)
    assert (await app_client.put(url, json=seed)).status_code == 200
    keys = ["0", "01", "-1", "a/b", "~1", "quote'\"", "back\\slash", ""]
    added = {key: {"keep": True, key: index} for index, key in enumerate(keys)}
    expected = {**sibling, **added}
    for changes, wanted in [
        (added, expected),
        ({key: {} for key in keys}, expected),
        ({key: None for key in keys}, sibling),
    ]:
        response = await app_client.patch(
            url,
            json={field: {"custom": changes}},
            headers={"Content-Type": "application/merge-patch+json"},
        )
        assert response.status_code == 200, response.text
        stored = (await txn_client.database.client.get(**document))["_source"]
        for result in (response.json(), stored):
            assert result[field]["custom"] == wanted
            assert "a" not in result[field]["custom"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("operation", ["remove", "replace"])
@pytest.mark.parametrize("member", ["absent", "scalar/0", "list/1"])
async def test_invalid_json_patch_is_400_without_mutation(
    app_client, ctx, txn_client, monkeypatch, validator, resource, operation, member
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    seed = deepcopy(getattr(ctx, resource))
    container = seed["properties"] if resource == "item" else seed
    container["scalar"] = "existing"
    container["list"] = ["only"]
    assert (await app_client.put(url, json=seed)).status_code == 200
    before = await txn_client.database.client.get(**document)
    prefix = "/properties" if resource == "item" else ""
    response = await app_client.patch(
        url,
        json=[
            {"op": "add", "path": prefix + "/title", "value": "must not persist"},
            {"op": operation, "path": prefix + "/" + member, "value": 1},
        ],
        headers={"Content-Type": "application/json-patch+json"},
    )
    assert response.status_code == 400, response.text
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize(
    "ops",
    [
        [{"op": "copy", "from": "/a/0", "path": "/a/1"}] * 2,
        [{"op": "move", "from": "/a/0", "path": "/a/1"}] * 2,
        [{"op": "remove", "path": "/a/0"}] * 2,
        [
            {"op": "copy", "from": "/b", "path": "/c"},
            {"op": "add", "path": "/c/0", "value": "new"},
        ],
        [
            {"op": "copy", "from": "/o", "path": "/p"},
            {"op": "replace", "path": "/p/k", "value": 2},
        ],
        [
            {"op": "remove", "path": "/x"},
            {"op": "replace", "path": "/x", "value": 2},
        ],
        [{"op": "replace", "path": "/l/0/k", "value": 2}],
        [{"op": "add", "path": "/l/0/m", "value": 3}],
        [{"op": "remove", "path": "/l/0/k"}],
        [{"op": "copy", "from": "/n/1", "path": "/q"}],
        [
            {"op": "test", "path": "/n/1", "value": "v"},
            {"op": "add", "path": "/q", "value": 1},
        ],
    ],
    ids=[
        "copy-twice",
        "move-twice",
        "remove-twice",
        "copy-add",
        "copy-replace",
        "remove-replace",
        "replace-in-array-element",
        "add-in-array-element",
        "remove-in-array-element",
        "copy-numeric-member",
        "test-numeric-member",
    ],
)
async def test_json_patch_applies_operations_in_order_on_copies(
    app_client, ctx, txn_client, monkeypatch, validator, resource, ops
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    seed = deepcopy(getattr(ctx, resource))
    fields = {
        "a": ["x", "y", "z"],
        "b": ["x"],
        "o": {"k": 1},
        "x": 1,
        "l": [{"k": 1}],
        "n": {"1": "v"},
    }
    (seed["properties"] if resource == "item" else seed).update(deepcopy(fields))
    assert (await app_client.put(url, json=seed)).status_code == 200
    before = await txn_client.database.client.get(**document)
    prefix = "/properties" if resource == "item" else ""
    patch = [
        {**op, **{key: prefix + op[key] for key in ("path", "from") if key in op}}
        for op in ops
    ]
    try:
        expected = jsonpatch.apply_patch(fields, ops)
    except jsonpatch.JsonPatchConflict:
        expected = None

    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": "application/json-patch+json"}
    )

    after = await txn_client.database.client.get(**document)
    if expected is None:
        assert response.status_code == 400, response.text
        assert after["_source"] == before["_source"]
        return
    assert response.status_code == 200, response.text
    for result in (response.json(), after["_source"]):
        container = result["properties"] if resource == "item" else result
        assert {key: container.get(key) for key in "abcopxlnq"} == {
            key: expected.get(key) for key in "abcopxlnq"
        }


async def test_validation_typeerror_is_not_reclassified(txn_client, ctx, monkeypatch):
    validation = AsyncMock(side_effect=TypeError("validation defect"))
    monkeypatch.setattr(txn_client, "_validate_single_item", validation)
    with pytest.raises(TypeError, match="validation defect"):
        await txn_client._apply_and_validate_patch(
            ctx.item,
            [{"op": "add", "path": "/properties/title", "value": "patched"}],
            "application/json-patch+json",
        )
    validation.assert_awaited_once()


@pytest.mark.parametrize("validator", ["false", "true"])
async def test_json_patch_add_dash_appends_to_array(
    app_client, ctx, txn_client, monkeypatch, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "item")
    first = {"rel": "cite-as", "href": "https://doi.org/10.1234/first"}
    second = {"rel": "describedby", "href": "https://example.com/errata"}
    before = (await txn_client.database.client.get(**document))["_source"]["links"]

    response = await app_client.patch(
        url,
        json=[
            {"op": "add", "path": "/links/-", "value": first},
            {"op": "add", "path": "/links/-", "value": second},
        ],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert response.status_code == 200, response.text
    stored = (await txn_client.database.client.get(**document))["_source"]["links"]
    # With the validator on, the item is serialised again and its "collection"
    # link is regenerated, so look at the appended links only.
    appended = [link for link in stored if link["rel"] in (first["rel"], second["rel"])]
    assert appended == [first, second]
    assert len(stored) == len(before) + 2
    if validator == "false":
        assert stored == before + [first, second]


async def test_json_patch_add_dash_appends_to_properties_array(
    app_client, ctx, txn_client
):
    url, document = _target(ctx, "item")
    seed = deepcopy(ctx.item)
    seed["properties"]["tags"] = ["a", "b"]
    assert (await app_client.put(url, json=seed)).status_code == 200

    response = await app_client.patch(
        url,
        json=[{"op": "add", "path": "/properties/tags/-", "value": "c"}],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert response.status_code == 200, response.text
    stored = (await txn_client.database.client.get(**document))["_source"]
    assert stored["properties"]["tags"] == ["a", "b", "c"]


async def _seed_numeric_members(app_client, ctx, txn_client, monkeypatch):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    url, document = _target(ctx, "item")
    seed = deepcopy(ctx.item)
    seed["properties"]["01"] = "zero-one"
    seed["properties"]["-"] = "dash"
    assert (await app_client.put(url, json=seed)).status_code == 200
    return url, document


async def _patch(app_client, url, operations):
    return await app_client.patch(
        url, json=operations, headers={"Content-Type": "application/json-patch+json"}
    )


@pytest.mark.parametrize("member", ["01", "-"])
async def test_json_patch_replace_numeric_member_of_object(
    app_client, ctx, txn_client, monkeypatch, member
):
    url, document = await _seed_numeric_members(
        app_client, ctx, txn_client, monkeypatch
    )

    response = await _patch(
        app_client,
        url,
        [{"op": "replace", "path": f"/properties/{member}", "value": "new"}],
    )

    assert response.status_code == 200, response.text
    properties = (await txn_client.database.client.get(**document))["_source"][
        "properties"
    ]
    assert properties[member] == "new"
    assert "1" not in properties and "-1" not in properties


@pytest.mark.parametrize("member", ["02", "-"])
async def test_json_patch_replace_missing_numeric_member_of_object_fails(
    app_client, ctx, txn_client, monkeypatch, member
):
    url, document = _target(ctx, "item")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    before = (await txn_client.database.client.get(**document))["_source"]

    response = await _patch(
        app_client,
        url,
        [{"op": "replace", "path": f"/properties/{member}", "value": "new"}],
    )

    assert response.status_code == 400, response.text
    after = (await txn_client.database.client.get(**document))["_source"]
    assert after == before


@pytest.mark.parametrize("member", ["01", "-"])
async def test_json_patch_remove_numeric_member_of_object(
    app_client, ctx, txn_client, monkeypatch, member
):
    url, document = await _seed_numeric_members(
        app_client, ctx, txn_client, monkeypatch
    )

    response = await _patch(
        app_client, url, [{"op": "remove", "path": f"/properties/{member}"}]
    )

    assert response.status_code == 200, response.text
    properties = (await txn_client.database.client.get(**document))["_source"][
        "properties"
    ]
    assert member not in properties


@pytest.mark.parametrize("member,value", [("01", "zero-one"), ("-", "dash")])
@pytest.mark.parametrize("op", ["copy", "move"])
async def test_json_patch_copy_and_move_from_numeric_member_of_object(
    app_client, ctx, txn_client, monkeypatch, op, member, value
):
    url, document = await _seed_numeric_members(
        app_client, ctx, txn_client, monkeypatch
    )

    response = await _patch(
        app_client,
        url,
        [{"op": op, "from": f"/properties/{member}", "path": "/properties/target"}],
    )

    assert response.status_code == 200, response.text
    properties = (await txn_client.database.client.get(**document))["_source"][
        "properties"
    ]
    assert properties["target"] == value
    assert (member in properties) == (op == "copy")


@pytest.mark.parametrize(
    "member,value,status",
    [
        ("01", "zero-one", 200),
        ("01", "other", 400),
        ("-", "dash", 200),
        ("-", "other", 400),
    ],
)
async def test_json_patch_test_numeric_member_of_object(
    app_client, ctx, txn_client, monkeypatch, member, value, status
):
    url, _ = await _seed_numeric_members(app_client, ctx, txn_client, monkeypatch)

    response = await _patch(
        app_client,
        url,
        [{"op": "test", "path": f"/properties/{member}", "value": value}],
    )

    assert response.status_code == status, response.text


async def test_json_patch_remove_member_with_dash_in_path(
    app_client, ctx, txn_client, monkeypatch
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    url, document = _target(ctx, "item")
    seed = deepcopy(ctx.item)
    seed["assets"]["x-y.nc"] = {"href": "https://example.com/x-y.nc", "title": "t"}
    assert (await app_client.put(url, json=seed)).status_code == 200

    response = await _patch(
        app_client, url, [{"op": "remove", "path": "/assets/x-y.nc/title"}]
    )

    assert response.status_code == 200, response.text
    assets = (await txn_client.database.client.get(**document))["_source"]["assets"]
    assert assets["x-y.nc"] == {"href": "https://example.com/x-y.nc"}


# RFC 6902 section 4.1: "-" is the position after the last element of a list.
# Only "add" may use it; for every other operation, and as a "from" path, it
# refers to a value that does not exist, so the patch must fail (sections 4.2-4.6).
_DASH_NON_ADD_OPERATIONS = {
    "test": {"op": "test", "path": "/properties/tags/-", "value": "a"},
    "replace": {"op": "replace", "path": "/properties/tags/-", "value": "x"},
    "remove": {"op": "remove", "path": "/properties/tags/-"},
    "copy": {"op": "copy", "from": "/properties/tags/-", "path": "/properties/copied"},
    "move": {"op": "move", "from": "/properties/tags/-", "path": "/properties/moved"},
}


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("tags", [["a"], []], ids=["non-empty", "empty"])
@pytest.mark.parametrize("op", list(_DASH_NON_ADD_OPERATIONS))
async def test_json_patch_dash_index_with_non_add_operation_does_not_exist(
    app_client, ctx, txn_client, monkeypatch, validator, tags, op
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "item")
    seed = deepcopy(ctx.item)
    seed["properties"]["tags"] = tags
    assert (await app_client.put(url, json=seed)).status_code == 200
    before = await txn_client.database.client.get(**document)

    response = await _patch(app_client, url, [_DASH_NON_ADD_OPERATIONS[op]])

    assert response.status_code == 400, response.text
    if validator == "false":
        # The message from Debug.explain, not an out-of-bounds script error.
        # Checking response.text is not enough: it echoes the script source,
        # which contains every Debug.explain message.
        detail = response.json()["detail"]
        assert detail.get("to_string") == "- does not exist in properties/tags", detail
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "operation",
    [
        {"op": "add", "path": "/links/-/title", "value": "x"},
        {"op": "replace", "path": "/links/-/href", "value": "x"},
        {"op": "remove", "path": "/links/-/href"},
        {"op": "test", "path": "/links/-/href", "value": "x"},
        {"op": "copy", "from": "/links/-/href", "path": "/properties/copied"},
    ],
    ids=lambda operation: operation["op"],
)
async def test_json_patch_dash_index_inside_a_path_does_not_exist(
    app_client, ctx, txn_client, monkeypatch, validator, operation
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "item")
    before = await txn_client.database.client.get(**document)

    response = await _patch(app_client, url, [operation])

    assert response.status_code == 400, response.text
    if validator == "false":
        detail = response.json()["detail"]
        pointer = operation.get("from", operation["path"]).lstrip("/")
        assert detail.get("to_string") == f"- in {pointer} does not exist", detail
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


# A member lookup on a value that is not an object (a list or a scalar) refers to
# a value that does not exist; it must not reach containsKey on the wrong type.
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "operation,expected",
    [
        (
            {"op": "remove", "path": "/links/title/x"},
            "title in links/title/x does not exist",
        ),
        (
            {"op": "replace", "path": "/links/-1/x", "value": 1},
            "-1 in links/-1/x does not exist",
        ),
        (
            {"op": "remove", "path": "/links/foo"},
            "foo does not exist in links",
        ),
        (
            {"op": "test", "path": "/links/foo", "value": 1},
            "foo does not exist in links",
        ),
        (
            {"op": "remove", "path": "/properties/datetime/x"},
            "x does not exist in properties/datetime",
        ),
        (
            {"op": "replace", "path": "/properties/datetime/x", "value": 1},
            "x does not exist in properties/datetime",
        ),
        (
            {"op": "copy", "from": "/links/foo", "path": "/properties/copied"},
            "foo does not exist in links",
        ),
        (
            {"op": "move", "from": "/links/foo", "path": "/properties/moved"},
            "foo does not exist in links",
        ),
        (
            {"op": "remove", "path": "/properties/datetime/0"},
            "0 does not exist in properties/datetime",
        ),
        (
            {"op": "replace", "path": "/properties/datetime/-1", "value": 1},
            "-1 does not exist in properties/datetime",
        ),
    ],
    ids=[
        "list-member-in-path",
        "negative-index-in-path",
        "remove-list-member",
        "test-list-member",
        "remove-member-of-scalar",
        "replace-member-of-scalar",
        "copy-from-list-member",
        "move-from-list-member",
        "remove-index-of-scalar",
        "replace-negative-index-of-scalar",
    ],
)
async def test_json_patch_member_of_non_object_does_not_exist(
    app_client, ctx, txn_client, monkeypatch, validator, operation, expected
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "item")
    before = await txn_client.database.client.get(**document)

    response = await _patch(app_client, url, [operation])

    assert response.status_code == 400, response.text
    if validator == "false":
        detail = response.json()["detail"]
        assert detail.get("to_string") == expected, detail
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.parametrize(
    "operation,expected",
    [
        ({"op": "replace", "path": "/properties/-/x", "value": 2}, {"x": 2}),
        ({"op": "remove", "path": "/properties/-/x"}, {}),
        ({"op": "add", "path": "/properties/-/y", "value": 3}, {"x": 1, "y": 3}),
    ],
    ids=["replace", "remove", "add"],
)
async def test_json_patch_dash_member_of_object_inside_a_path(
    app_client, ctx, txn_client, monkeypatch, operation, expected
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    url, document = _target(ctx, "item")
    seed = deepcopy(ctx.item)
    seed["properties"]["-"] = {"x": 1}
    assert (await app_client.put(url, json=seed)).status_code == 200

    response = await _patch(app_client, url, [operation])

    assert response.status_code == 200, response.text
    properties = (await txn_client.database.client.get(**document))["_source"][
        "properties"
    ]
    assert properties["-"] == expected
