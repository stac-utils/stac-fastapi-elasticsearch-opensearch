"""Unit tests for stac_fastapi.core.utilities."""

from stac_fastapi.core.utilities import format_bulk_errors, json_merge_patch


def test_json_merge_patch_replaces_scalars_and_removes_on_null():
    target = {"a": 1, "b": 2}
    assert json_merge_patch(target, {"a": 3, "b": None}) == {"a": 3}


def test_json_merge_patch_recurses_into_existing_object():
    target = {"properties": {"title": "old", "datetime": "now"}}
    json_merge_patch(target, {"properties": {"title": "new"}})
    assert target == {"properties": {"title": "new", "datetime": "now"}}


def test_json_merge_patch_recurses_into_fresh_object_for_non_object_member():
    """RFC 7386 §2: an object patch over an absent or scalar member starts empty."""
    assert json_merge_patch({}, {"custom": {"a": None}}) == {"custom": {}}
    assert json_merge_patch({"custom": "scalar"}, {"custom": {"a": None}}) == {
        "custom": {}
    }


def test_format_bulk_errors_maps_actions_to_id_and_msg():
    errors = [
        {
            "create": {
                "_id": "item-1|collection-1",
                "status": 409,
                "error": {
                    "type": "version_conflict_engine_exception",
                    "reason": "boom",
                },
            }
        }
    ]
    assert format_bulk_errors(errors) == [{"id": "item-1", "msg": "boom"}]
