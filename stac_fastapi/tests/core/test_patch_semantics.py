"""Unit coverage for RFC 7386 merge-patch semantics."""

from stac_fastapi.core.utilities import json_merge_patch


def test_merge_patch_recurses_and_removes_null_members():
    target = {"properties": {"title": "old", "datetime": "now"}, "keep": True}

    result = json_merge_patch(
        target, {"properties": {"title": "new", "datetime": None}}
    )

    assert result == {"properties": {"title": "new"}, "keep": True}


def test_merge_patch_uses_fresh_objects_for_absent_and_scalar_members():
    assert json_merge_patch({}, {"custom": {"removed": None}}) == {"custom": {}}
    assert json_merge_patch({"custom": "scalar"}, {"custom": {"value": 1}}) == {
        "custom": {"value": 1}
    }
