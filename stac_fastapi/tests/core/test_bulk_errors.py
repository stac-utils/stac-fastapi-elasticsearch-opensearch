"""Unit tests for bulk transaction error serialization."""

from stac_fastapi.core.utilities import format_bulk_errors


def test_format_bulk_errors_preserves_item_id_and_reason():
    errors = [
        {
            "create": {
                "_id": "item-1|collection-1",
                "status": 409,
                "error": {"type": "conflict", "reason": "already exists"},
            }
        },
        {
            "index": {
                "_id": "item-2|collection-1",
                "status": 400,
                "error": {"type": "mapper", "reason": "invalid field"},
            }
        },
    ]

    assert format_bulk_errors(errors) == [
        {"id": "item-1", "msg": "already exists"},
        {"id": "item-2", "msg": "invalid field"},
    ]
