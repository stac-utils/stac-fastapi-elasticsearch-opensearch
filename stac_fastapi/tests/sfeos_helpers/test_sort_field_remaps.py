import stac_fastapi.sfeos_helpers.database.query as query_module
from stac_fastapi.sfeos_helpers.database.query import (
    _detect_keyword_sort_remaps_from_mapping,
    populate_sort_shared,
    remap_sort_field_shared,
)


class _SortField:
    def __init__(self, field: str, direction: str = "asc"):
        self.field = field
        self.direction = direction


def test_remap_sort_field_shared_global(monkeypatch):
    monkeypatch.setattr(
        query_module,
        "MANUAL_SORT_FIELD_REMAPS",
        {"title": "title.keyword"},
    )
    monkeypatch.setattr(query_module, "AUTO_SORT_FIELD_REMAPS", {})

    assert remap_sort_field_shared("title") == "title.keyword"
    assert remap_sort_field_shared("id") == "id"


def test_remap_sort_field_shared_collections_override(monkeypatch):
    monkeypatch.setattr(
        query_module,
        "MANUAL_SORT_FIELD_REMAPS",
        {"title": "global_title.keyword"},
    )
    monkeypatch.setattr(
        query_module,
        "MANUAL_COLLECTIONS_SORT_FIELD_REMAPS",
        {"title": "title.keyword"},
    )

    assert remap_sort_field_shared("title", is_collection=True) == "title.keyword"


def test_remap_sort_field_shared_invalid_json_is_ignored(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(query_module, "AUTO_SORT_FIELD_REMAPS", {})

    assert remap_sort_field_shared("title") == "title"


def test_populate_sort_shared_applies_collection_remap(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(
        query_module,
        "MANUAL_COLLECTIONS_SORT_FIELD_REMAPS",
        {"title": "title.keyword"},
    )
    monkeypatch.setattr(query_module, "AUTO_COLLECTIONS_SORT_FIELD_REMAPS", {})

    sort = populate_sort_shared([_SortField("title", "asc")], is_collection=True)

    assert "title.keyword" in sort
    assert sort["title.keyword"]["order"] == "asc"
    assert "id" in sort


def test_populate_sort_shared_default_without_remap(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(query_module, "MANUAL_COLLECTIONS_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(query_module, "AUTO_COLLECTIONS_SORT_FIELD_REMAPS", {})

    sort = populate_sort_shared([_SortField("title", "asc")], is_collection=True)

    assert "title" in sort
    assert "title.keyword" not in sort


def test_detect_keyword_sort_remaps_from_mapping():
    mapping = {
        "properties": {
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "description": {"type": "text"},
            "nested": {
                "properties": {
                    "label": {
                        "type": "text",
                        "fields": {"raw": {"type": "keyword"}},
                    }
                }
            },
        }
    }

    remaps = _detect_keyword_sort_remaps_from_mapping(mapping)

    assert remaps["title"] == "title.keyword"
    assert remaps["nested.label"] == "nested.label.raw"
    assert "description" not in remaps


def test_remap_sort_field_shared_uses_auto_detection_for_collections(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(query_module, "MANUAL_COLLECTIONS_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(
        query_module,
        "AUTO_COLLECTIONS_SORT_FIELD_REMAPS",
        {"title": "title.keyword"},
    )

    assert remap_sort_field_shared("title", is_collection=True) == "title.keyword"


def test_manual_collection_remap_overrides_auto_detection(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(
        query_module,
        "MANUAL_COLLECTIONS_SORT_FIELD_REMAPS",
        {"title": "title.sort"},
    )
    monkeypatch.setattr(
        query_module,
        "AUTO_COLLECTIONS_SORT_FIELD_REMAPS",
        {"title": "title.keyword"},
    )

    assert remap_sort_field_shared("title", is_collection=True) == "title.sort"


def test_env_changes_after_import_do_not_change_remaps(monkeypatch):
    monkeypatch.setattr(query_module, "MANUAL_SORT_FIELD_REMAPS", {})
    monkeypatch.setattr(query_module, "AUTO_SORT_FIELD_REMAPS", {})
    monkeypatch.setenv("STAC_FASTAPI_SORT_FIELD_REMAPS", '{"title":"title.keyword"}')

    assert remap_sort_field_shared("title") == "title"
