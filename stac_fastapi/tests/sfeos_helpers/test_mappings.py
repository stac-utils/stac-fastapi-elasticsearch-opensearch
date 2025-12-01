"""Tests for custom mappings configuration.

These tests verify the STAC_FASTAPI_ES_CUSTOM_MAPPINGS and
STAC_FASTAPI_ES_DYNAMIC_MAPPING environment variable functionality.
"""

import json

import pytest

from stac_fastapi.sfeos_helpers.mappings import (
    apply_custom_mappings,
    get_items_mappings,
    merge_mappings,
    parse_dynamic_mapping_config,
)


class TestMergeMappings:
    """Tests for the merge_mappings function."""

    def test_recursive_merge_preserves_existing_and_adds_new(self):
        """Test recursive merging preserves existing keys and adds new ones at all levels."""
        base = {
            "properties": {
                "properties": {
                    "datetime": {"type": "date_nanos"},
                    "created": {"type": "date"},
                }
            }
        }
        custom = {"properties": {"properties": {"custom_field": {"type": "keyword"}}}}
        merge_mappings(base, custom)

        # Existing fields preserved
        assert base["properties"]["properties"]["datetime"] == {"type": "date_nanos"}
        assert base["properties"]["properties"]["created"] == {"type": "date"}
        # New field added
        assert base["properties"]["properties"]["custom_field"] == {"type": "keyword"}

    def test_custom_overwrites_on_key_collision(self):
        """Test that custom values overwrite base values when keys collide."""
        base = {"level1": {"a": {"type": "date_nanos"}}}
        custom = {"level1": {"a": {"type": "date"}}}
        merge_mappings(base, custom)
        assert base["level1"]["a"] == {"type": "date"}

    @pytest.mark.parametrize(
        "base,custom,expected",
        [
            # Dict replaces non-dict
            ({"a": "string"}, {"a": {"nested": "dict"}}, {"a": {"nested": "dict"}}),
            # Non-dict replaces dict
            ({"a": {"nested": "dict"}}, {"a": "string"}, {"a": "string"}),
        ],
        ids=["dict_replaces_non_dict", "non_dict_replaces_dict"],
    )
    def test_type_replacement(self, base, custom, expected):
        """Test that values are replaced when types don't match for merging."""
        merge_mappings(base, custom)
        assert base == expected


class TestParseDynamicMappingConfig:
    """Tests for the parse_dynamic_mapping_config function."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            (None, True),
            ("true", True),
            ("TRUE", True),
            ("True", True),
            ("false", False),
            ("FALSE", False),
            ("False", False),
            ("strict", "strict"),
            ("STRICT", "strict"),
            ("runtime", "runtime"),
        ],
        ids=[
            "none_defaults_true",
            "true_lowercase",
            "true_uppercase",
            "true_mixed",
            "false_lowercase",
            "false_uppercase",
            "false_mixed",
            "strict_lowercase",
            "strict_uppercase",
            "other_value",
        ],
    )
    def test_parse_dynamic_mapping_config(self, input_value, expected):
        """Test dynamic mapping config parsing for various inputs."""
        assert parse_dynamic_mapping_config(input_value) == expected


class TestApplyCustomMappings:
    """Tests for the apply_custom_mappings function."""

    @pytest.mark.parametrize(
        "custom_json",
        [None, ""],
        ids=["none", "empty_string"],
    )
    def test_no_op_for_empty_input(self, custom_json):
        """Test that None or empty string leaves mappings unchanged."""
        mappings = {"properties": {"id": {"type": "keyword"}}}
        original = {"properties": {"id": {"type": "keyword"}}}
        apply_custom_mappings(mappings, custom_json)
        assert mappings == original

    def test_merges_valid_json(self):
        """Test that valid JSON custom mappings are merged into properties."""
        mappings = {
            "properties": {
                "properties": {"properties": {"datetime": {"type": "date_nanos"}}}
            }
        }
        custom_json = json.dumps(
            {"properties": {"properties": {"sar:frequency_band": {"type": "keyword"}}}}
        )
        apply_custom_mappings(mappings, custom_json)

        assert mappings["properties"]["properties"]["properties"]["datetime"] == {
            "type": "date_nanos"
        }
        assert mappings["properties"]["properties"]["properties"][
            "sar:frequency_band"
        ] == {"type": "keyword"}

    def test_invalid_json_logs_error_and_preserves_mappings(self, caplog):
        """Test that invalid JSON logs an error and doesn't modify mappings."""
        mappings = {"properties": {"id": {"type": "keyword"}}}
        original = {"properties": {"id": {"type": "keyword"}}}
        apply_custom_mappings(mappings, "not valid json")
        assert mappings == original
        assert "Failed to parse STAC_FASTAPI_ES_CUSTOM_MAPPINGS JSON" in caplog.text


class TestGetItemsMappings:
    """Tests for the get_items_mappings function."""

    @pytest.mark.parametrize(
        "dynamic_mapping,expected",
        [
            ("true", True),
            ("false", False),
            ("strict", "strict"),
        ],
        ids=["dynamic_true", "dynamic_false", "dynamic_strict"],
    )
    def test_dynamic_mapping_values(self, dynamic_mapping, expected):
        """Test dynamic mapping configuration with various values."""
        mappings = get_items_mappings(dynamic_mapping=dynamic_mapping)
        assert mappings["dynamic"] == expected

    def test_custom_mappings_merged_preserving_defaults(self):
        """Test that custom mappings are merged while preserving default fields."""
        custom = json.dumps(
            {"properties": {"properties": {"custom:field": {"type": "keyword"}}}}
        )
        mappings = get_items_mappings(custom_mappings=custom)

        # Custom field added
        assert mappings["properties"]["properties"]["properties"]["custom:field"] == {
            "type": "keyword"
        }
        # Default fields preserved
        assert mappings["properties"]["id"] == {"type": "keyword"}
        assert mappings["properties"]["geometry"] == {"type": "geo_shape"}
        assert mappings["properties"]["properties"]["properties"]["datetime"] == {
            "type": "date_nanos"
        }

    def test_custom_can_override_defaults(self):
        """Test that custom mappings can override default field types."""
        custom = json.dumps(
            {"properties": {"properties": {"datetime": {"type": "date"}}}}
        )
        mappings = get_items_mappings(custom_mappings=custom)
        assert mappings["properties"]["properties"]["properties"]["datetime"] == {
            "type": "date"
        }

    def test_returns_independent_copies(self):
        """Test that each call returns a new independent copy of mappings."""
        mappings1 = get_items_mappings()
        mappings2 = get_items_mappings()
        mappings1["properties"]["test"] = "value"
        assert "test" not in mappings2["properties"]

    def test_has_required_base_structure(self):
        """Test that returned mappings have required base structure."""
        mappings = get_items_mappings()
        assert "numeric_detection" in mappings
        assert "dynamic_templates" in mappings
        assert all(
            key in mappings["properties"] for key in ["id", "collection", "geometry"]
        )


class TestSTACExtensionUseCases:
    """Integration tests for real-world STAC extension use cases."""

    @pytest.mark.parametrize(
        "extension_name,custom_fields",
        [
            (
                "sar",
                {
                    "properties": {
                        "properties": {
                            "sar:frequency_band": {"type": "keyword"},
                            "sar:center_frequency": {"type": "float"},
                            "sar:polarizations": {"type": "keyword"},
                        }
                    }
                },
            ),
            (
                "cube",
                {
                    "properties": {
                        "properties": {
                            "cube:dimensions": {"type": "object", "enabled": False},
                            "cube:variables": {"type": "object", "enabled": False},
                        }
                    }
                },
            ),
        ],
        ids=["sar_extension", "cube_extension"],
    )
    def test_add_extension_fields(self, extension_name, custom_fields):
        """Test adding STAC extension fields via custom mappings."""
        mappings = get_items_mappings(custom_mappings=json.dumps(custom_fields))

        props = mappings["properties"]["properties"]["properties"]
        for field_name, field_config in custom_fields["properties"][
            "properties"
        ].items():
            assert props[field_name] == field_config
        # Default fields still present
        assert props["datetime"] == {"type": "date_nanos"}

    def test_performance_optimization_with_disabled_dynamic_mapping(self):
        """Test disabling dynamic mapping with selective field indexing."""
        query_fields = {
            "properties": {
                "properties": {
                    "platform": {"type": "keyword"},
                    "eo:cloud_cover": {"type": "float"},
                }
            }
        }
        mappings = get_items_mappings(
            dynamic_mapping="false", custom_mappings=json.dumps(query_fields)
        )

        assert mappings["dynamic"] is False
        props = mappings["properties"]["properties"]["properties"]
        assert props["platform"] == {"type": "keyword"}
        assert props["eo:cloud_cover"] == {"type": "float"}
