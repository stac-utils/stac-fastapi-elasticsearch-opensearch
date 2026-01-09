import json

from stac_fastapi.sfeos_helpers.mappings import get_items_mappings


class TestMappingsFile:
    def test_mappings_file_applied(self, monkeypatch, tmp_path):
        """Test that mappings are read from file when env var is set."""
        custom_mappings = {
            "properties": {"properties": {"file_field": {"type": "keyword"}}}
        }
        mappings_file = tmp_path / "mappings.json"
        mappings_file.write_text(json.dumps(custom_mappings))

        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", str(mappings_file))
        monkeypatch.delenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", raising=False)

        mappings = get_items_mappings()

        assert mappings["properties"]["properties"]["file_field"] == {"type": "keyword"}

    def test_env_var_precedence(self, monkeypatch, tmp_path):
        """Test that STAC_FASTAPI_ES_CUSTOM_MAPPINGS takes precedence over file."""
        file_mappings = {
            "properties": {"properties": {"shared_field": {"type": "keyword"}}}
        }
        mappings_file = tmp_path / "mappings.json"
        mappings_file.write_text(json.dumps(file_mappings))

        env_mappings = {
            "properties": {"properties": {"shared_field": {"type": "text"}}}
        }

        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", str(mappings_file))
        monkeypatch.setenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", json.dumps(env_mappings))

        mappings = get_items_mappings()

        assert mappings["properties"]["properties"]["shared_field"] == {"type": "text"}

    def test_missing_file_handled_gracefully(self, monkeypatch, caplog):
        """Test that missing file is logged and ignored."""
        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", "/non/existent/file.json")
        monkeypatch.delenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", raising=False)

        get_items_mappings()

        assert "Failed to read STAC_FASTAPI_ES_MAPPINGS_FILE" in caplog.text

    def test_invalid_json_in_file(self, monkeypatch, tmp_path, caplog):
        """Test that invalid JSON in file is logged and ignored."""
        mappings_file = tmp_path / "invalid.json"
        mappings_file.write_text("{this is not valid json}")

        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", str(mappings_file))
        monkeypatch.delenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", raising=False)

        get_items_mappings()

        assert "Failed to parse STAC_FASTAPI_ES_CUSTOM_MAPPINGS JSON" in caplog.text

    def test_file_and_env_var_both_set(self, monkeypatch, tmp_path):
        """Test that env var completely overrides file when both are set."""
        file_mappings = {
            "properties": {
                "properties": {
                    "file_only_field": {"type": "keyword"},
                    "shared_field": {"type": "text"},
                }
            }
        }
        mappings_file = tmp_path / "mappings.json"
        mappings_file.write_text(json.dumps(file_mappings))

        env_mappings = {
            "properties": {
                "properties": {
                    "env_only_field": {"type": "keyword"},
                    "shared_field": {"type": "integer"},
                }
            }
        }

        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", str(mappings_file))
        monkeypatch.setenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", json.dumps(env_mappings))

        mappings = get_items_mappings()

        # Only env var fields should be present
        assert "env_only_field" in mappings["properties"]["properties"]
        assert "file_only_field" not in mappings["properties"]["properties"]
        assert mappings["properties"]["properties"]["shared_field"] == {
            "type": "integer"
        }

    def test_empty_file_handled_gracefully(self, monkeypatch, tmp_path):
        """Test that empty file is handled without error."""
        mappings_file = tmp_path / "empty.json"
        mappings_file.write_text("")

        monkeypatch.setenv("STAC_FASTAPI_ES_MAPPINGS_FILE", str(mappings_file))
        monkeypatch.delenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS", raising=False)

        # Should not raise, just use default mappings
        mappings = get_items_mappings()
        assert "properties" in mappings
