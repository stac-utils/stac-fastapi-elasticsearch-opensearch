import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from stac_fastapi.core.queryables import (
    QueryablesCache,
    get_properties_from_cql2_filter,
)


class TestQueryablesCache:
    @pytest.fixture
    def mock_db_logic(self):
        db_logic = MagicMock()
        db_logic.get_queryables_mapping = AsyncMock(
            return_value={"prop1": "type1", "prop2": "type2"}
        )
        return db_logic

    @pytest.fixture
    def queryables_cache(self, mock_db_logic):
        with patch.dict(
            os.environ, {"VALIDATE_QUERYABLES": "true", "QUERYABLES_CACHE_TTL": "60"}
        ):
            cache = QueryablesCache(mock_db_logic)
            return cache

    def test_init(self, mock_db_logic):
        with patch.dict(
            os.environ, {"VALIDATE_QUERYABLES": "true", "QUERYABLES_CACHE_TTL": "120"}
        ):
            cache = QueryablesCache(mock_db_logic)
            assert cache.validation_enabled is True
            assert cache.cache_ttl == 120

    def test_reload_settings(self, queryables_cache):
        with patch.dict(
            os.environ, {"VALIDATE_QUERYABLES": "false", "QUERYABLES_CACHE_TTL": "300"}
        ):
            queryables_cache.reload_settings()
            assert queryables_cache.validation_enabled is False
            assert queryables_cache.cache_ttl == 300

    @pytest.mark.asyncio
    async def test_get_all_queryables_updates_cache(
        self, queryables_cache, mock_db_logic
    ):
        queryables = await queryables_cache.get_all_queryables()
        assert queryables == {"prop1", "prop2"}
        mock_db_logic.get_queryables_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_all_queryables_uses_cache(self, queryables_cache, mock_db_logic):
        await queryables_cache.get_all_queryables()
        mock_db_logic.get_queryables_mapping.assert_called_once()

        # Should use cache now
        await queryables_cache.get_all_queryables()
        mock_db_logic.get_queryables_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_all_queryables_refresh_after_ttl(
        self, queryables_cache, mock_db_logic
    ):
        await queryables_cache.get_all_queryables()
        mock_db_logic.get_queryables_mapping.assert_called_once()

        # Simulate time passing
        queryables_cache._last_updated = time.time() - queryables_cache.cache_ttl - 1

        await queryables_cache.get_all_queryables()
        assert mock_db_logic.get_queryables_mapping.call_count == 2

    @pytest.mark.asyncio
    async def test_get_all_queryables_disabled(self, queryables_cache):
        queryables_cache.validation_enabled = False
        queryables = await queryables_cache.get_all_queryables()
        assert queryables == set()

    @pytest.mark.asyncio
    async def test_validate_valid_fields(self, queryables_cache):
        await queryables_cache.validate({"prop1"})

    @pytest.mark.asyncio
    async def test_validate_invalid_fields(self, queryables_cache):
        with pytest.raises(HTTPException) as excinfo:
            await queryables_cache.validate({"invalid_prop"})
        assert excinfo.value.status_code == 400
        assert "Invalid query fields: invalid_prop" in str(excinfo.value.detail)

    @pytest.mark.asyncio
    async def test_validate_disabled(self, queryables_cache):
        queryables_cache.validation_enabled = False
        await queryables_cache.validate({"invalid_prop"})


def test_get_properties_from_cql2_filter():
    # Simple prop
    cql2 = {"op": "=", "args": [{"property": "prop1"}, "value"]}
    props = get_properties_from_cql2_filter(cql2)
    assert props == {"prop1"}

    # Nested props
    cql2_nested = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "prop1"}, "v1"]},
            {"op": "<", "args": [{"property": "prop2"}, 10]},
        ],
    }
    props = get_properties_from_cql2_filter(cql2_nested)
    assert props == {"prop1", "prop2"}

    # Empty/invalid
    assert get_properties_from_cql2_filter({}) == set()


def test_get_properties_from_cql2_filter_strips_properties_prefix():
    """Test that 'properties.' prefix is stripped from property names."""
    # Single property with prefix
    cql2 = {"op": "<", "args": [{"property": "properties.none"}, 5]}
    props = get_properties_from_cql2_filter(cql2)
    assert props == {"none"}

    # Mixed with and without prefix
    cql2_nested = {
        "op": "and",
        "args": [
            {"op": "=", "args": [{"property": "properties.test"}, "v1"]},
            {"op": "<", "args": [{"property": "eo:cloud_cover"}, 10]},
        ],
    }
    props = get_properties_from_cql2_filter(cql2_nested)
    assert props == {"test", "eo:cloud_cover"}
