"""Tests for the mapping module."""

import pytest

from stac_fastapi.sfeos_helpers.database.mapping import get_queryables_mapping_shared


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_simple():
    """Test basic mapping extraction."""
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "collection": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "datetime": {"type": "date"},
                            "eo:cloud_cover": {"type": "float"},
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    assert "id" in result
    assert result["id"] == "id"
    assert "collection" in result
    assert result["collection"] == "collection"
    assert "datetime" in result
    assert result["datetime"] == "properties.datetime"
    assert "eo:cloud_cover" in result
    assert result["eo:cloud_cover"] == "properties.eo:cloud_cover"


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_nested_properties():
    """Test that nested properties are properly traversed.

    This tests the case where a property like 'processing:software.eometadatatool'
    exists, which is represented as a nested object in Elasticsearch/OpenSearch.
    """
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "processing:software": {
                                "properties": {
                                    "eometadatatool": {"type": "keyword"},
                                    "version": {"type": "keyword"},
                                }
                            },
                            "eo:cloud_cover": {"type": "float"},
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    # Check that nested properties are properly traversed
    assert "processing:software.eometadatatool" in result
    assert (
        result["processing:software.eometadatatool"]
        == "properties.processing:software.eometadatatool"
    )
    assert "processing:software.version" in result
    assert (
        result["processing:software.version"]
        == "properties.processing:software.version"
    )

    # Regular properties should still work
    assert "eo:cloud_cover" in result
    assert result["eo:cloud_cover"] == "properties.eo:cloud_cover"


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_deeply_nested():
    """Test deeply nested properties."""
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "properties": {
                        "properties": {
                            "level1": {
                                "properties": {
                                    "level2": {
                                        "properties": {"level3": {"type": "keyword"}}
                                    }
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    assert "level1.level2.level3" in result
    assert result["level1.level2.level3"] == "properties.level1.level2.level3"


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_disabled_fields():
    """Test that disabled fields are excluded."""
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "properties": {
                        "properties": {
                            "enabled_field": {"type": "keyword"},
                            "disabled_field": {"type": "keyword", "enabled": False},
                            "parent": {
                                "properties": {
                                    "enabled_nested": {"type": "keyword"},
                                    "disabled_nested": {
                                        "type": "keyword",
                                        "enabled": False,
                                    },
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    assert "enabled_field" in result
    assert "disabled_field" not in result
    assert "parent.enabled_nested" in result
    assert "parent.disabled_nested" not in result


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_container_fields():
    """Test that container fields (without type) are not included but their children are."""
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "properties": {
                        "properties": {
                            # This is a container field with no type
                            "container": {
                                "properties": {
                                    "child1": {"type": "keyword"},
                                    "child2": {"type": "float"},
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    # Container field should not be in results (no type)
    assert "container" not in result
    # But its children should be
    assert "container.child1" in result
    assert "container.child2" in result


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_multiple_indices():
    """Test mapping from multiple indices are merged."""
    mappings = {
        "index1": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "field1": {"type": "keyword"},
                        }
                    },
                }
            }
        },
        "index2": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "field2": {"type": "float"},
                        }
                    },
                }
            }
        },
    }

    result = await get_queryables_mapping_shared(mappings)

    assert "field1" in result
    assert "field2" in result


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_excluded_fields(monkeypatch):
    """Test that fields in EXCLUDED_FROM_QUERYABLES are excluded."""
    # Set the environment variable to exclude fields
    monkeypatch.setenv(
        "EXCLUDED_FROM_QUERYABLES",
        "properties.auth:schemes,properties.storage:schemes",
    )

    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "eo:cloud_cover": {"type": "float"},
                            "auth:schemes": {
                                "properties": {
                                    "s3": {
                                        "properties": {
                                            "type": {"type": "keyword"},
                                            "requester_pays": {"type": "boolean"},
                                        }
                                    },
                                    "http": {
                                        "properties": {
                                            "type": {"type": "keyword"},
                                        }
                                    },
                                }
                            },
                            "storage:schemes": {
                                "properties": {
                                    "s3": {
                                        "properties": {
                                            "platform": {"type": "keyword"},
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    # Regular fields should be present
    assert "id" in result
    assert "eo:cloud_cover" in result

    # Excluded fields and their children should NOT be present
    assert "auth:schemes" not in result
    assert "auth:schemes.s3" not in result
    assert "auth:schemes.s3.type" not in result
    assert "auth:schemes.s3.requester_pays" not in result
    assert "auth:schemes.http" not in result
    assert "auth:schemes.http.type" not in result
    assert "storage:schemes" not in result
    assert "storage:schemes.s3" not in result
    assert "storage:schemes.s3.platform" not in result


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_excluded_fields_top_level(monkeypatch):
    """Test that exclusions work for fields at top level (no properties. prefix in path).

    Some indices (like EOPF) have auth:schemes at the top level, resulting in
    field paths like 'auth:schemes.s3.type' instead of 'properties.auth:schemes.s3.type'.
    The exclusion should work for both cases.
    """
    # Set the environment variable to exclude fields with properties. prefix
    monkeypatch.setenv(
        "EXCLUDED_FROM_QUERYABLES",
        "properties.auth:schemes,properties.storage:schemes",
    )

    # Mapping where auth:schemes is at the TOP level (not under properties.properties)
    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "eo:cloud_cover": {"type": "float"},
                    "auth:schemes": {
                        "properties": {
                            "s3": {
                                "properties": {
                                    "type": {"type": "keyword"},
                                    "requester_pays": {"type": "boolean"},
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    # Regular fields should be present
    assert "id" in result
    assert "eo:cloud_cover" in result

    # Excluded fields should NOT be present even without properties. prefix in path
    assert "auth:schemes" not in result
    assert "auth:schemes.s3" not in result
    assert "auth:schemes.s3.type" not in result
    assert "auth:schemes.s3.requester_pays" not in result


@pytest.mark.asyncio
async def test_get_queryables_mapping_shared_excluded_fields_no_prefix_config(
    monkeypatch,
):
    """Test that exclusions work when configured WITHOUT properties. prefix.

    If user sets EXCLUDED_FROM_QUERYABLES='auth:schemes', it should also
    exclude 'properties.auth:schemes' and vice versa.
    """
    # Set the environment variable WITHOUT properties. prefix
    monkeypatch.setenv(
        "EXCLUDED_FROM_QUERYABLES",
        "auth:schemes",
    )

    mappings = {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "properties": {
                        "properties": {
                            "eo:cloud_cover": {"type": "float"},
                            "auth:schemes": {
                                "properties": {
                                    "s3": {
                                        "properties": {
                                            "type": {"type": "keyword"},
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    result = await get_queryables_mapping_shared(mappings)

    # Regular fields should be present
    assert "id" in result
    assert "eo:cloud_cover" in result

    # Excluded fields should NOT be present (properties. prefix auto-added)
    assert "auth:schemes" not in result
    assert "auth:schemes.s3" not in result
    assert "auth:schemes.s3.type" not in result
