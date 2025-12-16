"""Header-based filtering utilities.

This module provides functions for parsing filter headers from stac-auth-proxy.
Headers allow stac-auth-proxy to pass collection and geometry filters to sfeos.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import Request

logger = logging.getLogger(__name__)

# Header names
FILTER_COLLECTIONS_HEADER = "X-Filter-Collections"
FILTER_GEOMETRY_HEADER = "X-Filter-Geometry"


def parse_filter_collections(request: Request) -> Optional[List[str]]:
    """Parse collection filter from X-Filter-Collections header.

    Args:
        request: FastAPI Request object.

    Returns:
        List of collection IDs if header is present, None otherwise.
        Empty list if header value is empty string.

    Example:
        Header "X-Filter-Collections: col-a,col-b,col-c" returns ["col-a", "col-b", "col-c"]
    """
    header_value = request.headers.get(FILTER_COLLECTIONS_HEADER)

    if header_value is None:
        return None

    # Handle empty header value
    if not header_value.strip():
        return []

    # Parse comma-separated list
    collections = [c.strip() for c in header_value.split(",") if c.strip()]
    logger.debug(f"Parsed filter collections from header: {collections}")

    return collections


def parse_filter_geometry(request: Request) -> Optional[Dict[str, Any]]:
    """Parse geometry filter from X-Filter-Geometry header.

    Args:
        request: FastAPI Request object.

    Returns:
        GeoJSON geometry dict if header is present and valid, None otherwise.

    Example:
        Header 'X-Filter-Geometry: {"type":"Polygon","coordinates":[...]}'
        returns the parsed GeoJSON dict.
    """
    header_value = request.headers.get(FILTER_GEOMETRY_HEADER)

    if header_value is None:
        return None

    if not header_value.strip():
        return None

    try:
        geometry = json.loads(header_value)
        logger.debug(
            f"Parsed filter geometry from header: {geometry.get('type', 'unknown')}"
        )
        return geometry
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse geometry header: {e}")
        return None
