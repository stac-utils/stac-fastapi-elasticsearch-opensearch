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


def geometry_intersects_filter(
    item_geometry: Dict[str, Any], filter_geometry: Dict[str, Any]
) -> bool:
    """Check if item geometry intersects with the filter geometry.

    Args:
        item_geometry: GeoJSON geometry dict from the item.
        filter_geometry: GeoJSON geometry dict from header filter.

    Returns:
        True if geometries intersect (or if shapely not available), False otherwise.

    Note:
        Requires shapely to be installed. If shapely is not available,
        this function returns True (allows access) to avoid breaking
        deployments without shapely.
    """
    try:
        from shapely.geometry import shape
    except ImportError:
        logger.warning(
            "shapely not installed - geometry filter check skipped. "
            "Install shapely for full geometry filtering support."
        )
        return True  # Allow access if shapely not available

    try:
        item_shape = shape(item_geometry)
        filter_shape = shape(filter_geometry)
        return item_shape.intersects(filter_shape)
    except Exception as e:
        logger.warning(f"Geometry intersection check failed: {e}")
        # On error, allow access (fail open)
        return True


def check_collection_access(
    request: Request, collection_id: str, resource_type: str = "Collection"
) -> None:
    """Check if collection access is allowed by header filter.

    Args:
        request: FastAPI Request object.
        collection_id: The ID of the collection to check access for.
        resource_type: Type of resource for error message ("Collection" or "Item").

    Raises:
        HTTPException: 404 if collection is not in the allowed list.

    Note:
        Does nothing if no header filter is present (allows all access).
    """
    from fastapi import HTTPException

    header_collections = parse_filter_collections(request)
    if header_collections is not None and collection_id not in header_collections:
        raise HTTPException(status_code=404, detail=f"{resource_type} not found")


def check_item_geometry_access(
    request: Request, item_geometry: Optional[Dict[str, Any]]
) -> None:
    """Check if item geometry intersects with allowed geometry filter.

    Args:
        request: FastAPI Request object.
        item_geometry: GeoJSON geometry dict from the item.

    Raises:
        HTTPException: 404 if geometries do not intersect.

    Note:
        Does nothing if no header filter is present or if item has no geometry.
    """
    from fastapi import HTTPException

    header_geometry = parse_filter_geometry(request)
    if header_geometry is not None and item_geometry:
        if not geometry_intersects_filter(item_geometry, header_geometry):
            raise HTTPException(status_code=404, detail="Item not found")


def create_geometry_filter_object(
    geometry_dict: Optional[Dict[str, Any]]
) -> Optional[Any]:
    """Create a SimpleNamespace geometry object for database filtering.

    Args:
        geometry_dict: GeoJSON geometry dict from header filter.

    Returns:
        SimpleNamespace with type and coordinates attributes, or None if input is None.

    Note:
        The returned object can be passed to database.apply_intersects_filter().
    """
    if geometry_dict is None:
        return None

    from types import SimpleNamespace

    return SimpleNamespace(
        type=geometry_dict.get("type", ""),
        coordinates=geometry_dict.get("coordinates", []),
    )


def get_geometry_filter_from_header(request: Request) -> Optional[Any]:
    """Get geometry filter object from header if present.

    Convenience function that combines parse_filter_geometry and
    create_geometry_filter_object into a single call.

    Args:
        request: FastAPI Request object.

    Returns:
        SimpleNamespace with type and coordinates attributes, or None if
        no geometry header is present.
    """
    header_geometry = parse_filter_geometry(request)
    return create_geometry_filter_object(header_geometry)
