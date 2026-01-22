"""Header-based filtering utilities.

This module provides functions for parsing filter headers from stac-auth-proxy.
Headers allow stac-auth-proxy to pass collection and geometry filters to sfeos.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Set
from urllib.parse import unquote_plus

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


def bbox_to_polygon(bbox: List[float]) -> Dict[str, Any]:
    """Convert bbox to GeoJSON Polygon.

    Args:
        bbox: Bounding box as [minx, miny, maxx, maxy] or [minx, miny, minz, maxx, maxy, maxz].

    Returns:
        GeoJSON Polygon geometry dict.

    Raises:
        ValueError: If bbox doesn't have exactly 4 or 6 elements.
    """
    if len(bbox) == 6:
        minx, miny, maxx, maxy = bbox[0], bbox[1], bbox[3], bbox[4]
    elif len(bbox) == 4:
        minx, miny, maxx, maxy = bbox[0], bbox[1], bbox[2], bbox[3]
    else:
        raise ValueError(
            f"Invalid bbox length: expected 4 or 6 elements, got {len(bbox)}"
        )

    return {
        "type": "Polygon",
        "coordinates": [
            [
                [minx, miny],
                [maxx, miny],
                [maxx, maxy],
                [minx, maxy],
                [minx, miny],
            ]
        ],
    }


def extract_geometry_from_cql2_filter(
    cql2_filter: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Extract geometry from CQL2 spatial filter if present.

    Recursively searches the CQL2 filter tree for spatial operations
    that imply intersection (s_intersects, s_contains, s_within) and extracts
    the geometry. Only extracts the first geometry found.

    Note: s_disjoint is excluded because it has inverse semantics (returns items
    that do NOT intersect with the geometry).

    Args:
        cql2_filter: CQL2 JSON filter dictionary.

    Returns:
        GeoJSON geometry dict if spatial operator found, None otherwise.
    """
    if cql2_filter is None:
        return None

    spatial_ops = {"s_intersects", "s_contains", "s_within"}

    def _extract_geometry(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None

        op = node.get("op", "")

        if op in spatial_ops:
            args = node.get("args", [])
            if len(args) >= 2:
                geometry = args[1]
                if isinstance(geometry, dict) and "type" in geometry:
                    return geometry

        if op in ["and", "or", "not"]:
            for arg in node.get("args", []):
                result = _extract_geometry(arg)
                if result:
                    return result

        return None

    return _extract_geometry(cql2_filter)


def compute_geometry_intersection(
    geometries: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Compute intersection of multiple geometries.

    Args:
        geometries: List of GeoJSON geometry dicts.

    Returns:
        GeoJSON geometry dict representing the intersection, or None if:
        - The list is empty
        - Geometries are disjoint (intersection is empty)
        - Shapely is not available (returns first geometry as fallback)

    Note:
        Requires shapely to be installed for actual intersection computation.
        If shapely is not available, returns the first geometry as fallback.
    """
    if not geometries:
        return None

    if len(geometries) == 1:
        return geometries[0]

    try:
        from shapely.geometry import mapping, shape
    except ImportError:
        logger.warning(
            "shapely not installed - geometry intersection skipped. "
            "Install shapely for full geometry intersection support."
        )
        return geometries[0]

    try:
        result = shape(geometries[0])

        for geom_dict in geometries[1:]:
            other = shape(geom_dict)
            result = result.intersection(other)

            if result.is_empty:
                logger.debug("Geometry intersection resulted in empty geometry")
                return None

        return mapping(result)
    except Exception as e:
        logger.warning(f"Geometry intersection failed: {e}")
        return geometries[0]


def collect_geometries_for_intersection(
    request: Request,
    bbox: Optional[List[float]] = None,
    intersects: Optional[Any] = None,
    cql2_filter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Collect all geometry sources for intersection computation.

    Args:
        request: FastAPI Request object (for header geometry).
        bbox: Optional bounding box from request.
        intersects: Optional intersects geometry from request.
        cql2_filter: Optional CQL2 filter that may contain s_intersects.

    Returns:
        List of GeoJSON geometry dicts to be intersected.
    """
    geometries = []

    header_geometry = parse_filter_geometry(request)
    if header_geometry:
        geometries.append(header_geometry)

    if bbox:
        geometries.append(bbox_to_polygon(bbox))

    if intersects:
        if isinstance(intersects, dict):
            geometries.append(intersects)
        elif hasattr(intersects, "type") and hasattr(intersects, "coordinates"):
            geometries.append(
                {"type": intersects.type, "coordinates": intersects.coordinates}
            )

    cql2_geometry = extract_geometry_from_cql2_filter(cql2_filter)
    if cql2_geometry:
        geometries.append(cql2_geometry)

    return geometries


def extract_collections_from_cql2(
    filter_expr: Any,
    filter_lang: Optional[str] = None,
) -> Set[str]:
    """Extract collection IDs referenced in a CQL2 filter.

    Handles both cql2-json and cql2-text formats.

    Args:
        filter_expr: The filter expression (dict for cql2-json, str for cql2-text)
        filter_lang: Optional filter language hint

    Returns:
        Set of collection IDs found in the filter
    """
    if filter_expr is None:
        return set()

    try:
        if isinstance(filter_expr, str):
            filter_expr = unquote_plus(filter_expr)
            try:
                from cql2 import Expr

                expr = Expr(filter_expr)
                filter_dict = expr.to_json()
            except ImportError:
                logger.warning(
                    "cql2 library not installed - CQL2 text parsing skipped. "
                    "Install cql2 for full CQL2-text support."
                )
                return set()
            except Exception:
                return set()
        elif isinstance(filter_expr, dict):
            filter_dict = filter_expr
        else:
            return set()

        return _extract_collections_recursive(filter_dict)
    except Exception as e:
        logger.debug(f"Failed to extract collections from CQL2: {str(e)}")
        return set()


def _extract_collections_recursive(node: Any) -> Set[str]:
    """Recursively extract collection values from CQL2 JSON AST."""
    collections: Set[str] = set()

    if not isinstance(node, dict):
        return collections

    op = node.get("op", "").lower()
    args = node.get("args", [])

    if op in ("=", "eq"):
        if len(args) == 2:
            prop, val = args[0], args[1]
            if _is_collection_property(prop) and isinstance(val, str):
                collections.add(val)

    elif op == "in":
        if len(args) == 2:
            prop, vals = args[0], args[1]
            if _is_collection_property(prop) and isinstance(vals, list):
                for v in vals:
                    if isinstance(v, str):
                        collections.add(v)

    elif op in ("and", "or"):
        for arg in args:
            collections.update(_extract_collections_recursive(arg))

    elif op == "not":
        if args:
            collections.update(_extract_collections_recursive(args[0]))

    return collections


def _is_collection_property(prop: Any) -> bool:
    """Check if a CQL2 property reference is for 'collection'."""
    if isinstance(prop, dict):
        return prop.get("property", "").lower() == "collection"
    return False


def collect_request_collections(
    query_collections: Optional[List[str]] = None,
    body_collections: Optional[List[str]] = None,
    cql2_filter: Optional[Dict[str, Any]] = None,
    filter_lang: Optional[str] = None,
) -> Set[str]:
    """Collect all collection IDs from various request sources.

    Args:
        query_collections: Collections from query params (?collections=a,b)
        body_collections: Collections from request body ({"collections": ["a", "b"]})
        cql2_filter: CQL2 filter that may contain collection references
        filter_lang: Filter language for CQL2 parsing

    Returns:
        Set of all collection IDs found across all sources.
        Empty set means no collections were explicitly requested.
    """
    all_collections: Set[str] = set()

    if query_collections:
        all_collections.update(query_collections)

    if body_collections:
        all_collections.update(body_collections)

    if cql2_filter:
        cql2_collections = extract_collections_from_cql2(cql2_filter, filter_lang)
        all_collections.update(cql2_collections)

    return all_collections


def compute_collection_intersection(
    requested_collections: Optional[Set[str]],
    header_collections: Optional[List[str]],
) -> Optional[List[str]]:
    """Compute intersection of requested collections with allowed collections from header.

    Args:
        requested_collections: Set of collection IDs requested by the user.
            None or empty means user didn't specify collections (use all allowed).
        header_collections: List of allowed collection IDs from X-Filter-Collections header.
            None means no header filter (allow all).

    Returns:
        List of collection IDs to use for filtering:
        - None: No filtering needed (no header present and no collections requested)
        - Empty list: Intersection is empty (return empty results)
        - Non-empty list: Collections to filter by

    Behavior:
        - No header, no request collections -> None (no filter)
        - No header, has request collections -> request collections as list
        - Has header, no request collections -> header collections (allowed set)
        - Has header, has request collections -> intersection of both
    """
    # No header filter present - authorization not active
    if header_collections is None:
        if requested_collections:
            return list(requested_collections)
        return None

    # Header present but empty - no collections allowed
    if len(header_collections) == 0:
        return []

    allowed_set = set(header_collections)

    # User didn't request specific collections - use all allowed
    if not requested_collections:
        return header_collections

    # Compute intersection
    intersection = requested_collections & allowed_set

    if not intersection:
        logger.debug(
            f"Collection intersection is empty. "
            f"Requested: {requested_collections}, Allowed: {allowed_set}"
        )
        return []

    return list(intersection)
