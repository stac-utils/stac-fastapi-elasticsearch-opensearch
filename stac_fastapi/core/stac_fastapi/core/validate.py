"""STAC validation module.

Provides validation for STAC items and collections using multiple validation backends:
- Pydantic validation (always enabled via FastAPI/stac_pydantic)
- Fast JSON Schema compiled validation (ultra-fast, sequential STAC validation)
"""

import asyncio
import logging

from dateutil import parser

try:
    import fastjsonschema
    import stac_validator.fast_validator as fv_module
    from stac_validator.fast_validator import get_validator

    # Permanently mute the validator's CLI output for the SFEOS server
    fv_module.QUIET_MODE = True
except ImportError:
    fastjsonschema = None
    fv_module = None
    get_validator = None

from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)


def validate_batch_with_stac_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, list[str]]]:
    """Validate a batch of STAC items using compiled fastjsonschema functions.

    Performs ultra-fast sequential validation in memory. Bypasses multiprocessing
    to save massive amounts of RAM and Inter-Process Communication (IPC) CPU overhead.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps error messages to lists of affected item IDs.
    """
    # Guard clause: Return immediately if validation is disabled or the batch is empty
    if not get_bool_env("ENABLE_STAC_VALIDATOR") or not items:
        return items, {}

    if fastjsonschema is None or get_validator is None:
        raise ImportError(
            "STAC validator is not installed. "
            "Install it with: pip install stac-fastapi-elasticsearch[validator]"
        )

    valid_items: list[dict] = []
    invalid_items: dict[str, list[str]] = {}

    for idx, item in enumerate(items):
        # Reliable ID matching: use exact object ID or fallback to index
        item_id = item.get("id", f"unknown_id_{idx}")

        stac_type = (
            "Item" if item.get("type") == "Feature" else item.get("type", "unknown")
        )
        stac_version = item.get("stac_version", "1.0.0")
        extensions = item.get("stac_extensions", [])

        try:
            # get_validator uses internal caching, so this compiles instantly for repeated schemas
            validator, _ = get_validator(stac_type, stac_version, extensions)
            validator(item)
            valid_items.append(item)

        except fastjsonschema.JsonSchemaValueException as e:
            err_msg = f"{e.name} {e.message.replace(e.name, '').strip()}"
            if "disallowed definition" in err_msg and "collection" in err_msg:
                err_msg = (
                    "STAC Spec Violation: Missing {'rel': 'collection'} in links array."
                )

            if err_msg not in invalid_items:
                invalid_items[err_msg] = []
            invalid_items[err_msg].append(item_id)

        except Exception as e:
            err_msg = str(e)
            if err_msg not in invalid_items:
                invalid_items[err_msg] = []
            invalid_items[err_msg].append(item_id)

    return valid_items, invalid_items


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a single STAC item or collection using the optional STAC validator.

    Args:
        stac_data: STAC data as a raw dict or parsed Pydantic model object.
        pydantic_model: The Pydantic model class to use for validation (Item or Collection).

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValueError: If strict STAC validation fails.
    """
    # 1. Pydantic Parsing/Validation Layer
    if isinstance(stac_data, (Item, Collection)):
        stac_obj = stac_data
        stac_dict = stac_data.model_dump(mode="json", exclude_none=True)
    else:
        stac_obj = pydantic_model(**stac_data)
        # Dump the parsed object so defaults, dates, and coercions are accurately reflected
        stac_dict = stac_obj.model_dump(mode="json", exclude_none=True)

    # 2. STAC Validator Layer (optional, enabled via ENABLE_STAC_VALIDATOR env var)
    if get_bool_env("ENABLE_STAC_VALIDATOR"):
        try:
            import fastjsonschema
            import stac_validator.fast_validator as fv_module
            from stac_validator.fast_validator import get_validator

            # Permanently mute the validator's CLI output for the SFEOS server
            fv_module.QUIET_MODE = True
        except ImportError as e:
            raise ImportError("stac_validator not installed.") from e

        stac_type = (
            "Item"
            if stac_dict.get("type") == "Feature"
            else stac_dict.get("type", "unknown")
        )
        stac_version = stac_dict.get("stac_version", "1.0.0")
        extensions = stac_dict.get("stac_extensions", [])

        try:
            validator, _ = get_validator(stac_type, stac_version, extensions)
            validator(stac_dict)

        except fastjsonschema.JsonSchemaValueException as e:
            item_id = stac_dict.get("id", "unknown_id")
            err_msg = f"{e.name} {e.message.replace(e.name, '').strip()}"
            if "disallowed definition" in err_msg and "collection" in err_msg:
                err_msg = (
                    "STAC Spec Violation: Missing {'rel': 'collection'} in links array."
                )
            raise ValueError(f"STAC validation failed for '{item_id}': {err_msg}")

        except Exception as e:
            item_id = stac_dict.get("id", "unknown_id")
            raise ValueError(f"STAC validation failed for '{item_id}': {str(e)}")

    return stac_obj


async def async_validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Asynchronous wrapper for validate_stac.

    Offloads the validation to a separate thread to prevent blocking
    the FastAPI asyncio event loop during API requests.

    Args:
        stac_data: STAC data as dict or Pydantic model.
        pydantic_model: The Pydantic model class to use for validation (Item or Collection).

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValueError: If validation fails.
    """
    return await asyncio.to_thread(validate_stac, stac_data, pydantic_model)


async def async_validate_batch_with_stac_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, list[str]]]:
    """Asynchronously validate a batch of STAC items.

    Offloads the CPU-bound validation loop to a separate thread to prevent
    blocking the FastAPI asyncio event loop.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps error messages to lists of affected item IDs.
    """
    return await asyncio.to_thread(validate_batch_with_stac_validator, items)


def validate_geometry_bounds(coords: list) -> None:
    """Recursively validate that all coordinate pairs fall within WGS84 limits.

    Traverses nested coordinate lists to enforce global bounds (±180° lon, ±90° lat)
    across all geometry types (Point, LineString, Polygon, MultiPolygon, etc.).

    Args:
        coords: Coordinate list (may be nested).

    Raises:
        ValueError: If any coordinate pair falls outside WGS84 bounds.
    """
    if not coords:
        return

    # Check if we reached an atomic coordinate pair [lon, lat]
    if isinstance(coords[0], (int, float)):
        if len(coords) < 2:
            raise ValueError("Coordinate pair must have at least 2 elements [lon, lat]")
        lon, lat = coords[0], coords[1]
        if not (-180 <= lon <= 180) or not (-90 <= lat <= 90):
            raise ValueError(f"Coordinates out of global WGS84 bounds: [{lon}, {lat}]")
    else:
        # Step deeper into nested coordinate lists
        for sub_coord in coords:
            validate_geometry_bounds(sub_coord)


def validate_item_topology_lightweight(item_dict: dict) -> None:
    """Lightweight validation to enforce global coordinate boundaries and antimeridian checks.

    Validates all geometry types for WGS84 bounds compliance, and checks for improper
    antimeridian line skips in Polygon and MultiPolygon rings. Polygon rings are also
    checked against a configurable vertex limit (default 5000) to prevent DoS attacks
    with pathologically complex geometries.

    Args:
        item_dict: The STAC item dictionary to validate.

    Raises:
        ValueError: If geometry is invalid, out of bounds, crosses antimeridian improperly,
                   or exceeds the vertex limit.
    """
    from stac_fastapi.core.utilities import get_int_env

    geometry = item_dict.get("geometry")
    if not geometry:
        return

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if coords is None:
        return

    try:
        # 1. Global Bounds Enforcer: Validates ALL vertices recursively
        validate_geometry_bounds(coords)

        # 2. Antimeridian Isolation: Only check jumps on linear polygon rings
        if geom_type not in ("Polygon", "MultiPolygon"):
            return

        max_vertices = get_int_env("MAX_TOPOLOGY_VERTICES", default=5000)

        def check_antimeridian_rings(rings: list) -> None:
            for ring in rings:
                if len(ring) < 4:
                    raise ValueError(
                        "Polygon ring must have at least 4 coordinates (closed loop)."
                    )
                if len(ring) > max_vertices:
                    raise ValueError(
                        f"Geometry has too many vertices ({len(ring)}). Maximum allowed is {max_vertices}."
                    )

                # Check antimeridian jumps between consecutive points
                # (WGS84 bounds already validated by validate_geometry_bounds above)
                for i in range(len(ring) - 1):
                    lon1 = ring[i][0]
                    lon2 = ring[i + 1][0]
                    if abs(lon1 - lon2) > 180:
                        raise ValueError(
                            f"Geometry crosses the antimeridian without proper truncation/splitting "
                            f"between longitude {lon1} and {lon2}."
                        )

        if geom_type == "Polygon":
            check_antimeridian_rings(coords)
        elif geom_type == "MultiPolygon":
            for polygon in coords:
                check_antimeridian_rings(polygon)

    except (KeyError, TypeError, IndexError) as e:
        raise ValueError(f"Malformed GeoJSON coordinate structure: {str(e)}")


def batch_validate_topology(
    features: list[dict],
) -> tuple[list[dict], dict[str, list[str]]]:
    """Synchronize batch topology validation suitable for thread pool execution.

    Processes an array of features, returning the valid ones alongside a dictionary
    of caught topology errors. Designed to be offloaded to a background thread via
    asyncio.to_thread to prevent blocking the FastAPI event loop during CPU-bound
    geometry validation.

    Args:
        features: List of feature dictionaries to validate.

    Returns:
        Tuple of (valid_features, topology_errors) where topology_errors maps
        error messages to lists of affected item IDs.
    """
    re_validated_features = []
    topology_errors: dict[str, list[str]] = {}

    for item in features:
        try:
            validate_item_topology_lightweight(item)
            re_validated_features.append(item)
        except ValueError as e:
            err_msg = f"Topology Error: {str(e)}"
            if err_msg not in topology_errors:
                topology_errors[err_msg] = []
            topology_errors[err_msg].append(item.get("id", "unknown"))

    return re_validated_features, topology_errors


def validate_datetime_range(item: dict) -> None:
    """Validate datetime range fields in a STAC item.

    Validates STAC spec requirements:
    - If datetime is null, both start_datetime and end_datetime must be set
    - If start_datetime is set, end_datetime must also be set (and vice versa)
    - start_datetime must be <= end_datetime
    - If datetime is set, it must fall within start/end range

    Args:
        item (dict): STAC item to validate.

    Raises:
        ValueError: If datetime range validation fails.
    """
    properties = item.get("properties", {})

    start_str = properties.get("start_datetime")
    dt_str = properties.get("datetime")
    end_str = properties.get("end_datetime")

    # Parse datetime strings
    try:
        start = parser.isoparse(start_str) if start_str else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid ISO 8601 format for 'start_datetime': {start_str}")

    try:
        dt = parser.isoparse(dt_str) if dt_str else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid ISO 8601 format for 'datetime': {dt_str}")

    try:
        end = parser.isoparse(end_str) if end_str else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid ISO 8601 format for 'end_datetime': {end_str}")

    # STAC Spec: If datetime is null, both start_datetime and end_datetime must be set
    if dt is None and (start is None or end is None):
        raise ValueError(
            "If 'datetime' is null, both 'start_datetime' and 'end_datetime' must be set"
        )

    # STAC Spec: If either start_datetime or end_datetime is set, both must be set
    if (start is not None or end is not None) and (start is None or end is None):
        raise ValueError(
            "Both 'start_datetime' and 'end_datetime' must be set together, or both must be null"
        )

    # Validate start <= end when both are set
    if start is not None and end is not None:
        if not (start <= end):
            raise ValueError(
                f"'start_datetime' ({start_str}) must be <= 'end_datetime' ({end_str})"
            )

    # Validate start <= datetime <= end when datetime is set
    if dt is not None:
        if start is not None and not (start <= dt):
            raise ValueError(
                f"'datetime' ({dt_str}) must be >= 'start_datetime' ({start_str})"
            )
        if end is not None and not (dt <= end):
            raise ValueError(
                f"'datetime' ({dt_str}) must be <= 'end_datetime' ({end_str})"
            )
