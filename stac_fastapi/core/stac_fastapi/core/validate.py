"""STAC validation module.

Provides validation for STAC items and collections using multiple validation backends:
- Pydantic validation (always enabled)
- Python STAC Validator (fallback, sequential)
"""

import asyncio
import logging
import os

from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)

SCHEMA_CACHE_SIZE = int(os.getenv("SCHEMA_CACHE_SIZE", "32"))


# Singleton STAC validator instance to reuse schema cache across requests
_stac_validator_instance = None


def _get_stac_validator():
    """Get or create the singleton STAC validator instance.

    Initializes and caches a single StacValidate instance with a configured
    schema cache to improve performance across all validation calls within
    the application lifetime.

    Returns:
        StacValidate: The singleton validator instance with schema caching enabled.
    """
    global _stac_validator_instance
    if _stac_validator_instance is None:
        from stac_validator import stac_validator
        from stac_validator.utilities import set_schema_cache_size

        set_schema_cache_size(SCHEMA_CACHE_SIZE)
        _stac_validator_instance = stac_validator.StacValidate(verbose=True)
    return _stac_validator_instance


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a STAC item or collection using optional STAC validator.

    If stac_data is already a Pydantic model object, Pydantic validation is skipped
    (assuming it was already validated by FastAPI). Only STAC validator is run if enabled.

    Args:
        stac_data: STAC data as dict or Pydantic model object.
        pydantic_model: The Pydantic model class to use for validation (Item or Collection).

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValidationError: If STAC validation fails.
    """
    # 1. Pydantic Parsing/Validation
    # If already a Pydantic model object, skip Pydantic validation (FastAPI already validated it)
    if isinstance(stac_data, (Item, Collection)):
        stac_obj = stac_data
        stac_dict = stac_data.model_dump(mode="json")
    else:
        # For dict input, validate with Pydantic first
        stac_obj = pydantic_model(**stac_data)
        stac_dict = stac_data

    # 2. Logic for Python STAC Validator (Legacy/Optional)
    if get_bool_env("ENABLE_STAC_VALIDATOR"):
        try:
            # Check if stac_validator is installed
            import stac_validator  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "STAC validator is not installed. "
                "Install it with: pip install stac-fastapi-core[validator] "
                "or pip install stac-fastapi-elasticsearch[validator] "
                "or pip install stac-fastapi-opensearch[validator]"
            ) from e

        # Use singleton validator instance to reuse schema cache
        stac = _get_stac_validator()
        is_valid = stac.validate_dict(stac_dict)

        if not is_valid:
            # Log detailed error information
            error_msg = "Unknown validation error"
            if stac.message:
                error_details = stac.message[0]
                error_msg = error_details.get("error_message", "")
                failed_schema = error_details.get("failed_schema", "")
                error_verbose = error_details.get("error_verbose", {})

                # Build comprehensive error message
                if error_msg:
                    # Use the error_message as-is if available
                    pass
                elif error_verbose and isinstance(error_verbose, dict):
                    # Try to extract meaningful details from error_verbose
                    validator = error_verbose.get("validator", "")
                    path = error_verbose.get("path_in_document", [])
                    message = error_verbose.get("message", "")

                    if message:
                        error_msg = message
                        if path:
                            error_msg += f" at {'.'.join(str(p) for p in path)}"
                    elif validator:
                        error_msg = f"{validator}"
                        if path:
                            error_msg += f" at {'.'.join(str(p) for p in path)}"
                    else:
                        error_msg = "Unknown validation error"
                else:
                    error_msg = "Unknown validation error"

                logger.error(f"STAC validation failed: {error_msg}")
                if failed_schema:
                    logger.error(f"Failed schema: {failed_schema}")
                if error_verbose:
                    logger.error(f"Validation details: {error_verbose}")
            else:
                logger.error("STAC validation failed with no error message")
            raise ValueError(f"STAC validation failed: {error_msg}")

    return stac_obj


def validate_item(stac_data: dict | Item) -> Item:
    """Validate a STAC item using optional STAC validator.

    Convenience wrapper around validate_stac for items.

    Args:
        stac_data: Item data as dict or Item object.

    Returns:
        Validated Item object.

    Raises:
        ValueError: If validation fails.
    """
    return validate_stac(stac_data, pydantic_model=Item)


def validate_collection(collection_data: dict | Collection) -> Collection:
    """Validate a STAC collection using optional STAC validator.

    Convenience wrapper around validate_stac for collections.

    Args:
        collection_data: Collection data as dict or Collection object.

    Returns:
        Validated Collection object.

    Raises:
        ValueError: If validation fails.
    """
    return validate_stac(collection_data, pydantic_model=Collection)


async def async_validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Asynchronous wrapper for validate_stac.

    Offloads the CPU-bound STAC validation to a separate thread to prevent
    blocking the FastAPI asyncio event loop during API requests.

    Args:
        stac_data: STAC data as dict or Pydantic model.
        pydantic_model: The Pydantic model class to use for validation.

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValueError: If validation fails.
    """
    return await asyncio.to_thread(validate_stac, stac_data, pydantic_model)


async def async_validate_item(stac_data: dict | Item) -> Item:
    """Async convenience wrapper around async_validate_stac for items.

    Args:
        stac_data: Item data as dict or Item object.

    Returns:
        Validated Item object.

    Raises:
        ValueError: If validation fails.
    """
    return await async_validate_stac(stac_data, pydantic_model=Item)


async def async_validate_collection(
    collection_data: dict | Collection,
) -> Collection:
    """Async convenience wrapper around async_validate_stac for collections.

    Args:
        collection_data: Collection data as dict or Collection object.

    Returns:
        Validated Collection object.

    Raises:
        ValueError: If validation fails.
    """
    return await async_validate_stac(collection_data, pydantic_model=Collection)
