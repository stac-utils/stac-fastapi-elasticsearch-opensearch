"""STAC validation module.

Provides validation for STAC items and collections using multiple validation backends:
- Pydantic validation (always enabled)
- Python STAC Validator with multi-processing (concurrent batch validation)
"""

import asyncio
import logging
import os
import threading

from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)

# Suppress verbose logging from stac_validator
logging.getLogger("stac_validator.utilities").setLevel(logging.WARNING)

SCHEMA_CACHE_SIZE = int(os.getenv("SCHEMA_CACHE_SIZE", "32"))
MAX_VALIDATION_WORKERS = int(os.getenv("MAX_VALIDATION_WORKERS", "0"))

# Global instances to cache validators and avoid repeated initialization
_batch_validator_instance = None
_single_validator_instance = None
_validator_lock = threading.Lock()


def _get_batch_validator():
    """Get or create the singleton batch validator instance.

    Initializes and caches a single batch validator instance with schema caching
    to improve performance across all validation calls within the application lifetime.
    Uses double-checked locking for thread safety.

    Returns:
        The batch validator instance (validate_dicts function with cached schemas).
    """
    global _batch_validator_instance
    if _batch_validator_instance is None:
        with _validator_lock:
            if _batch_validator_instance is None:
                try:
                    from stac_validator.batch_validator import validate_dicts
                    from stac_validator.utilities import set_schema_cache_size

                    set_schema_cache_size(SCHEMA_CACHE_SIZE)
                    _batch_validator_instance = validate_dicts
                except ImportError as e:
                    logger.error("stac_validator batch_validator not available")
                    raise ImportError(
                        "STAC validator batch_validator is not installed. "
                        "Install it with: pip install stac-fastapi-core[validator] "
                        "or pip install stac-fastapi-elasticsearch[validator] "
                        "or pip install stac-fastapi-opensearch[validator]"
                    ) from e
    return _batch_validator_instance


def _get_single_validator():
    """Get or create the singleton single-item validator instance.

    Initializes and caches a single StacValidator instance with schema caching
    to improve performance for single-item validation.
    Uses double-checked locking for thread safety.

    Returns:
        The StacValidator instance with cached schemas.
    """
    global _single_validator_instance
    if _single_validator_instance is None:
        with _validator_lock:
            if _single_validator_instance is None:
                try:
                    from stac_validator import StacValidator
                    from stac_validator.utilities import set_schema_cache_size

                    set_schema_cache_size(SCHEMA_CACHE_SIZE)
                    _single_validator_instance = StacValidator()
                except ImportError as e:
                    logger.error("stac_validator not available")
                    raise ImportError(
                        "STAC validator is not installed. "
                        "Install it with: pip install stac-fastapi-core[validator] "
                        "or pip install stac-fastapi-elasticsearch[validator] "
                        "or pip install stac-fastapi-opensearch[validator]"
                    ) from e
    return _single_validator_instance


def _extract_error_message(validation_result: dict) -> str:
    """Extract a readable error message from a validation result.

    Args:
        validation_result: The validation result dictionary from stac_validator.

    Returns:
        A formatted error message string.
    """
    err_msg = validation_result.get("error_message", "")

    if not err_msg:
        # Fallback to errors array if error_message is empty
        errors = validation_result.get("errors", [])
        if errors:
            err_msg = "; ".join(
                str(e) if isinstance(e, str) else e.get("message", str(e))
                for e in errors
            )
        else:
            err_msg = "Validation failed with no error details"

    # Include schema information for debugging
    failed_schema = validation_result.get("failed_schema", "")
    if failed_schema:
        err_msg = f"{err_msg}. For more information check the schema: {failed_schema}"

    return err_msg


def validate_batch_with_stac_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, list[str]]]:
    """Validate a batch of STAC items using stac_validator with multi-processing.

    Uses the singleton batch validator instance with cached schemas for concurrent validation
    across multiple worker processes. Separates valid items from invalid ones.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps error messages to lists of affected item IDs.
    """
    validate_dicts = _get_batch_validator()

    valid_items = []
    invalid_items = {}

    try:
        # Validate all items concurrently using multi-processing
        # max_workers=None uses CPU count, max_workers=0 uses sequential processing
        results = validate_dicts(
            items,
            max_workers=MAX_VALIDATION_WORKERS if MAX_VALIDATION_WORKERS > 0 else None,
            show_progress=False,
        )

        # Group errors by message to avoid duplication
        errors_by_message: dict[str, list[str]] = {}

        for idx, result in enumerate(results):
            item = items[idx]
            item_id = item.get("id", f"unknown_id_{idx}")

            if result.get("valid_stac", False):
                valid_items.append(item)
            else:
                # Extract and format error message
                err_msg = _extract_error_message(result)

                # Group by error message
                if err_msg not in errors_by_message:
                    errors_by_message[err_msg] = []
                errors_by_message[err_msg].append(item_id)
                logger.error(f"STAC validation failed for '{item_id}': {err_msg}")

        # Convert grouped errors to final format: error message -> list of item IDs
        invalid_items = errors_by_message

    except Exception as exc:
        logger.error(f"Batch validation request failed: {exc}")
        error_msg = f"Batch validation failed: {str(exc)}"
        item_ids = [
            item.get("id", f"unknown_id_{idx}") for idx, item in enumerate(items)
        ]
        invalid_items[error_msg] = item_ids

    return valid_items, invalid_items


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a single STAC item or collection using optional STAC validator.

    If stac_data is already a Pydantic model object, Pydantic validation is skipped
    (assuming it was already validated by FastAPI). Only STAC validator is run if enabled.

    Args:
        stac_data: STAC data as dict or Pydantic model object.
        pydantic_model: The Pydantic model class to use for validation (Item or Collection).

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValueError: If STAC validation fails.
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

    # 2. STAC Validator (optional, enabled via ENABLE_STAC_VALIDATOR env var)
    if get_bool_env("ENABLE_STAC_VALIDATOR"):
        # Use cached single-item validator instance (avoid multi-processing overhead)
        validator = _get_single_validator()
        validation_result = validator.validate_dict(stac_dict)

        if not validation_result.get("valid_stac", False):
            item_id = stac_dict.get("id", "unknown_id")
            error_msg = _extract_error_message(validation_result)
            raise ValueError(f"STAC validation failed for '{item_id}': {error_msg}")

    return stac_obj


async def async_validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Asynchronous wrapper for validate_stac.

    Offloads the CPU-bound STAC validation to a separate thread to prevent
    blocking the FastAPI asyncio event loop during API requests.

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
    """Asynchronously validate a batch of STAC items using multi-processing.

    Offloads the CPU-bound batch validation to a separate thread to prevent
    blocking the FastAPI asyncio event loop.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps error messages to lists of affected item IDs.
    """
    return await asyncio.to_thread(validate_batch_with_stac_validator, items)
