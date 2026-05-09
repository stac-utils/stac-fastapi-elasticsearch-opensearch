"""STAC validation module.

Provides validation for STAC items and collections using the fast Python validator microservice.
- Pydantic validation (always enabled)
- Fast Python validator microservice (high-performance, concurrent)
"""

import asyncio
import logging
import os

import httpx
from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)

FAST_VALIDATOR_URL = os.getenv(
    "FAST_VALIDATOR_URL", "http://stac-validator:8000/validate"
)


def validate_batch_with_fast_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Validate a batch of STAC items using the fast Python validator service.

    Sends items to the fast validator microservice for concurrent validation.
    Separates valid items from invalid ones based on the validator response.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps item IDs to their validation error messages.
    """
    valid_items = []
    invalid_items = {}

    with httpx.Client(timeout=30.0) as client:
        try:
            # Wrap items in a FeatureCollection for the validator
            feature_collection = {"type": "FeatureCollection", "features": items}
            response = client.post(FAST_VALIDATOR_URL, json=feature_collection)
            response.raise_for_status()

            response_data = response.json()
            logger.debug(f"Fast validator response: {response_data}")

            # Fast validator returns a list with one object containing validation results
            if isinstance(response_data, list) and len(response_data) > 0:
                batch_data = response_data[0]
            else:
                batch_data = response_data

            # Fast validator returns valid_stac (bool) and errors (list of grouped errors)
            if batch_data.get("valid_stac", False):
                # All items are valid
                valid_items = items
            else:
                # Some items are invalid, check errors
                errors = batch_data.get("errors", [])
                valid_item_ids = set(item.get("id") for item in items)

                logger.debug(f"Found {len(errors)} validation error groups")
                logger.debug(f"Errors structure: {errors}")

                # Mark items with errors as invalid
                # Errors are grouped by message with affected_items list
                for error in errors:
                    logger.debug(f"Processing error: {error}")
                    err_msg = error.get("error_message", "Validation failed")
                    affected_items = error.get("affected_items", [])
                    logger.debug(
                        f"Error message: {err_msg}, Affected items: {affected_items}"
                    )

                    for item_id in affected_items:
                        logger.debug(f"Error for item {item_id}: {err_msg}")
                        invalid_items[item_id] = err_msg
                        if item_id in valid_item_ids:
                            valid_item_ids.discard(item_id)

                # Collect valid items
                valid_items = [
                    item for item in items if item.get("id") in valid_item_ids
                ]
                logger.debug(
                    f"Valid items: {len(valid_items)}, Invalid items: {len(invalid_items)}"
                )

        except Exception as e:
            logger.error(f"Fast validator request failed: {e}", exc_info=True)
            for item in items:
                invalid_items[
                    item.get("id", "unknown_id")
                ] = "Fast validator unreachable or failed."

    return valid_items, invalid_items


async def async_validate_batch_with_fast_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Asynchronously validate a batch of STAC items using the fast Python validator service.

    Sends items to the fast validator microservice for concurrent validation.
    Separates valid items from invalid ones based on the validator response.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps item IDs to their validation error messages.
    """
    valid_items = []
    invalid_items = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # Wrap items in a FeatureCollection for the validator
            feature_collection = {"type": "FeatureCollection", "features": items}
            response = await client.post(FAST_VALIDATOR_URL, json=feature_collection)
            response.raise_for_status()

            response_data = response.json()
            logger.debug(f"Fast validator response: {response_data}")

            # Fast validator returns a list with one object containing validation results
            if isinstance(response_data, list) and len(response_data) > 0:
                batch_data = response_data[0]
            else:
                batch_data = response_data

            # Fast validator returns valid_stac (bool) and errors (list of grouped errors)
            if batch_data.get("valid_stac", False):
                # All items are valid
                valid_items = items
            else:
                # Some items are invalid, check errors
                errors = batch_data.get("errors", [])
                valid_item_ids = set(item.get("id") for item in items)

                logger.debug(f"Found {len(errors)} validation error groups")
                logger.debug(f"Errors structure: {errors}")

                # Mark items with errors as invalid
                # Errors are grouped by message with affected_items list
                for error in errors:
                    logger.debug(f"Processing error: {error}")
                    err_msg = error.get("error_message", "Validation failed")
                    affected_items = error.get("affected_items", [])
                    logger.debug(
                        f"Error message: {err_msg}, Affected items: {affected_items}"
                    )

                    for item_id in affected_items:
                        logger.debug(f"Error for item {item_id}: {err_msg}")
                        invalid_items[item_id] = err_msg
                        if item_id in valid_item_ids:
                            valid_item_ids.discard(item_id)

                # Collect valid items
                valid_items = [
                    item for item in items if item.get("id") in valid_item_ids
                ]
                logger.debug(
                    f"Valid items: {len(valid_items)}, Invalid items: {len(invalid_items)}"
                )

        except Exception as exc:
            logger.error(f"Batch validation request failed: {exc}", exc_info=True)
            for item in items:
                item_id = item.get("id", "unknown_id")
                invalid_items[
                    item_id
                ] = "Fast validator unreachable or failed to process."

    return valid_items, invalid_items


def _validate_with_fast_validator_service(stac_dict: dict) -> None:
    """Validate a STAC item or collection using the fast Python validator service.

    Sends a single STAC item or collection to the fast validator microservice
    for high-speed schema validation. Raises an error if validation fails.

    Args:
        stac_dict: STAC item or collection data as a dictionary.

    Raises:
        RuntimeError: If the fast validator service is unreachable.
        ValueError: If the STAC data fails validation.
    """
    with httpx.Client(timeout=30.0) as client:
        try:
            response = client.post(FAST_VALIDATOR_URL, json=stac_dict)
            response.raise_for_status()
        except httpx.RequestError as exc:
            logger.error(f"Networking error to fast validator: {exc}")
            raise RuntimeError(
                f"Fast validator unreachable at {FAST_VALIDATOR_URL}"
            ) from exc

        # Parse the fast validator response
        response_data = response.json()

        # Fast validator returns a list with one object containing validation results
        if isinstance(response_data, list) and len(response_data) > 0:
            batch_data = response_data[0]
        else:
            batch_data = response_data

        # Check if validation failed
        if not batch_data.get("valid_stac", False):
            # Get the first error for the response
            errors = batch_data.get("errors", [])
            if errors:
                error = errors[0]
                msg = error.get("error_message", "Invalid STAC")
                full_msg = f"Fast Validator Rejected STAC: {msg}"
            else:
                full_msg = "Fast Validator Rejected STAC: Validation failed"

            logger.error(full_msg)
            # Raise the ValueError so create_item catches it and returns a 400
            raise ValueError(full_msg)


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a STAC item or collection using the fast Python validator service.

    If stac_data is already a Pydantic model object, Pydantic validation is skipped
    (assuming it was already validated by FastAPI). Fast validator is run if enabled.

    Args:
        stac_data: STAC data as dict or Pydantic model object.
        pydantic_model: The Pydantic model class to use for validation (Item or Collection).

    Returns:
        Validated STAC object (Item or Collection).

    Raises:
        ValueError: If validation fails.
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

    # 2. Fast Validator (High Performance)
    if get_bool_env("ENABLE_FAST_VALIDATOR"):
        _validate_with_fast_validator_service(stac_dict)
        return stac_obj

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
