"""STAC validation module.

Provides validation for STAC items and collections using the Go Validator microservice.
- Pydantic validation (always enabled)
- Go Validator microservice (high-performance, concurrent)
"""

import asyncio
import logging
import os

import httpx
from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)

GO_VALIDATOR_URL = os.getenv(
    "GO_VALIDATOR_URL", "http://gostac-validator:8080/validate"
)


def validate_batch_with_go(items: list[dict]) -> tuple[list[dict], dict[str, str]]:
    """Validate a batch of STAC items using the Go Validator service.

    Sends items to the Go Validator microservice for concurrent validation.
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
            response = client.post(GO_VALIDATOR_URL, json=items)
            response.raise_for_status()

            batch_data = response.json()
            results = batch_data.get("results", [])

            for idx, item in enumerate(items):
                item_id = item.get("id", f"unknown_id_{idx}")

                if idx < len(results) and results[idx].get("valid", False):
                    valid_items.append(item)
                else:
                    err_msg = "Unknown validation error"
                    if idx < len(results) and results[idx].get("errors"):
                        err_msg = results[idx]["errors"][0].get("message", err_msg)
                        loc = results[idx]["errors"][0].get("instance_location", "")
                        err_msg = f"{err_msg} (at {loc})"

                    invalid_items[item_id] = err_msg

        except Exception:
            for item in items:
                invalid_items[
                    item.get("id", "unknown_id")
                ] = "Go Validator unreachable or failed."

    return valid_items, invalid_items


async def async_validate_batch_with_go(
    items: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Asynchronously validate a batch of STAC items using the Go Validator service.

    Sends items to the Go Validator microservice for concurrent validation.
    Separates valid items from invalid ones based on the validator response.

    Args:
        items: List of STAC item dictionaries to validate.

    Returns:
        Tuple of (valid_items_list, invalid_items_dict) where invalid_items_dict
        maps item IDs to their validation error messages.
    """
    valid_items = []
    invalid_items = {}  # Changed from set to dict

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(GO_VALIDATOR_URL, json=items)
            response.raise_for_status()

            batch_data = response.json()
            results = batch_data.get("results", [])

            for idx, item in enumerate(items):
                item_id = item.get("id", f"unknown_id_{idx}")

                if idx < len(results) and results[idx].get("valid", False):
                    valid_items.append(item)
                else:
                    err_msg = "Unknown validation error"
                    if idx < len(results) and results[idx].get("errors"):
                        err_msg = results[idx]["errors"][0].get("message", err_msg)
                        loc = results[idx]["errors"][0].get("instance_location", "")
                        err_msg = f"{err_msg} (at {loc})"

                    logger.error(f"Batch validation failed for '{item_id}': {err_msg}")
                    # Map the ID to the specific error
                    invalid_items[item_id] = err_msg

        except Exception as exc:
            logger.error(f"Batch validation request failed: {exc}")
            for item in items:
                item_id = item.get("id", "unknown_id")
                invalid_items[
                    item_id
                ] = "Go Validator unreachable or failed to process."

    return valid_items, invalid_items


def _validate_with_go_service(stac_dict: dict) -> None:
    """Validate a STAC item or collection using the Go Validator service.

    Sends a single STAC item or collection to the Go Validator microservice
    for high-speed schema validation. Raises an error if validation fails.

    Args:
        stac_dict: STAC item or collection data as a dictionary.

    Raises:
        RuntimeError: If the Go Validator service is unreachable.
        ValueError: If the STAC data fails validation.
    """
    with httpx.Client(timeout=30.0) as client:
        try:
            response = client.post(GO_VALIDATOR_URL, json=stac_dict)
            response.raise_for_status()
        except httpx.RequestError as exc:
            logger.error(f"Networking error to Go Validator: {exc}")
            raise RuntimeError(
                f"Go Validator unreachable at {GO_VALIDATOR_URL}"
            ) from exc

        # Parse the Go BatchResponse
        batch_data = response.json()

        # Check if there are ANY invalid items in the response
        if batch_data.get("invalid_count", 0) > 0:
            # Dig into the results array to find the specific error
            results = batch_data.get("results", [])
            for res in results:
                if not res.get("valid", True):
                    errors = res.get("errors", [])
                    msg = (
                        errors[0].get("message", "Invalid STAC")
                        if errors
                        else "Validation failed"
                    )
                    loc = errors[0].get("instance_location", "unknown")

                    full_msg = f"Go Validator Rejected STAC: {msg} (at {loc})"
                    logger.error(full_msg)

                    # Raise the ValueError so create_item catches it and returns a 400
                    raise ValueError(full_msg)


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a STAC item or collection using the Go Validator service.

    If stac_data is already a Pydantic model object, Pydantic validation is skipped
    (assuming it was already validated by FastAPI). Go Validator is run if enabled.

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

    # 2. Go Validator (High Performance)
    if get_bool_env("ENABLE_GO_VALIDATOR"):
        _validate_with_go_service(stac_dict)
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
