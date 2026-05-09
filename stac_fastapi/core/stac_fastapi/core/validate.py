"""STAC validation module.

Provides validation for STAC items and collections.
- Pydantic validation (always enabled)
- Fast Python validator microservice (high-performance, concurrent over HTTP)
"""

import logging
import os

import httpx
from stac_pydantic import Collection, Item

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)

FAST_VALIDATOR_URL = os.getenv(
    "FAST_VALIDATOR_URL", "http://stac-validator:8000/validate"
)


# ---------------------------------------------------------
# RESPONSE PARSING HELPERS
# ---------------------------------------------------------


def _parse_batch_response(
    items: list[dict], response_data: dict | list
) -> tuple[list[dict], dict[str, str]]:
    """Extract valid and invalid items from the Fast Validator response payload."""
    batch_data = (
        response_data[0]
        if isinstance(response_data, list) and response_data
        else response_data
    )

    if batch_data.get("valid_stac", False):
        return items, {}

    invalid_items = {}
    valid_item_ids = {item.get("id") for item in items}

    for error in batch_data.get("errors", []):
        err_msg = error.get("error_message", "Validation failed")
        for item_id in error.get("affected_items", []):
            invalid_items[item_id] = err_msg
            valid_item_ids.discard(item_id)

    valid_items = [item for item in items if item.get("id") in valid_item_ids]
    return valid_items, invalid_items


def _check_single_validation_error(response_data: dict | list) -> None:
    """Raise a ValueError if a single STAC object fails validation."""
    batch_data = (
        response_data[0]
        if isinstance(response_data, list) and response_data
        else response_data
    )

    if not batch_data.get("valid_stac", False):
        errors = batch_data.get("errors", [])
        msg = (
            errors[0].get("error_message", "Validation failed")
            if errors
            else "Validation failed"
        )
        full_msg = f"Fast Validator Rejected STAC: {msg}"
        logger.error(full_msg)
        raise ValueError(full_msg)


# ---------------------------------------------------------
# BATCH VALIDATION (Used by Background Worker)
# ---------------------------------------------------------


def validate_batch_with_fast_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Validate a batch of STAC items using the fast validator microservice (Sync)."""
    if not items:
        return [], {}

    with httpx.Client(timeout=30.0) as client:
        try:
            payload = {"type": "FeatureCollection", "features": items}
            response = client.post(FAST_VALIDATOR_URL, json=payload)
            response.raise_for_status()
            return _parse_batch_response(items, response.json())

        except Exception as e:
            logger.error(f"Fast validator request failed: {e}")
            return (
                [],
                {
                    item.get(
                        "id", "unknown_id"
                    ): "Fast validator unreachable or failed."
                    for item in items
                },
            )


async def async_validate_batch_with_fast_validator(
    items: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Validate a batch of STAC items using the fast validator microservice (Async)."""
    if not items:
        return [], {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            payload = {"type": "FeatureCollection", "features": items}
            response = await client.post(FAST_VALIDATOR_URL, json=payload)
            response.raise_for_status()
            return _parse_batch_response(items, response.json())

        except Exception as e:
            logger.error(f"Fast validator request failed: {e}")
            return (
                [],
                {
                    item.get(
                        "id", "unknown_id"
                    ): "Fast validator unreachable or failed."
                    for item in items
                },
            )


# ---------------------------------------------------------
# SINGLE VALIDATION (Used by Direct API endpoints)
# ---------------------------------------------------------


def validate_stac(
    stac_data: dict | Item | Collection,
    pydantic_model: type[Item] | type[Collection] = Item,
) -> Item | Collection:
    """Validate a STAC item or collection using Pydantic and the microservice (Sync)."""
    # 1. Pydantic Parsing/Validation
    if isinstance(stac_data, (Item, Collection)):
        stac_obj = stac_data
        stac_dict = stac_data.model_dump(mode="json")
    else:
        stac_obj = pydantic_model(**stac_data)
        stac_dict = stac_data

    # 2. Fast Validator Microservice (HTTP)
    if get_bool_env("ENABLE_FAST_VALIDATOR"):
        with httpx.Client(timeout=30.0) as client:
            try:
                response = client.post(FAST_VALIDATOR_URL, json=stac_dict)
                response.raise_for_status()
            except httpx.RequestError as exc:
                logger.error(f"Networking error to fast validator: {exc}")
                raise RuntimeError(
                    f"Fast validator unreachable at {FAST_VALIDATOR_URL}"
                ) from exc

            _check_single_validation_error(response.json())

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
    """Validate a STAC item or collection using Pydantic and the microservice (Async)."""
    # 1. Pydantic Parsing/Validation
    if isinstance(stac_data, (Item, Collection)):
        stac_obj = stac_data
        stac_dict = stac_data.model_dump(mode="json")
    else:
        stac_obj = pydantic_model(**stac_data)
        stac_dict = stac_data

    # 2. Fast Validator Microservice (Native Async HTTP)
    if get_bool_env("ENABLE_FAST_VALIDATOR"):
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(FAST_VALIDATOR_URL, json=stac_dict)
                response.raise_for_status()
            except httpx.RequestError as exc:
                logger.error(f"Networking error to fast validator: {exc}")
                raise RuntimeError(
                    f"Fast validator unreachable at {FAST_VALIDATOR_URL}"
                ) from exc

            _check_single_validation_error(response.json())

    return stac_obj


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
