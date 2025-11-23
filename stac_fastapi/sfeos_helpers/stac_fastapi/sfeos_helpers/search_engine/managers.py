"""Index management utilities."""

import logging
import os
from datetime import timedelta
from typing import Any, Dict, NamedTuple

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
)

from .index_operations import IndexOperations

logger = logging.getLogger(__name__)


class ProductDatetimes(NamedTuple):
    """Named tuple representing product datetime fields.

    Attributes:
        start_datetime (str): ISO format start datetime string.
        datetime (str): ISO format datetime string.
        end_datetime (str): ISO format end datetime string.
    """

    start_datetime: str
    datetime: str
    end_datetime: str


class IndexSizeManager:
    """Manages index size limits and operations."""

    def __init__(self, client: Any):
        """Initialize the index size manager.

        Args:
            client: Search engine client instance.
        """
        self.client = client
        self.max_size_gb = self._get_max_size_from_env()

    async def get_index_size_in_gb(self, index_name: str) -> float:
        """Get index size in gigabytes asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            float: Size of the index in gigabytes.
        """
        data = await self.client.indices.stats(index=index_name)
        return data["_all"]["primaries"]["store"]["size_in_bytes"] / 1e9

    async def is_index_oversized(self, index_name: str) -> bool:
        """Check if index exceeds size limit asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            bool: True if index exceeds size limit, False otherwise.
        """
        size_gb = await self.get_index_size_in_gb(index_name)
        is_oversized = size_gb > self.max_size_gb
        gb_milestone = int(size_gb)
        if gb_milestone > 0:
            logger.info(f"Index '{index_name}' size: {gb_milestone}GB")

        if is_oversized:
            logger.warning(
                f"Index '{index_name}' is oversized: {size_gb:.2f} GB "
                f"(limit: {self.max_size_gb} GB)"
            )

        return is_oversized

    @staticmethod
    def _get_max_size_from_env() -> float:
        """Get max size from environment variable with error handling.

        Returns:
            float: Maximum index size in GB.

        Raises:
            ValueError: If environment variable contains invalid value.
        """
        env_value = os.getenv("DATETIME_INDEX_MAX_SIZE_GB", "25")

        try:
            max_size = float(env_value)
            if max_size <= 0:
                raise ValueError(
                    f"DATETIME_INDEX_MAX_SIZE_GB must be positive, got: {max_size}"
                )
            return max_size
        except (ValueError, TypeError):
            error_msg = (
                f"Invalid value for DATETIME_INDEX_MAX_SIZE_GB environment variable: "
                f"'{env_value}'. Must be a positive number. Using default value 25.0 GB."
            )
            logger.warning(error_msg)

        return 25.0


class DatetimeIndexManager:
    """Manages datetime-based index operations."""

    def __init__(self, client: Any, index_operations: IndexOperations):
        """Initialize the datetime index manager.

        Args:
            client: Search engine client instance.
            index_operations (IndexOperations): Search engine adapter instance.
        """
        self.client = client
        self.index_operations = index_operations
        self.size_manager = IndexSizeManager(client)

    @staticmethod
    def validate_product_datetimes(product: Dict[str, Any]) -> ProductDatetimes:
        """Validate and extract datetime fields from product.

        Args:
            product (Dict[str, Any]): Product data containing datetime information.

        Returns:
            ProductDatetimes: Named tuple containing:
                - start_datetime (str): Start datetime value
                - datetime (str): datetime value
                - end_datetime (str): End datetime value

        Raises:
            HTTPException: If product start_datetime is missing or invalid.
        """
        properties = product.get("properties", {})
        start_datetime_value = properties.get("start_datetime")
        datetime_value = properties.get("datetime")
        end_datetime_value = properties.get("end_datetime")

        if not start_datetime_value or not datetime_value or not end_datetime_value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Product 'start_datetime', 'datetime' and 'end_datetime' is required for indexing",
            )

        if not (start_datetime_value <= datetime_value <= end_datetime_value):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'start_datetime' <= 'datetime' <= 'end_datetime' is required",
            )

        return ProductDatetimes(
            start_datetime=start_datetime_value,
            datetime=datetime_value,
            end_datetime=end_datetime_value,
        )

    async def handle_new_collection(
        self, collection_id: str, product_datetimes: ProductDatetimes
    ) -> str:
        """Handle index creation for new collection asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.

        Returns:
            str: Created start_datetime alias name.
        """
        start_datetime = extract_date(product_datetimes.start_datetime)
        datetime = extract_date(product_datetimes.datetime)
        end_datetime = extract_date(product_datetimes.end_datetime)

        target_index = await self.index_operations.create_datetime_index(
            self.client,
            collection_id,
            str(start_datetime),
            str(datetime),
            str(end_datetime),
        )
        logger.info(
            f"Successfully created index '{target_index}' for collection '{collection_id}'"
        )
        return target_index

    async def handle_early_date(
        self,
        collection_id: str,
        product_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
    ) -> str:
        """Handle product with date earlier than existing indexes asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.
            old_aliases (Dict[str, str]): Dictionary mapping alias types to their current names.

        Returns:
            str: Updated start_datetime alias name.
        """
        start_dt = extract_date(product_datetimes.start_datetime)
        dt = extract_date(product_datetimes.datetime)
        end_dt = extract_date(product_datetimes.end_datetime)

        old_start_datetime_alias = extract_first_date_from_index(
            old_aliases["start_datetime"]
        )
        old_datetime_alias = extract_first_date_from_index(old_aliases["datetime"])
        old_end_datetime_alias = extract_first_date_from_index(
            old_aliases["end_datetime"]
        )

        new_start_datetime_alias = self.index_operations.create_alias_name(
            collection_id, "start_datetime", str(start_dt)
        )

        aliases_to_change = []
        aliases_to_create = []

        if start_dt < old_start_datetime_alias:
            aliases_to_create.append(new_start_datetime_alias)
            aliases_to_change.append(old_aliases["start_datetime"])

        if dt > old_datetime_alias:
            new_datetime_alias = self.index_operations.create_alias_name(
                collection_id, "datetime", str(dt)
            )
            aliases_to_create.append(new_datetime_alias)
            aliases_to_change.append(old_aliases["datetime"])

        if end_dt > old_end_datetime_alias:
            new_end_datetime_alias = self.index_operations.create_alias_name(
                collection_id, "end_datetime", str(end_dt)
            )
            aliases_to_create.append(new_end_datetime_alias)
            aliases_to_change.append(old_aliases["end_datetime"])

        if aliases_to_change:
            await self.index_operations.change_alias_name(
                self.client,
                old_aliases["start_datetime"],
                aliases_to_change,
                aliases_to_create,
            )
        return new_start_datetime_alias

    async def handle_oversized_index(
        self,
        collection_id: str,
        product_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
    ) -> str:
        """Handle index that exceeds size limit asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.
            old_aliases (Dict[str, str]): Dictionary mapping alias types to their current names.

        Returns:
            str: Updated or newly created start_datetime alias name.
        """
        target_index = old_aliases["start_datetime"]
        start_dt = extract_date(product_datetimes.start_datetime)
        dt = extract_date(product_datetimes.datetime)
        end_dt = extract_date(product_datetimes.end_datetime)

        old_start_datetime_alias = extract_first_date_from_index(target_index)
        old_datetime_alias = extract_first_date_from_index(old_aliases["datetime"])
        old_end_datetime_alias = extract_first_date_from_index(
            old_aliases["end_datetime"]
        )

        if start_dt != old_start_datetime_alias:
            aliases_to_change = []
            aliases_to_create = []

            new_start_datetime_alias = f"{target_index}-{str(start_dt)}"
            aliases_to_create.append(new_start_datetime_alias)
            aliases_to_change.append(target_index)

            if dt > old_datetime_alias:
                new_datetime_alias = self.index_operations.create_alias_name(
                    collection_id, "datetime", str(dt)
                )
                aliases_to_create.append(new_datetime_alias)
                aliases_to_change.append(old_aliases["datetime"])

            if end_dt > old_end_datetime_alias:
                new_end_datetime_alias = self.index_operations.create_alias_name(
                    collection_id, "end_datetime", str(end_dt)
                )
                aliases_to_create.append(new_end_datetime_alias)
                aliases_to_change.append(old_aliases["end_datetime"])

            await self.index_operations.change_alias_name(
                self.client, target_index, aliases_to_change, aliases_to_create
            )
            target_index = await self.index_operations.create_datetime_index(
                self.client,
                collection_id,
                str(start_dt + timedelta(days=1)),
                str(dt),
                str(end_dt),
            )

        return target_index
