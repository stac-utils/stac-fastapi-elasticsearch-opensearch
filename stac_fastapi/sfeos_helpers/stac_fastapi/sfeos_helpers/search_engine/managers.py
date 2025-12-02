"""Index management utilities."""

import logging
import os
from datetime import timedelta
from typing import Any, Dict, NamedTuple

from dateutil import parser  # type: ignore
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
        start_datetime (str | None): ISO format start datetime string or None.
        datetime (str | None): ISO format datetime string or None.
        end_datetime (str | None): ISO format end datetime string or None.
    """

    start_datetime: str | None
    datetime: str | None
    end_datetime: str | None


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
    def validate_product_datetimes(
        product: Dict[str, Any], use_datetime
    ) -> ProductDatetimes:
        """Validate and extract datetime fields from product.

        Validation rules depend on USE_DATETIME:
        - USE_DATETIME=True: 'datetime' is required, optional start/end
        - USE_DATETIME=False: both 'start_datetime' and 'end_datetime' required, start <= end

        Args:
            product (Dict[str, Any]): Product data containing datetime information.
            use_datetime (bool): Flag determining validation mode.
            - True: validates against 'datetime' field.
            - False: validates against 'start_datetime' and 'end_datetime' fields.

        Returns:
            ProductDatetimes: Named tuple containing parsed datetime values:
                - start_datetime (str | None): ISO 8601 start datetime string or None.
                - datetime (str | None): ISO 8601 datetime string or None.
                - end_datetime (str | None): ISO 8601 end datetime string or None.

        Raises:
            HTTPException: If validation fails based on USE_DATETIME configuration.
        """
        properties = product.get("properties", {})
        start_str = properties.get("start_datetime")
        dt_str = properties.get("datetime")
        end_str = properties.get("end_datetime")

        start = parser.isoparse(start_str) if start_str else None
        dt = parser.isoparse(dt_str) if dt_str else None
        end = parser.isoparse(end_str) if end_str else None

        if use_datetime:
            if not dt:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'datetime' field is required",
                )
        else:
            if not start or not end:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Both 'start_datetime' and 'end_datetime' fields are required",
                )
            if not (start <= end):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'start_datetime' must be <= 'end_datetime'",
                )
            if dt and not (start <= dt <= end):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'start_datetime' <= 'datetime' <= 'end_datetime' is required",
                )

        return ProductDatetimes(
            start_datetime=start_str,
            datetime=dt_str,
            end_datetime=end_str,
        )

    async def handle_new_collection(
        self,
        collection_id: str,
        primary_datetime_name: str,
        product_datetimes: ProductDatetimes,
    ) -> str:
        """Handle index creation for new collection asynchronously.

        Args:
            collection_id (str): Collection identifier.
            primary_datetime_name (str): Name of the primary datetime field.
                If "start_datetime", indexes are created on start_datetime and end_datetime fields.
                If "datetime", indexes are created on the datetime field.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.

        Returns:
            str: Created datetime index name.
        """
        index_params = {
            "start_datetime": str(extract_date(product_datetimes.start_datetime))
            if primary_datetime_name == "start_datetime"
            else None,
            "datetime": str(extract_date(product_datetimes.datetime))
            if primary_datetime_name == "datetime"
            else None,
            "end_datetime": str(extract_date(product_datetimes.end_datetime))
            if primary_datetime_name == "start_datetime"
            else None,
        }

        target_index = await self.index_operations.create_datetime_index(
            self.client, collection_id, **index_params
        )

        logger.info(
            f"Successfully created index '{target_index}' for collection '{collection_id}'"
        )
        return target_index

    async def handle_early_date(
        self,
        collection_id: str,
        primary_datetime_name: str,
        product_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
    ) -> str:
        """Handle product with date earlier than existing indexes asynchronously.

        Args:
            collection_id (str): Collection identifier.
            primary_datetime_name (str): Name of the primary datetime field.
                If "start_datetime", handles start_datetime and end_datetime fields.
                If "datetime", handles the datetime field.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.
            old_aliases (Dict[str, str]): Dictionary mapping alias types to their current names.

        Returns:
            str: Updated datetime alias name.
        """
        new_aliases = []
        old_alias_names = []

        if primary_datetime_name == "start_datetime":
            new_start_alias = self.index_operations.create_alias_name(
                collection_id,
                "start_datetime",
                str(extract_date(product_datetimes.start_datetime)),
            )

            if extract_date(
                product_datetimes.start_datetime
            ) < extract_first_date_from_index(old_aliases["start_datetime"]):
                new_aliases.append(new_start_alias)
                old_alias_names.append(old_aliases["start_datetime"])

            new_end_alias = self.index_operations.create_alias_name(
                collection_id,
                "end_datetime",
                str(extract_date(product_datetimes.end_datetime)),
            )

            if extract_date(
                product_datetimes.end_datetime
            ) > extract_first_date_from_index(old_aliases["end_datetime"]):
                new_aliases.append(new_end_alias)
                old_alias_names.append(old_aliases["end_datetime"])

            new_primary_alias = new_start_alias
        else:

            new_primary_alias = self.index_operations.create_alias_name(
                collection_id, "datetime", str(extract_date(product_datetimes.datetime))
            )

            if extract_date(product_datetimes.datetime) < extract_first_date_from_index(
                old_aliases["datetime"]
            ):
                new_aliases.append(new_primary_alias)
                old_alias_names.append(old_aliases["datetime"])

        if old_alias_names:
            await self.index_operations.change_alias_name(
                self.client,
                old_aliases[primary_datetime_name],
                old_alias_names,
                new_aliases,
            )

        return new_primary_alias

    async def handle_oversized_index(
        self,
        collection_id: str,
        primary_datetime_name: str,
        product_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
    ) -> str:
        """Handle index that exceeds size limit asynchronously.

        Args:
            collection_id (str): Collection identifier.
            primary_datetime_name (str): Name of the primary datetime field.
                If "start_datetime", handles start_datetime and end_datetime fields.
                If "datetime", handles the datetime field.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.
            old_aliases (Dict[str, str]): Dictionary mapping alias types to their current names.

        Returns:
            str: Updated or newly created datetime alias name.
        """
        current_alias = old_aliases[primary_datetime_name]
        new_aliases = []
        old_alias_names = []

        if primary_datetime_name == "start_datetime":
            start_dt = extract_date(product_datetimes.start_datetime)
            end_dt = extract_date(product_datetimes.end_datetime)
            old_start_dt = extract_first_date_from_index(current_alias)
            old_end_dt = extract_first_date_from_index(old_aliases["end_datetime"])

            if start_dt != old_start_dt:
                new_start_alias = f"{current_alias}-{str(start_dt)}"
                new_aliases.append(new_start_alias)
                old_alias_names.append(current_alias)

            if end_dt > old_end_dt:
                new_end_alias = self.index_operations.create_alias_name(
                    collection_id, "end_datetime", str(end_dt)
                )
                new_aliases.append(new_end_alias)
                old_alias_names.append(old_aliases["end_datetime"])

            if old_alias_names:
                await self.index_operations.change_alias_name(
                    self.client, current_alias, old_alias_names, new_aliases
                )

            if start_dt != old_start_dt:
                return await self.index_operations.create_datetime_index(
                    self.client,
                    collection_id,
                    start_datetime=str(start_dt + timedelta(days=1)),
                    datetime=None,
                    end_datetime=str(end_dt),
                )
        else:
            dt = extract_date(product_datetimes.datetime)
            old_dt = extract_first_date_from_index(current_alias)

            if dt != old_dt:
                new_datetime_alias = f"{current_alias}-{str(dt)}"
                await self.index_operations.change_alias_name(
                    self.client, current_alias, [current_alias], [new_datetime_alias]
                )
                return await self.index_operations.create_datetime_index(
                    self.client,
                    collection_id,
                    start_datetime=None,
                    datetime=str(dt + timedelta(days=1)),
                    end_datetime=None,
                )

        return current_alias
