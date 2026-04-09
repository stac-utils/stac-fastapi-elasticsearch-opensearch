"""Index management utilities."""

import logging
import os
from datetime import date, timedelta
from typing import Any, Dict, NamedTuple

from dateutil import parser  # type: ignore
from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
    extract_last_date_from_index,
    is_index_closed,
)

from .index_operations import IndexOperations

logger = logging.getLogger(__name__)


class ProductDatetimes(NamedTuple):
    """Named tuple representing product datetime fields.

    Attributes:
        start_datetime (str): ISO format start datetime string.
        end_datetime (str): ISO format end datetime string.
    """

    start_datetime: str
    end_datetime: str


class IndexOperationResult(NamedTuple):
    """Result of an index/alias operation.

    Attributes:
        target_alias (str): Alias to use for insertion.
        new_aliases_entry (dict[str, str] | None): Updated aliases dict if OS/ES was
            modified, None otherwise (no I/O needed on cache).
        old_start_datetime_alias (str | None): Previous start_datetime alias replaced;
            None for new entries.
    """

    target_alias: str
    new_aliases_entry: dict[str, str] | None
    old_start_datetime_alias: str | None


class IndexSizeManager:
    """Manages index size limits and operations."""

    def __init__(self, client: Any):
        """Initialize the index size manager.

        Args:
            client: Search engine client instance.
        """
        self.client = client
        self.max_size_gb = self._get_max_size_from_env()

    async def is_index_oversized(self, index_name: str) -> bool:
        """Check if index exceeds size limit asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            bool: True if index exceeds size limit, False otherwise.
        """
        await self.client.indices.refresh(index=index_name)
        stats = await self.client.indices.stats(index=index_name)

        total_size_bytes = 0
        total_doc_count = 0

        for idx_name, idx_stats in stats["indices"].items():
            primaries = idx_stats["primaries"]
            total_size_bytes += primaries["store"]["size_in_bytes"]
            total_doc_count += primaries["docs"]["count"]

        if total_doc_count == 0:
            logger.debug(f"Index '{index_name}' is empty (0 documents)")
            return False

        size_gb = total_size_bytes / (1024**3)
        is_oversized = size_gb > self.max_size_gb
        gb_milestone = int(size_gb)

        if gb_milestone > 0:
            logger.info(
                f"Index '{index_name}' size: {gb_milestone}GB ({total_doc_count} documents)"
            )

        if is_oversized:
            logger.warning(
                f"Index '{index_name}' is oversized: {size_gb:.2f} GB "
                f"(limit: {self.max_size_gb} GB, documents: {total_doc_count})"
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
    def validate_product_datetimes(product: dict[str, Any]) -> ProductDatetimes:
        """Validate and extract datetime fields from product.

        Args:
            product (Dict[str, Any]): Product data containing datetime information..

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
            end_datetime=end_str,
        )

    async def handle_new_collection(
        self,
        collection_id: str,
        product_datetimes: ProductDatetimes,
    ) -> IndexOperationResult:
        """Handle index creation for new collection asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Object containing start_datetime, datetime, and end_datetime.

        Returns:
            IndexOperationResult: Result containing the created index alias and cache update info.
        """
        start_date = str(extract_date(product_datetimes.start_datetime))
        end_date = str(extract_date(product_datetimes.end_datetime))

        start_alias, end_alias = await self.index_operations.create_datetime_index(
            self.client, collection_id, start_datetime=start_date, end_datetime=end_date
        )

        logger.info(
            f"Successfully created index '{start_alias}' for collection '{collection_id}'"
        )
        return IndexOperationResult(
            target_alias=start_alias,
            new_aliases_entry={
                "start_datetime": start_alias,
                "end_datetime": end_alias,
            },
            old_start_datetime_alias=None,
        )

    async def handle_early_date(
        self,
        collection_id: str,
        product_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
        is_first_index: bool,
    ) -> IndexOperationResult:
        """Handle product with datetime earlier than current index range.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Product datetime values.
            old_aliases (Dict[str, str]): Current datetime aliases.
            is_first_index (bool): Whether this is the first index in the collection.

        Returns:
            IndexOperationResult: Result containing target alias and cache update info.
                new_aliases_entry is None when no changes were made (most common path).
        """
        product_start = extract_date(product_datetimes.start_datetime)
        product_end = extract_date(product_datetimes.end_datetime)

        index_start = extract_first_date_from_index(old_aliases["start_datetime"])
        index_end = extract_first_date_from_index(old_aliases["end_datetime"])
        index_is_closed = is_index_closed(old_aliases["start_datetime"])

        start_changed = product_start < index_start
        end_changed = product_end > index_end

        if not start_changed and not end_changed:
            return IndexOperationResult(
                target_alias=old_aliases["start_datetime"],
                new_aliases_entry=None,
                old_start_datetime_alias=None,
            )

        new_aliases = []
        old_alias_names = []
        new_primary_alias = old_aliases["start_datetime"]

        if start_changed:
            if index_is_closed and is_first_index:
                new_index_start = f"{product_start}-{index_start - timedelta(days=1)}"
                (
                    created_alias,
                    end_alias,
                ) = await self.index_operations.create_datetime_index(
                    self.client,
                    collection_id,
                    str(new_index_start),
                    str(product_end),
                )
                return IndexOperationResult(
                    target_alias=created_alias,
                    new_aliases_entry={
                        "start_datetime": created_alias,
                        "end_datetime": end_alias,
                    },
                    old_start_datetime_alias=None,
                )
            elif index_is_closed:
                closed_end = extract_last_date_from_index(old_aliases["start_datetime"])
                new_start_alias = self.index_operations.create_alias_name(
                    collection_id, "start_datetime", f"{product_start}-{closed_end}"
                )
            else:
                new_start_alias = self.index_operations.create_alias_name(
                    collection_id, "start_datetime", str(product_start)
                )
            new_aliases.append(new_start_alias)
            old_alias_names.append(old_aliases["start_datetime"])
            new_primary_alias = new_start_alias

        if end_changed:
            new_end_alias = self.index_operations.create_alias_name(
                collection_id, "end_datetime", str(product_end)
            )
            new_aliases.append(new_end_alias)
            old_alias_names.append(old_aliases["end_datetime"])

        if old_alias_names:
            await self.index_operations.change_alias_name(
                self.client,
                old_aliases["start_datetime"],
                old_alias_names,
                new_aliases,
            )

        final_start = (
            new_primary_alias if start_changed else old_aliases["start_datetime"]
        )
        final_end = new_end_alias if end_changed else old_aliases["end_datetime"]
        return IndexOperationResult(
            target_alias=new_primary_alias,
            new_aliases_entry={
                "start_datetime": final_start,
                "end_datetime": final_end,
            },
            old_start_datetime_alias=old_aliases["start_datetime"],
        )

    async def handle_oversized_index(
        self,
        collection_id: str,
        product_datetimes: ProductDatetimes,
        latest_index_datetimes: ProductDatetimes,
        old_aliases: Dict[str, str],
        is_first_split: bool = False,
    ) -> list[IndexOperationResult]:
        """Handle index that exceeds size limit asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetimes (ProductDatetimes): Product datetime values.
            latest_index_datetimes (ProductDatetimes | None): Datetime range of the latest index.
            old_aliases (Dict[str, str]): Current datetime aliases.
            is_first_split (bool): Whether this is the first time the index is split.

        Returns:
            list[IndexOperationResult]: Results for each OS/ES operation performed.
        """
        results: list[IndexOperationResult] = []
        current_alias = old_aliases["start_datetime"]
        old_alias_names = []
        new_aliases = []

        new_start_alias = (
            f"{current_alias}-{str(latest_index_datetimes.start_datetime)}"
        )
        new_aliases.append(new_start_alias)
        old_alias_names.append(current_alias)

        product_start_datetime = parser.isoparse(
            product_datetimes.start_datetime
        ).date()
        latest_start_datetime_in_index = parser.isoparse(
            latest_index_datetimes.start_datetime
        ).date()
        product_end_date = parser.isoparse(product_datetimes.end_datetime).date()
        latest_end_datetime_in_index = parser.isoparse(
            latest_index_datetimes.end_datetime
        ).date()

        if product_start_datetime > latest_start_datetime_in_index:
            end_datetime = latest_end_datetime_in_index
        else:
            end_datetime = max(product_end_date, latest_end_datetime_in_index)

        new_end_alias = self.index_operations.create_alias_name(
            collection_id, "end_datetime", str(end_datetime)
        )
        new_aliases.append(new_end_alias)
        old_alias_names.append(old_aliases["end_datetime"])

        await self.index_operations.change_alias_name(
            self.client, current_alias, old_alias_names, new_aliases
        )
        results.append(
            IndexOperationResult(
                target_alias=new_start_alias,
                new_aliases_entry={
                    "start_datetime": new_start_alias,
                    "end_datetime": new_end_alias,
                },
                old_start_datetime_alias=current_alias,
            )
        )

        if product_start_datetime > latest_start_datetime_in_index:
            end_date = str(parser.isoparse(product_datetimes.end_datetime).date())
        else:
            end_date = str(
                parser.isoparse(latest_index_datetimes.start_datetime).date()
                + timedelta(days=1)
            )

        new_index_start = str(latest_start_datetime_in_index + timedelta(days=1))
        (
            new_index_alias,
            new_index_end_alias,
        ) = await self.index_operations.create_datetime_index(
            self.client,
            collection_id,
            start_datetime=new_index_start,
            end_datetime=end_date,
        )
        results.append(
            IndexOperationResult(
                target_alias=new_index_alias,
                new_aliases_entry={
                    "start_datetime": new_index_alias,
                    "end_datetime": new_index_end_alias,
                },
                old_start_datetime_alias=None,
            )
        )

        if is_first_split:
            epoch_date = date(1970, 1, 11)
            closed_start = extract_first_date_from_index(current_alias)
            historical_end = closed_start - timedelta(days=1)

            historical_start = f"{epoch_date}-{historical_end}"
            (
                hist_alias,
                hist_end_alias,
            ) = await self.index_operations.create_datetime_index(
                self.client,
                collection_id,
                start_datetime=historical_start,
                end_datetime=str(epoch_date),
            )
            logger.info(
                f"Created historical index for collection '{collection_id}' "
                f"covering {epoch_date} to {historical_end}"
            )
            results.append(
                IndexOperationResult(
                    target_alias=hist_alias,
                    new_aliases_entry={
                        "start_datetime": hist_alias,
                        "end_datetime": hist_end_alias,
                    },
                    old_start_datetime_alias=None,
                )
            )

        return results
