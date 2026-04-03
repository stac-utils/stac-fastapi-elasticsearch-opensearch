"""Async index insertion strategies."""

import logging
from typing import Any

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
    index_alias_by_collection_id,
    is_index_closed,
    mk_item_id,
)

from .base import BaseIndexInserter
from .index_operations import IndexOperations
from .managers import DatetimeIndexManager, ProductDatetimes
from .selection import DatetimeBasedIndexSelector

logger = logging.getLogger(__name__)


class DatetimeIndexInserter(BaseIndexInserter):
    """Async datetime-based index insertion strategy."""

    def __init__(self, client: Any, index_operations: IndexOperations):
        """Initialize the async datetime index inserter.

        Args:
            client: Async search engine client instance.
            index_operations (IndexOperations): Search engine adapter instance.
        """
        self.client = client
        self.index_operations = index_operations
        self.datetime_manager = DatetimeIndexManager(client, index_operations)
        self.index_selector = DatetimeBasedIndexSelector(client)

    @staticmethod
    def should_create_collection_index() -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: False, as datetime strategy doesn't create collection indexes.
        """
        return False

    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index asynchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        return await self.index_operations.create_simple_index(client, collection_id)

    async def refresh_cache(self) -> None:
        """Refresh the index selector cache.

        This method refreshes the cached index information used for
        datetime-based index selection.
        """
        await self.index_selector.refresh_cache()

    def validate_datetime_field_update(self, field_path: str) -> None:
        """Validate if a datetime field can be updated.

        For datetime-based indexing, the primary datetime field cannot be modified
        because it determines the index where the item is stored.

        Args:
            field_path (str): The path of the field being updated.
        """
        # TODO: In the future, updating these fields will be able to move an item between indices by changing the time-based aliases
        if field_path in ("properties/start_datetime", "properties/end_datetime"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Updating '{field_path}' is not yet supported for datetime-based indexing. "
                    "This feature will be available in a future release, enabling automatic "
                    "index and time-based alias updates when datetime values change."
                ),
            )

    async def get_target_index(
        self, collection_id: str, product: dict[str, Any]
    ) -> str:
        """Get target index for a single product.

        Args:
            collection_id (str): Collection identifier.
            product (dict[str, Any]): Product data containing datetime information.

        Returns:
            str: Target index name for the product.
        """
        return await self._get_target_index_internal(
            collection_id, product, check_size=True, use_cache=True
        )

    async def prepare_bulk_actions(
        self, collection_id: str, items: list[dict[str, Any]], op_type: str = "create"
    ) -> list[dict[str, Any]]:
        """Prepare bulk actions for multiple items.

        Args:
            collection_id (str): Collection identifier.
            items (list[dict[str, Any]]): List of items to process.
            op_type (str): The operation type for the bulk actions. "create" for insert-only
                (rejects duplicates), "index" for upsert. Defaults to "create".

        Returns:
            list[dict[str, Any]]: List of bulk actions ready for execution.
        """
        if not items:
            msg = "The product list cannot be empty."
            logger.error(msg)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

        items.sort(key=lambda item: item["properties"]["start_datetime"])

        first_item = items[0]
        first_target_index = await self._get_target_index_internal(
            collection_id, first_item, check_size=True, use_cache=True
        )

        actions = [
            {
                "_op_type": op_type,
                "_index": first_target_index,
                "_id": mk_item_id(first_item["id"], first_item["collection"]),
                "_source": first_item,
            }
        ]

        for item in items[1:]:
            target_index = await self._get_target_index_internal(
                collection_id, item, check_size=False, use_cache=True
            )
            actions.append(
                {
                    "_op_type": op_type,
                    "_index": target_index,
                    "_id": mk_item_id(item["id"], item["collection"]),
                    "_source": item,
                }
            )

        return actions

    async def _get_target_index_internal(
        self,
        collection_id: str,
        product: dict[str, Any],
        check_size: bool = True,
        use_cache: bool = True,
    ) -> str | None:
        """Get target index with size checking internally.

        Args:
            collection_id (str): Collection identifier.
            product (dict[str, Any]): Product data.
            check_size (bool): Whetheru to check index size limits.

        Returns:
            str: Target index name.
        """
        product_datetimes = self.datetime_manager.validate_product_datetimes(product)

        all_indexes = await self.index_selector.get_collection_indexes(
            collection_id, use_cache=use_cache
        )

        if not all_indexes:
            target_index = await self.datetime_manager.handle_new_collection(
                collection_id, product_datetimes
            )
            await self.refresh_cache()
            return target_index

        all_indexes = sorted(all_indexes, key=lambda x: x[0]["start_datetime"])

        target_index = await self.index_selector.select_indexes(
            [collection_id], product_datetimes.start_datetime, for_insertion=True
        )

        start_date = extract_date(product_datetimes.start_datetime)
        earliest_index_date = extract_first_date_from_index(
            all_indexes[0][0]["start_datetime"]
        )

        if start_date < earliest_index_date:
            target_index = await self.datetime_manager.handle_early_date(
                collection_id,
                product_datetimes,
                all_indexes[0][0],
                True,
            )
            await self.refresh_cache()
            return target_index

        if not target_index:
            target_index = all_indexes[-1][0]["start_datetime"]

        aliases_dict, is_first_index = self._find_aliases_for_index(
            all_indexes, target_index
        )

        if target_index != all_indexes[-1][0]["start_datetime"]:
            await self.datetime_manager.handle_early_date(
                collection_id,
                product_datetimes,
                aliases_dict,
                is_first_index,
            )
            await self.refresh_cache()
            return target_index

        if check_size and await self.datetime_manager.size_manager.is_index_oversized(
            target_index
        ):
            latest_item = await self.index_operations.find_latest_item_in_index(
                self.client, target_index
            )
            latest_index_datetimes = ProductDatetimes(
                start_datetime=str(
                    extract_date(latest_item["_source"]["properties"]["start_datetime"])
                ),
                datetime=None,
                end_datetime=str(
                    extract_first_date_from_index(aliases_dict["end_datetime"])
                )
                if aliases_dict.get("end_datetime")
                else None,
            )

            is_first_split = not any(
                is_index_closed(idx[0].get("start_datetime")) for idx in all_indexes
            )

            await self.datetime_manager.handle_oversized_index(
                collection_id,
                product_datetimes,
                latest_index_datetimes,
                aliases_dict,
                is_first_split=is_first_split,
            )
            await self.refresh_cache()
            all_indexes = await self.index_selector.get_collection_indexes(
                collection_id, use_cache=use_cache
            )
            all_indexes = sorted(all_indexes, key=lambda x: x[0]["start_datetime"])
            return (
                await self.index_selector.select_indexes(
                    [collection_id],
                    product_datetimes.start_datetime,
                    for_insertion=True,
                )
                or all_indexes[-1][0]["start_datetime"]
            )

        await self.datetime_manager.handle_early_date(
            collection_id,
            product_datetimes,
            aliases_dict,
            is_first_index,
        )
        await self.refresh_cache()
        all_indexes = await self.index_selector.get_collection_indexes(
            collection_id, use_cache=use_cache
        )
        all_indexes = sorted(all_indexes, key=lambda x: x[0]["start_datetime"])
        return all_indexes[-1][0]["start_datetime"]

    @staticmethod
    def _find_aliases_for_index(
        all_indexes: list, target_index: str
    ) -> tuple[dict[str, Any] | None, bool]:
        """Find aliases for a given index.

        Args:
            all_indexes: List of index alias dictionaries.
            target_index: Target index name to find.

        Returns:
            Tuple of (aliases_dict or None, is_first_element).
        """
        for idx, item in enumerate(all_indexes):
            aliases_dict = item[0]
            if target_index in aliases_dict.values():
                return aliases_dict, idx == 0
        return None, False


class SimpleIndexInserter(BaseIndexInserter):
    """Simple async index insertion strategy."""

    def __init__(self, index_operations: IndexOperations, client: Any):
        """Initialize the async simple index inserter.

        Args:
            index_operations (IndexOperations): Search engine adapter instance.
            client: Async search engine client instance.
        """
        self.search_adapter = index_operations
        self.client = client

    @staticmethod
    def should_create_collection_index() -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: True, as simple strategy creates collection indexes.
        """
        return True

    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index asynchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        return await self.search_adapter.create_simple_index(client, collection_id)

    async def get_target_index(
        self, collection_id: str, product: dict[str, Any]
    ) -> str:
        """Get target index (always the collection alias).

        Args:
            collection_id (str): Collection identifier.
            product (dict[str, Any]): Product data (not used in simple strategy).

        Returns:
            str: Collection alias name.
        """
        return index_alias_by_collection_id(collection_id)

    async def prepare_bulk_actions(
        self, collection_id: str, items: list[dict[str, Any]], op_type: str = "create"
    ) -> list[dict[str, Any]]:
        """Prepare bulk actions for simple indexing.

        Args:
            collection_id (str): Collection identifier.
            items (list[dict[str, Any]]): List of items to process.
            op_type (str): The operation type for the bulk actions. "create" for insert-only
                (rejects duplicates), "index" for upsert. Defaults to "create".

        Returns:
            list[dict[str, Any]]: List of bulk actions with collection alias as target.
        """
        target_index = index_alias_by_collection_id(collection_id)
        return [
            {
                "_op_type": op_type,
                "_index": target_index,
                "_id": mk_item_id(item["id"], item["collection"]),
                "_source": item,
            }
            for item in items
        ]
