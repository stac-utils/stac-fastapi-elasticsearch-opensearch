"""Async index insertion strategies."""
import logging
from datetime import timedelta
from typing import Any, Dict, List

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
    index_alias_by_collection_id,
    mk_item_id,
)

from .base import BaseIndexInserter
from .index_operations import IndexOperations
from .managers import DatetimeIndexManager
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

    async def get_target_index(
        self, collection_id: str, product: Dict[str, Any]
    ) -> str:
        """Get target index for a single product.

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data containing datetime information.

        Returns:
            str: Target index name for the product.
        """
        index_selector = DatetimeBasedIndexSelector(self.client)
        return await self._get_target_index_internal(
            index_selector, collection_id, product, check_size=True
        )

    async def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Prepare bulk actions for multiple items.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            List[Dict[str, Any]]: List of bulk actions ready for execution.
        """
        if not items:
            msg = "The product list cannot be empty."
            logger.error(msg)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

        items.sort(key=lambda item: item["properties"]["datetime"])
        index_selector = DatetimeBasedIndexSelector(self.client)

        await self._ensure_indexes_exist(index_selector, collection_id, items)
        await self._check_and_handle_oversized_index(
            index_selector, collection_id, items
        )

        actions = []
        for item in items:
            target_index = await self._get_target_index_internal(
                index_selector, collection_id, item, check_size=False
            )
            actions.append(
                {
                    "_index": target_index,
                    "_id": mk_item_id(item["id"], item["collection"]),
                    "_source": item,
                }
            )

        return actions

    async def _get_target_index_internal(
        self,
        index_selector,
        collection_id: str,
        product: Dict[str, Any],
        check_size: bool = True,
    ) -> str:
        """Get target index with size checking internally.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data.
            check_size (bool): Whetheru to check index size limits.

        Returns:
            str: Target index name.
        """
        product_datetime = self.datetime_manager.validate_product_datetime(product)
        datetime_range = {"gte": product_datetime, "lte": product_datetime}
        target_index = await index_selector.select_indexes(
            [collection_id], datetime_range
        )
        all_indexes = await index_selector.get_collection_indexes(collection_id)

        if not all_indexes:
            target_index = await self.datetime_manager.handle_new_collection(
                collection_id, product_datetime
            )
            await index_selector.refresh_cache()
            return target_index

        all_indexes.sort()
        start_date = extract_date(product_datetime)
        end_date = extract_first_date_from_index(all_indexes[0])

        if start_date < end_date:
            alias = await self.datetime_manager.handle_early_date(
                collection_id, start_date, end_date
            )
            await index_selector.refresh_cache()

            return alias

        if target_index != all_indexes[-1]:
            return target_index

        if check_size and await self.datetime_manager.size_manager.is_index_oversized(
            target_index
        ):
            target_index = await self.datetime_manager.handle_oversized_index(
                collection_id, target_index, product_datetime
            )
            await index_selector.refresh_cache()

        return target_index

    async def _ensure_indexes_exist(
        self, index_selector, collection_id: str, items: List[Dict[str, Any]]
    ):
        """Ensure necessary indexes exist for the items.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.
        """
        all_indexes = await index_selector.get_collection_indexes(collection_id)

        if not all_indexes:
            first_item = items[0]
            await self.index_operations.create_datetime_index(
                self.client,
                collection_id,
                extract_date(first_item["properties"]["datetime"]),
            )
            await index_selector.refresh_cache()

    async def _check_and_handle_oversized_index(
        self, index_selector, collection_id: str, items: List[Dict[str, Any]]
    ) -> None:
        """Check if index is oversized and create new index if needed.

        Checks if the index where the first item would be inserted is oversized.
        If so, creates a new index starting from the next day.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            None
        """
        first_item = items[0]
        first_item_index = await self._get_target_index_internal(
            index_selector, collection_id, first_item, check_size=False
        )

        all_indexes = await index_selector.get_collection_indexes(collection_id)
        all_indexes.sort()
        latest_index = all_indexes[-1]

        if first_item_index != latest_index:
            return None

        if not await self.datetime_manager.size_manager.is_index_oversized(
            first_item_index
        ):
            return None

        latest_item = await self.index_operations.find_latest_item_in_index(
            self.client, latest_index
        )
        product_datetime = latest_item["_source"]["properties"]["datetime"]
        end_date = extract_date(product_datetime)
        await self.index_operations.update_index_alias(
            self.client, str(end_date), latest_index
        )
        next_day_start = end_date + timedelta(days=1)
        await self.index_operations.create_datetime_index(
            self.client, collection_id, str(next_day_start)
        )
        await index_selector.refresh_cache()


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
        self, collection_id: str, product: Dict[str, Any]
    ) -> str:
        """Get target index (always the collection alias).

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data (not used in simple strategy).

        Returns:
            str: Collection alias name.
        """
        return index_alias_by_collection_id(collection_id)

    async def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Prepare bulk actions for simple indexing.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            List[Dict[str, Any]]: List of bulk actions with collection alias as target.
        """
        target_index = index_alias_by_collection_id(collection_id)
        return [
            {
                "_index": target_index,
                "_id": mk_item_id(item["id"], item["collection"]),
                "_source": item,
            }
            for item in items
        ]
