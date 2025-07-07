"""Sync index insertion strategies."""

from datetime import timedelta
from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
    index_alias_by_collection_id,
    mk_item_id,
)

from .adapters import SearchEngineAdapter
from .base import BaseSyncIndexInserter
from .managers import DatetimeIndexManager
from .selection import SyncDatetimeBasedIndexSelector


class SyncDatetimeIndexInserter(BaseSyncIndexInserter):
    """Sync datetime-based index insertion strategy."""

    def __init__(self, client: Any, search_adapter: SearchEngineAdapter):
        """Initialize the sync datetime index inserter.

        Args:
            client: Sync search engine client instance.
            search_adapter (SearchEngineAdapter): Search engine adapter instance.
        """
        self.client = client
        self.search_adapter = search_adapter
        self.datetime_manager = DatetimeIndexManager(client, search_adapter)

    def should_create_collection_index(self) -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: False, as datetime strategy doesn't create collection indexes.
        """
        return False

    def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index synchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        return self.search_adapter.create_simple_index_sync(client, collection_id)

    def get_target_index(self, collection_id: str, product: Dict[str, Any]) -> str:
        """Get target index for a single product.

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data containing datetime information.

        Returns:
            str: Target index name for the product.
        """
        index_selector = SyncDatetimeBasedIndexSelector(self.client)
        return self._get_target_index_internal(
            index_selector, collection_id, product, check_size=True
        )

    def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Prepare bulk actions for multiple items.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            List[Dict[str, Any]]: List of bulk actions ready for execution.
        """
        index_selector = SyncDatetimeBasedIndexSelector(self.client)

        self._ensure_indexes_exist(index_selector, collection_id, items)
        split_info = self._handle_index_splitting(index_selector, collection_id, items)
        return self._create_bulk_actions(
            index_selector, collection_id, items, split_info
        )

    def _get_target_index_internal(
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
            check_size (bool): Whether to check index size limits.

        Returns:
            str: Target index name.
        """
        product_datetime = self.datetime_manager._validate_product_datetime(product)

        datetime_range = {"gte": product_datetime, "lte": product_datetime}
        target_index = index_selector.select_indexes([collection_id], datetime_range)
        all_indexes = index_selector.get_collection_indexes(collection_id)

        if not all_indexes:
            target_index = self.datetime_manager.handle_new_collection_sync(
                collection_id, product_datetime
            )
            index_selector.refresh_cache()
            return target_index

        all_indexes.sort()
        start_date = extract_date(product_datetime)
        end_date = extract_first_date_from_index(all_indexes[0])

        if start_date < end_date:
            alias = self.datetime_manager.handle_early_date_sync(
                collection_id, start_date, end_date
            )
            index_selector.refresh_cache()
            return alias

        if target_index != all_indexes[-1]:
            return target_index

        if check_size and self.datetime_manager.size_manager.is_index_oversized_sync(
            target_index
        ):
            target_index = self.datetime_manager.handle_oversized_index_sync(
                collection_id, target_index, product_datetime
            )
            index_selector.refresh_cache()

        return target_index

    def _ensure_indexes_exist(
        self, index_selector, collection_id: str, items: List[Dict[str, Any]]
    ):
        """Ensure necessary indexes exist for the items.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.
        """
        all_indexes = index_selector.get_collection_indexes(collection_id)

        if not all_indexes:
            first_item = items[0]
            self.search_adapter.create_datetime_index_sync(
                self.client,
                collection_id,
                extract_date(first_item["properties"]["datetime"]),
            )
            index_selector.refresh_cache()

    def _handle_index_splitting(
        self, index_selector, collection_id: str, items: List[Dict[str, Any]]
    ):
        """Handle potential index splitting due to size limits.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            Optional[Dict]: Split information if splitting occurred, None otherwise.
        """
        all_indexes = index_selector.get_collection_indexes(collection_id)
        all_indexes.sort()
        latest_index = all_indexes[-1]

        first_item = items[0]
        first_item_index = self._get_target_index_internal(
            index_selector, collection_id, first_item, check_size=False
        )

        if first_item_index != latest_index:
            return None

        if self.datetime_manager.size_manager.is_index_oversized_sync(first_item_index):
            return self._create_new_index_for_split(
                collection_id, first_item_index, first_item
            )

        return None

    def _create_new_index_for_split(
        self, collection_id: str, latest_index: str, first_item: Dict[str, Any]
    ):
        """Create new index for splitting oversized index.

        Args:
            collection_id (str): Collection identifier.
            latest_index (str): Current latest index name.
            first_item (Dict[str, Any]): First item being processed.

        Returns:
            Optional[Dict]: Split information with new index details.
        """
        current_index_end_date = extract_first_date_from_index(latest_index)
        first_item_date = extract_date(first_item["properties"]["datetime"])

        if first_item_date != current_index_end_date:
            self.search_adapter.update_index_alias_sync(
                self.client, str(current_index_end_date), latest_index
            )
            next_day_start = current_index_end_date + timedelta(days=1)
            new_index = self.search_adapter.create_datetime_index_sync(
                self.client, collection_id, str(next_day_start)
            )
            return {
                "split_date": current_index_end_date,
                "new_index": new_index,
            }
        return None

    def _create_bulk_actions(
        self,
        index_selector,
        collection_id: str,
        items: List[Dict[str, Any]],
        split_info: Optional[Dict],
    ) -> List[Dict[str, Any]]:
        """Create bulk actions for all items.

        Args:
            index_selector: Index selector instance.
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.
            split_info (Optional[Dict]): Split information if applicable.

        Returns:
            List[Dict[str, Any]]: List of prepared bulk actions.
        """
        actions = []

        for item in items:
            if split_info:
                item_date = extract_date(item["properties"]["datetime"])
                if item_date > split_info["split_date"]:
                    target_index = split_info["new_index"]
                else:
                    target_index = self._get_target_index_internal(
                        index_selector, collection_id, item, check_size=False
                    )
            else:
                target_index = self._get_target_index_internal(
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


class SyncSimpleIndexInserter(BaseSyncIndexInserter):
    """Simple sync index insertion strategy."""

    def __init__(self, search_adapter: SearchEngineAdapter, client: Any):
        """Initialize the sync simple index inserter.

        Args:
            search_adapter (SearchEngineAdapter): Search engine adapter instance.
            client: Sync search engine client instance.
        """
        self.search_adapter = search_adapter
        self.client = client

    def should_create_collection_index(self) -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: True, as simple strategy creates collection indexes.
        """
        return True

    def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index synchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        return self.search_adapter.create_simple_index_sync(client, collection_id)

    def get_target_index(self, collection_id: str, product: Dict[str, Any]) -> str:
        """Get target index (always the collection alias).

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data (not used in simple strategy).

        Returns:
            str: Collection alias name.
        """
        return index_alias_by_collection_id(collection_id)

    def prepare_bulk_actions(
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
