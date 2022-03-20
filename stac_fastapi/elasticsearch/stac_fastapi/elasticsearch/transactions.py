"""transactions extension client."""

import logging
from datetime import datetime, timezone
from typing import Optional

import attr
from overrides import overrides

from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
from stac_fastapi.elasticsearch.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import AsyncBaseTransactionsClient
from stac_fastapi.types.links import CollectionLinks

logger = logging.getLogger(__name__)


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    @overrides
    async def create_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Create item."""
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient()
            processed_items = [
                bulk_client.preprocess_item(item, base_url) for item in item["features"]
            ]
            return_msg = f"Successfully added {len(processed_items)} items."
            # todo: wrap as async
            self.database.bulk_sync(processed_items)

            return return_msg
        else:
            item = await self.database.prep_create_item(item=item, base_url=base_url)
            await self.database.create_item(item=item)
            return item

    @overrides
    async def update_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Update item."""
        base_url = str(kwargs["request"].base_url)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = str(now)

        await self.database.check_collection_exists(collection_id=item["collection"])

        # todo: index instead of delete and create
        await self.delete_item(item_id=item["id"], collection_id=item["collection"])
        await self.create_item(item=item, **kwargs)

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Delete item."""
        await self.database.delete_item(item_id=item_id, collection_id=collection_id)
        return None

    @overrides
    async def create_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Create collection."""
        base_url = str(kwargs["request"].base_url)
        collection_links = CollectionLinks(
            collection_id=collection["id"], base_url=base_url
        ).create_links()
        collection["links"] = collection_links
        await self.database.create_collection(collection=collection)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def update_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Update collection."""
        base_url = str(kwargs["request"].base_url)

        await self.database.find_collection(collection_id=collection["id"])
        await self.delete_collection(collection["id"])
        await self.create_collection(collection, **kwargs)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def delete_collection(self, collection_id: str, **kwargs) -> stac_types.Collection:
        """Delete collection."""
        return await self.database.delete_collection(collection_id=collection_id)


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """Elasticsearch bulk transactions."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    def __attrs_post_init__(self):
        """Create es engine."""
        settings = ElasticsearchSettings()
        self.client = settings.create_client

    def preprocess_item(self, item: stac_types.Item, base_url) -> stac_types.Item:
        """Preprocess items to match data model."""
        item = self.database.prep_create_item(item=item, base_url=base_url)
        return item

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Bulk item insertion using es."""
        request = kwargs.get("request")
        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = [
            self.preprocess_item(item, base_url) for item in items.items.values()
        ]

        self.database.bulk_sync(processed_items)

        return f"Successfully added {len(processed_items)} Items."
