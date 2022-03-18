"""transactions extension client."""

import logging
from datetime import datetime, timezone
from typing import Optional

import attr
import elasticsearch
from elasticsearch import helpers
from overrides import overrides

from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.core import COLLECTIONS_INDEX, ITEMS_INDEX
from stac_fastapi.elasticsearch.database_logic import mk_item_id
from stac_fastapi.elasticsearch.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseTransactionsClient
from stac_fastapi.types.errors import ConflictError, ForeignKeyError, NotFoundError
from stac_fastapi.types.links import CollectionLinks

logger = logging.getLogger(__name__)


@attr.s
class TransactionsClient(BaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    settings = ElasticsearchSettings()
    client = settings.create_client

    @overrides
    def create_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Create item."""
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient()
            processed_items = [
                bulk_client.preprocess_item(item, base_url) for item in item["features"]
            ]
            return_msg = f"Successfully added {len(processed_items)} items."
            bulk_client.bulk_sync(processed_items)

            return return_msg
        else:
            # todo: check if collection exists, but cache
            if not self.client.exists(index=COLLECTIONS_INDEX, id=item["collection"]):
                raise ForeignKeyError(f"Collection {item['collection']} does not exist")

            if self.client.exists(
                index=ITEMS_INDEX, id=mk_item_id(item["id"], item["collection"])
            ):
                raise ConflictError(
                    f"Item {item['id']} in collection {item['collection']} already exists"
                )

            item = BulkTransactionsClient().preprocess_item(item, base_url)

            es_resp = self.client.index(
                index=ITEMS_INDEX,
                id=mk_item_id(item["id"], item["collection"]),
                document=item,
            )

            if (meta := es_resp.get("meta")) and meta.get("status") == 409:
                raise ConflictError(
                    f"Item {item['id']} in collection {item['collection']} already exists"
                )

            return item

    @overrides
    def update_item(self, item: stac_types.Item, **kwargs) -> stac_types.Item:
        """Update item."""
        base_url = str(kwargs["request"].base_url)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = str(now)

        if not self.client.exists(index=COLLECTIONS_INDEX, id=item["collection"]):
            raise ForeignKeyError(f"Collection {item['collection']} does not exist")

        # todo: index instead of delete and create
        self.delete_item(item["id"], item["collection"])
        self.create_item(item, **kwargs)
        # self.client.update(index=ITEMS_INDEX,id=item["id"], body=item)
        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    def delete_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Delete item."""
        try:
            self.client.delete(index=ITEMS_INDEX, id=mk_item_id(item_id, collection_id))
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )
        return None

    @overrides
    def create_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Create collection."""
        base_url = str(kwargs["request"].base_url)
        collection_links = CollectionLinks(
            collection_id=collection["id"], base_url=base_url
        ).create_links()
        collection["links"] = collection_links

        if self.client.exists(index=COLLECTIONS_INDEX, id=collection["id"]):
            raise ConflictError(f"Collection {collection['id']} already exists")

        self.client.index(
            index=COLLECTIONS_INDEX,
            id=collection["id"],
            document=collection,
        )
        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    def update_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Update collection."""
        base_url = str(kwargs["request"].base_url)
        try:
            _ = self.client.get(index=COLLECTIONS_INDEX, id=collection["id"])
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection['id']} not found")
        self.delete_collection(collection["id"])
        self.create_collection(collection, **kwargs)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    def delete_collection(self, collection_id: str, **kwargs) -> stac_types.Collection:
        """Delete collection."""
        try:
            _ = self.client.get(index=COLLECTIONS_INDEX, id=collection_id)
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")
        self.client.delete(index=COLLECTIONS_INDEX, id=collection_id)
        return None


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """Postgres bulk transactions."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    def __attrs_post_init__(self):
        """Create es engine."""
        settings = ElasticsearchSettings()
        self.client = settings.create_client

    def preprocess_item(self, item: stac_types.Item, base_url) -> stac_types.Item:
        """Preprocess items to match data model."""
        if not self.client.exists(index=COLLECTIONS_INDEX, id=item["collection"]):
            raise ForeignKeyError(f"Collection {item['collection']} does not exist")

        if self.client.exists(index=ITEMS_INDEX, id=item["id"]):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return ItemSerializer.stac_to_db(item, base_url)

    def bulk_sync(self, processed_items):
        """Elasticsearch bulk insertion."""
        actions = [
            {
                "_index": ITEMS_INDEX,
                "_id": mk_item_id(item["id"], item["collection"]),
                "_source": item,
            }
            for item in processed_items
        ]
        helpers.bulk(self.client, actions)

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

        self.bulk_sync(processed_items)

        return f"Successfully added {len(processed_items)} Items."
