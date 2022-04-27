"""Serializers."""
import abc
from typing import Any

import attr

from stac_fastapi.elasticsearch.datetime_utils import now_to_rfc3339_str
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import CollectionLinks, ItemLinks, resolve_links


@attr.s  # type:ignore
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model."""

    @classmethod
    @abc.abstractmethod
    def db_to_stac(cls, item: dict, base_url: str) -> Any:
        """Transform database model to stac."""
        ...


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def stac_to_db(cls, stac_data: stac_types.Item, base_url: str) -> stac_types.Item:
        """Transform STAC Item to database-ready STAC Item."""
        item_links = ItemLinks(
            collection_id=stac_data["collection"],
            item_id=stac_data["id"],
            base_url=base_url,
        ).create_links()
        stac_data["links"] = item_links

        # elasticsearch doesn't like the fact that some values are float and some were int
        if "eo:bands" in stac_data["properties"]:
            for wave in stac_data["properties"]["eo:bands"]:
                for k, v in wave.items():
                    if type(v) != str:
                        v = float(v)
                        wave.update({k: v})

        now = now_to_rfc3339_str()
        if "created" not in stac_data["properties"]:
            stac_data["properties"]["created"] = now
        stac_data["properties"]["updated"] = now
        return stac_data

    @classmethod
    def db_to_stac(cls, item: dict, base_url: str) -> stac_types.Item:
        """Transform database model to stac item."""
        item_id = item["id"]
        collection_id = item["collection"]
        item_links = ItemLinks(
            collection_id=collection_id, item_id=item_id, base_url=base_url
        ).create_links()

        original_links = item["links"]
        if original_links:
            item_links += resolve_links(original_links, base_url)

        return stac_types.Item(
            type="Feature",
            stac_version=item["stac_version"] if "stac_version" in item else "",
            stac_extensions=item["stac_extensions"]
            if "stac_extensions" in item
            else [],
            id=item_id,
            collection=item["collection"] if "collection" in item else "",
            geometry=item["geometry"] if "geometry" in item else {},
            bbox=item["bbox"] if "bbox" in item else [],
            properties=item["properties"] if "properties" in item else {},
            links=item_links if "links" in item else [],
            assets=item["assets"] if "assets" in item else {},
        )


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    def db_to_stac(cls, collection: dict, base_url: str) -> stac_types.Collection:
        """Transform database model to stac collection."""
        collection_links = CollectionLinks(
            collection_id=collection["id"], base_url=base_url
        ).create_links()

        original_links = collection["links"]
        if original_links:
            collection_links += resolve_links(original_links, base_url)

        return stac_types.Collection(
            type="Collection",
            id=collection["id"],
            stac_extensions=collection["stac_extensions"]
            if "stac_extensions" in collection
            else [],
            stac_version=collection["stac_version"]
            if "stac_version" in collection
            else "",
            title=collection["title"] if "title" in collection else "",
            description=collection["description"]
            if "description" in collection
            else "",
            keywords=collection["keywords"] if "keywords" in collection else [],
            license=collection["license"] if "license" in collection else "",
            providers=collection["providers"] if "providers" in collection else {},
            summaries=collection["summaries"] if "summaries" in collection else {},
            extent=collection["extent"] if "extent" in collection else {},
            links=collection_links,
        )
