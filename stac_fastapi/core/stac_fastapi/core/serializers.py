"""Serializers."""

import abc
from copy import deepcopy
from typing import Any, List, Optional

import attr
from starlette.requests import Request

from stac_fastapi.core.datetime_utils import now_to_rfc3339_str
from stac_fastapi.core.models.links import CollectionLinks
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import ItemLinks, resolve_links


@attr.s
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model.

    This class is meant to be subclassed and implemented by specific serializers for different STAC objects (e.g. Item, Collection).
    """

    @classmethod
    @abc.abstractmethod
    def db_to_stac(cls, item: dict, base_url: str) -> Any:
        """Transform database model to STAC object.

        Arguments:
            item (dict): A dictionary representing the database model.
            base_url (str): The base URL of the STAC API.

        Returns:
            Any: A STAC object, e.g. an `Item` or `Collection`, representing the input `item`.
        """
        ...

    @classmethod
    @abc.abstractmethod
    def stac_to_db(cls, stac_object: Any, base_url: str) -> dict:
        """Transform STAC object to database model.

        Arguments:
            stac_object (Any): A STAC object, e.g. an `Item` or `Collection`.
            base_url (str): The base URL of the STAC API.

        Returns:
            dict: A dictionary representing the database model.
        """
        ...


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def stac_to_db(cls, stac_data: stac_types.Item, base_url: str) -> stac_types.Item:
        """Transform STAC item to database-ready STAC item.

        Args:
            stac_data (stac_types.Item): The STAC item object to be transformed.
            base_url (str): The base URL for the STAC API.

        Returns:
            stac_types.Item: The database-ready STAC item object.
        """
        item_links = resolve_links(stac_data.get("links", []), base_url)
        stac_data["links"] = item_links

        if get_bool_env("STAC_INDEX_ASSETS"):
            stac_data["assets"] = [
                {"es_key": k, **v} for k, v in stac_data.get("assets", {}).items()
            ]

        now = now_to_rfc3339_str()
        if "created" not in stac_data["properties"]:
            stac_data["properties"]["created"] = now
        stac_data["properties"]["updated"] = now
        return stac_data

    @classmethod
    def db_to_stac(cls, item: dict, base_url: str) -> stac_types.Item:
        """Transform database-ready STAC item to STAC item.

        Args:
            item (dict): The database-ready STAC item to be transformed.
            base_url (str): The base URL for the STAC API.

        Returns:
            stac_types.Item: The STAC item object.
        """
        item_id = item["id"]
        collection_id = item["collection"]
        item_links = ItemLinks(
            collection_id=collection_id, item_id=item_id, base_url=base_url
        ).create_links()

        original_links = item.get("links", [])
        if original_links:
            item_links += resolve_links(original_links, base_url)

        if get_bool_env("STAC_INDEX_ASSETS"):
            assets = {a.pop("es_key"): a for a in item.get("assets", [])}

        else:
            assets = item.get("assets", {})

        return stac_types.Item(
            type="Feature",
            stac_version=item.get("stac_version", ""),
            stac_extensions=item.get("stac_extensions", []),
            id=item_id,
            collection=item.get("collection", ""),
            geometry=item.get("geometry", {}),
            bbox=item.get("bbox", []),
            properties=item.get("properties", {}),
            links=item_links,
            assets=assets,
        )


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    def stac_to_db(
        cls, collection: stac_types.Collection, request: Request
    ) -> stac_types.Collection:
        """
        Transform STAC Collection to database-ready STAC collection.

        Args:
            stac_data: the STAC Collection object to be transformed
            starlette.requests.Request: the API request

        Returns:
            stac_types.Collection: The database-ready STAC Collection object.
        """
        collection = deepcopy(collection)
        collection["links"] = resolve_links(
            collection.get("links", []), str(request.base_url)
        )

        if get_bool_env("STAC_INDEX_ASSETS"):
            collection["assets"] = [
                {"es_key": k, **v} for k, v in collection.get("assets", {}).items()
            ]
            collection["item_assets"] = [
                {"es_key": k, **v} for k, v in collection.get("item_assets", {}).items()
            ]

        return collection

    @classmethod
    def db_to_stac(
        cls, collection: dict, request: Request, extensions: Optional[List[str]] = []
    ) -> stac_types.Collection:
        """Transform database model to STAC collection.

        Args:
            collection (dict): The collection data in dictionary form, extracted from the database.
            starlette.requests.Request: the API request
            extensions: A list of the extension class names (`ext.__name__`) or all enabled STAC API extensions.

        Returns:
            stac_types.Collection: The STAC collection object.
        """
        # Avoid modifying the input dict in-place ... doing so breaks some tests
        collection = deepcopy(collection)

        # Set defaults
        collection_id = collection.get("id")
        collection.setdefault("type", "Collection")
        collection.setdefault("stac_extensions", [])
        collection.setdefault("stac_version", "")
        collection.setdefault("title", "")
        collection.setdefault("description", "")
        collection.setdefault("keywords", [])
        collection.setdefault("license", "")
        collection.setdefault("providers", [])
        collection.setdefault("summaries", {})
        collection.setdefault(
            "extent", {"spatial": {"bbox": []}, "temporal": {"interval": []}}
        )
        collection.setdefault("assets", {})

        # Create the collection links using CollectionLinks
        collection_links = CollectionLinks(
            collection_id=collection_id, request=request, extensions=extensions
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, str(request.base_url))
        collection["links"] = collection_links

        if get_bool_env("STAC_INDEX_ASSETS"):
            collection["assets"] = {
                a.pop("es_key"): a for a in collection.get("assets", [])
            }
            collection["item_assets"] = {
                i.pop("es_key"): i for i in collection.get("item_assets", [])
            }

        else:
            collection["assets"] = collection.get("assets", {})
            if item_assets := collection.get("item_assets"):
                collection["item_assets"] = item_assets

        # Return the stac_types.Collection object
        return stac_types.Collection(**collection)
