"""Serializers."""

import abc
from copy import deepcopy
from typing import Any

import attr

from stac_fastapi.core.datetime_utils import now_to_rfc3339_str
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import (
    CatalogLinks,
    CollectionLinks,
    ItemLinks,
    resolve_links,
)


@attr.s
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model.

    This class is meant to be subclassed and implemented by specific serializers for different STAC objects (e.g. Item, Collection).
    """

    @classmethod
    @abc.abstractmethod
    def db_to_stac(cls, item: dict, base_url: str, catalog_id: str = None) -> Any:
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

        now = now_to_rfc3339_str()
        if "created" not in stac_data["properties"]:
            stac_data["properties"]["created"] = now
        stac_data["properties"]["updated"] = now
        return stac_data

    @classmethod
    def db_to_stac(
        cls, item: dict, base_url: str, catalog_id: str = None
    ) -> stac_types.Item:
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
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id=item_id,
            base_url=base_url,
        ).create_links()

        original_links = item.get("links", [])
        if original_links:
            item_links += resolve_links(original_links, base_url)

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
            assets=item.get("assets", {}),
        )


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    def stac_to_db(
        cls, collection: stac_types.Collection, base_url: str
    ) -> stac_types.Collection:
        """
        Transform STAC Collection to database-ready STAC collection.

        Args:
            stac_data: the STAC Collection object to be transformed
            base_url: the base URL for the STAC API

        Returns:
            stac_types.Collection: The database-ready STAC Collection object.
        """

        collection = deepcopy(collection)
        collection["links"] = resolve_links(collection.get("links", []), base_url)

        return collection

    @classmethod
    def db_to_stac(
        cls, collection: dict, base_url: str, catalog_id: str = None
    ) -> stac_types.Collection:
        """Transform database model to STAC collection.

        Args:
            collection (dict): The collection data in dictionary form, extracted from the database.
            base_url (str): The base URL for the collection.

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
            catalog_id=catalog_id, collection_id=collection_id, base_url=base_url
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)
        collection["links"] = collection_links

        # Return the stac_types.Collection object
        return stac_types.Collection(**collection)


class CatalogSerializer(Serializer):
    """Serialization methods for STAC catalogs."""

    @classmethod
    def stac_to_db(
        cls, catalog: stac_types.Catalog, base_url: str
    ) -> stac_types.Catalog:
        """
        Transform STAC Catalog to database-ready STAC catalog.

        Args:
            stac_data: the STAC Catalog object to be transformed
            base_url: the base URL for the STAC API

        Returns:
            stac_types.Catalog: The database-ready STAC Catalog object.
        """
        catalog = deepcopy(catalog)
        catalog["links"] = resolve_links(catalog.get("links", []), base_url)
        return catalog

    @classmethod
    def db_to_stac(cls, catalog: dict, base_url: str) -> stac_types.Catalog:
        """Transform database model to STAC catalog.

        Args:
            catalog (dict): The catalog data in dictionary form, extracted from the database.
            base_url (str): The base URL for the catalog.

        Returns:
            stac_types.Catalog: The STAC catalog object.
        """
        # Avoid modifying the input dict in-place ... doing so breaks some tests
        catalog = deepcopy(catalog)

        # Set defaults
        catalog_id = catalog.get("id")
        catalog.setdefault("type", "Collection")
        catalog.setdefault("stac_extensions", [])
        catalog.setdefault("stac_version", "")
        catalog.setdefault("title", "")
        catalog.setdefault("description", "")

        # Create the collection links using CatalogLinks
        catalog_links = CatalogLinks(
            catalog_id=catalog_id, base_url=base_url
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = catalog.get("links")
        if original_links:
            catalog_links += resolve_links(original_links, base_url)
        catalog["links"] = catalog_links

        # Return the stac_types.Collection object
        return stac_types.Catalog(**catalog)


class CatalogCollectionSerializer(Serializer):
    """Serialization methods for STAC catalogs."""

    def stac_to_db(cls, stac_object: Any, base_url: str) -> dict:
        """Transform STAC object to database model.

        NOT IMPLEMENTED AS COLLECTIONS AND CATALOGS WILL BE CREATED INDEPENDENTLY.
        See above implementation classes and functions.

        Arguments:
            stac_object (Any): A STAC object, e.g. an `Item` or `Collection`.
            base_url (str): The base URL of the STAC API.

        Returns:
            dict: A dictionary representing the database model.
        """
        raise NotImplementedError

    @classmethod
    def catalog_db_to_stac(cls, catalog: dict, base_url: str) -> stac_types.Catalog:
        """Transform database model to STAC catalog.

        Args:
            catalog (dict): The catalog data in dictionary form, extracted from the database.
            base_url (str): The base URL for the catalog.

        Returns:
            stac_types.Catalog: The STAC catalog object.
        """
        # Avoid modifying the input dict in-place ... doing so breaks some tests
        catalog = deepcopy(catalog)

        # Set defaults
        catalog_id = catalog.get("id")
        catalog.setdefault("type", "Collection")
        catalog.setdefault("stac_extensions", [])
        catalog.setdefault("stac_version", "")
        catalog.setdefault("title", "")
        catalog.setdefault("description", "")

        # Create the collection links using CatalogLinks
        catalog_links = CatalogLinks(
            catalog_id=catalog_id, base_url=base_url
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = catalog.get("links")
        if original_links:
            catalog_links += resolve_links(original_links, base_url)
        catalog["links"] = catalog_links

        # Return the stac_types.Collection object
        return stac_types.Catalog(**catalog)

    @classmethod
    def collection_db_to_stac(
        cls, collection: dict, base_url: str, catalog_id: str = None
    ) -> stac_types.Collection:
        """Transform database model to STAC collection.

        Args:
            collection (dict): The collection data in dictionary form, extracted from the database.
            base_url (str): The base URL for the collection.

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
            catalog_id=catalog_id, collection_id=collection_id, base_url=base_url
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)
        collection["links"] = collection_links

        # Return the stac_types.Collection object
        return stac_types.Collection(**collection)

    @classmethod
    def db_to_stac(
        cls, data: dict, base_url: str, catalog_id: str = None
    ) -> stac_types.Collection:
        """Transform database model to STAC catalog or collection.

        Args:
            collection (dict): The collection data in dictionary form, extracted from the database.
            base_url (str): The base URL for the collection.

        Returns:
            stac_types.Collection: The STAC collection object.
        """
        # Determine datatype to serialise and pass to correct serializer function
        if data["type"] == "Collection":
            return cls.collection_db_to_stac(
                catalog_id=catalog_id, collection=data, base_url=base_url
            )
        elif data["type"] == "Catalog":
            return cls.catalog_db_to_stac(data, base_url)
