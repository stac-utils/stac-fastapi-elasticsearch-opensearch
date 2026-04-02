"""Serializers."""

import abc
import logging
import os
from copy import deepcopy
from typing import Any

import attr
from starlette.requests import Request

from stac_fastapi.core.datetime_utils import now_to_rfc3339_str
from stac_fastapi.core.models import Catalog
from stac_fastapi.core.models.links import CollectionLinks
from stac_fastapi.core.utilities import get_bool_env, get_excluded_from_items
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import ItemLinks, resolve_links

logger = logging.getLogger(__name__)


@attr.s
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model."""

    @classmethod
    @abc.abstractmethod
    def db_to_stac(cls, item: dict, base_url: str) -> Any:
        """Transform database model to STAC object."""
        ...

    @classmethod
    @abc.abstractmethod
    def stac_to_db(cls, stac_object: Any, base_url: str) -> dict:
        """Transform STAC object to database model."""
        ...

    @classmethod
    def generate_poly_hierarchy_links(
        cls,
        base_url: str,
        resource_id: str,
        resource_type: str,  # "Collection" or "Catalog"
        parent_ids: list[str],
        context_parent_id: str | None = None,
    ) -> list[dict]:
        """Generate HATEOAS links for poly-hierarchical STAC resources.

        This helper method generates parent, related, canonical, and duplicate links
        for resources that can belong to multiple catalogs (poly-hierarchy).

        Args:
            base_url: The base URL of the API.
            resource_id: The ID of the resource (collection or catalog).
            resource_type: Either "Collection" or "Catalog".
            parent_ids: List of parent catalog IDs.
            context_parent_id: The current catalog context (if accessing via scoped endpoint).
                              None means accessing via global endpoint (e.g., /collections/{id}).

        Returns:
            List of link dictionaries following STAC link relation conventions.
        """
        links = []
        unique_pids = list(dict.fromkeys(parent_ids))

        # 1. Handle Contextual Parent (Move to front if present)
        if context_parent_id and context_parent_id in unique_pids:
            unique_pids.remove(context_parent_id)
            unique_pids.insert(0, context_parent_id)

        # 2. Generate Parent & Related Links
        if resource_type == "Collection" and context_parent_id is None:
            # Global collection endpoint: parent → root, catalogs → related
            links.append(
                {
                    "rel": "parent",
                    "href": base_url.rstrip("/"),
                    "type": "application/json",
                    "title": "Root Catalog",
                }
            )
            # All catalog parents become rel="related" links
            for pid in unique_pids:
                is_root = pid in ("stac-fastapi", "root")
                if not is_root:
                    links.append(
                        {
                            "rel": "related",
                            "href": f"{base_url}catalogs/{pid}",
                            "type": "application/json",
                            "title": pid,
                        }
                    )
        elif not unique_pids and resource_type == "Catalog":
            # Root catalog with no parents
            links.append(
                {
                    "rel": "parent",
                    "href": base_url,
                    "type": "application/json",
                    "title": "Root Catalog",
                }
            )
        else:
            # Scoped endpoint or catalog: first parent → parent, rest → related
            for idx, pid in enumerate(unique_pids):
                is_root = pid in ("stac-fastapi", "root")
                href = base_url if is_root else f"{base_url}catalogs/{pid}"
                links.append(
                    {
                        "rel": "parent" if idx == 0 else "related",
                        "href": href,
                        "type": "application/json",
                        "title": "Root Catalog" if is_root else pid,
                    }
                )

        # 3. Generate Canonical Link (Collections only - always points to global endpoint)
        if resource_type == "Collection":
            canonical_href = f"{base_url}collections/{resource_id}"
            links.append(
                {"rel": "canonical", "type": "application/json", "href": canonical_href}
            )

        # 4. Generate Duplicate Links (Collections only - catalogs don't have scoped read endpoints)
        if resource_type == "Collection":
            for pid in unique_pids:
                if pid == context_parent_id:
                    continue  # Skip current context
                is_root = pid in ("stac-fastapi", "root")
                if not is_root:
                    links.append(
                        {
                            "rel": "duplicate",
                            "type": "application/json",
                            "href": f"{base_url}catalogs/{pid}/collections/{resource_id}",
                            "title": f"Collection in catalog: {pid}",
                        }
                    )

        return links


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def stac_to_db(cls, stac_data: stac_types.Item, base_url: str) -> dict:
        """Transform STAC item to database-ready STAC item.

        Args:
            stac_data: The STAC item to be transformed.
            base_url: The base URL of the API.

        Returns:
            A dictionary representation of the item ready for database insertion.
        """
        # Add required STAC v1.0.0 collection link if item has collection field
        item_links = resolve_links(stac_data.get("links", []), base_url)

        # Ensure collection link exists (required by STAC v1.0.0 spec)
        if stac_data.get("collection"):
            collection_id = stac_data["collection"]

            # Check if collection link already exists
            has_collection_link = any(
                link.get("rel") == "collection" for link in item_links
            )

            if not has_collection_link:
                # Add collection link
                collection_link = {
                    "rel": "collection",
                    "href": f"{base_url}collections/{collection_id}",
                    "type": "application/json",
                }
                item_links.append(collection_link)

        stac_data["links"] = item_links

        if get_bool_env("STAC_INDEX_ASSETS"):
            stac_data["assets"] = [
                {"es_key": k, **v} for k, v in stac_data.get("assets", {}).items()
            ]

        now = now_to_rfc3339_str()
        if "properties" not in stac_data:
            stac_data["properties"] = {}
        if "created" not in stac_data["properties"]:
            stac_data["properties"]["created"] = now
        stac_data["properties"]["updated"] = now
        return stac_data

    @classmethod
    def db_to_stac(
        cls,
        item: dict,
        base_url: str,
        request: Request = None,
        extensions: list[str] | None = None,
    ) -> stac_types.Item:
        """Transform database-ready STAC item to STAC item.

        Args:
            item: The database item dictionary.
            base_url: The base URL of the API.
            request: The incoming starlette request for context.
            extensions: List of enabled API extensions.

        Returns:
            A stac_types.Item object.
        """
        item_id = item["id"]
        collection_id = item["collection"]
        extensions = extensions or []

        # 1. Base Item Links
        item_links = ItemLinks(
            collection_id=collection_id, item_id=item_id, base_url=base_url
        ).create_links()

        # 2. Contextual Scoping for Multi-Tenant
        if request and "CatalogsExtension" in extensions:
            # Use path_params for safe extraction (avoids fragile string parsing)
            catalog_id = request.path_params.get("catalog_id")
            if catalog_id:
                scoped_collection_url = (
                    f"{base_url}catalogs/{catalog_id}/collections/{collection_id}"
                )

                for link in item_links:
                    if link["rel"] in ["parent", "collection"]:
                        link["href"] = scoped_collection_url

        original_links = item.get("links", [])
        if original_links:
            item_links += resolve_links(original_links, base_url)

        assets = (
            {
                a.pop("es_key", f"asset_{idx}"): a
                for idx, a in enumerate(item.get("assets", []))
            }
            if get_bool_env("STAC_INDEX_ASSETS")
            else item.get("assets", {})
        )

        stac_item = stac_types.Item(
            type="Feature",
            stac_version=item.get("stac_version", "1.0.0"),
            stac_extensions=item.get("stac_extensions", []),
            id=item_id,
            collection=collection_id,
            geometry=item.get("geometry", {}),
            bbox=item.get("bbox", []),
            properties=item.get("properties", {}),
            links=item_links,
            assets=assets,
        )

        if excluded_fields := os.getenv("EXCLUDED_FROM_ITEMS"):
            for field_path in excluded_fields.split(","):
                if path := field_path.strip():
                    get_excluded_from_items(stac_item, path)

        return stac_item


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @staticmethod
    def _set_collection_defaults(collection: dict) -> None:
        """Set default values for required STAC Collection fields in-place.

        Args:
            collection: The collection dictionary to modify.
        """
        collection.setdefault("type", "Collection")
        if collection.get("stac_extensions") is None:
            collection["stac_extensions"] = []
        collection.setdefault("stac_version", "")
        collection.setdefault("title", "")
        collection.setdefault("description", "")
        if collection.get("keywords") is None:
            collection["keywords"] = []
        collection.setdefault("license", "")
        if collection.get("providers") is None:
            collection["providers"] = []
        if collection.get("summaries") is None:
            collection["summaries"] = {}
        collection.setdefault(
            "extent", {"spatial": {"bbox": []}, "temporal": {"interval": []}}
        )
        if collection.get("assets") is None:
            collection["assets"] = {}

    @staticmethod
    def _deserialize_assets(collection: dict) -> None:
        """Deserialize assets from database format in-place.

        Args:
            collection: The collection dictionary to modify.
        """
        if get_bool_env("STAC_INDEX_ASSETS"):
            collection["assets"] = {
                a.pop("es_key", f"asset_{idx}"): a
                for idx, a in enumerate(collection.get("assets", []))
            }
            collection["item_assets"] = {
                i.pop("es_key", f"item_asset_{idx}"): i
                for idx, i in enumerate(collection.get("item_assets", []))
            }
        else:
            collection["assets"] = collection.get("assets", {})
            if item_assets := collection.get("item_assets"):
                collection["item_assets"] = item_assets

    @classmethod
    def stac_to_db(cls, collection: stac_types.Collection, request: Request) -> dict:
        """Transform STAC Collection to database-ready STAC collection.

        Args:
            collection: The STAC collection object.
            request: The incoming starlette request.

        Returns:
            A dictionary representation of the collection for the database.
        """
        collection = deepcopy(collection)
        collection["links"] = resolve_links(
            collection.get("links", []), str(request.base_url)
        )
        if get_bool_env("STAC_INDEX_ASSETS"):
            for key in ["assets", "item_assets"]:
                if key in collection:
                    collection[key] = [
                        {"es_key": k, **v} for k, v in collection.get(key, {}).items()
                    ]
        return collection

    @classmethod
    def db_to_stac(
        cls, collection: dict, request: Request, extensions: list[str] | None = None
    ) -> stac_types.Collection:
        """Transform database model to STAC collection.

        Args:
            collection: The database collection dictionary.
            request: The incoming starlette request.
            extensions: List of enabled API extensions.

        Returns:
            A stac_types.Collection object with dynamic links.
        """
        extensions = extensions or []
        collection = deepcopy(collection)
        parent_ids = collection.pop("parent_ids", [])
        collection.pop("bbox_shape", None)

        # Set default values for required STAC Collection fields
        cls._set_collection_defaults(collection)

        collection_id = collection.get("id")
        base_url = str(request.base_url)

        collection_links = CollectionLinks(
            collection_id=collection_id, request=request, extensions=extensions
        ).create_links()

        if "CatalogsExtension" in extensions:
            # Remove the default structural parent link (will be replaced by poly-hierarchy links)
            collection_links = [
                link for link in collection_links if link.get("rel") != "parent"
            ]

            # Generate poly-hierarchy links using helper method
            context_parent_id = request.path_params.get("catalog_id")
            dynamic_links = cls.generate_poly_hierarchy_links(
                base_url, collection_id, "Collection", parent_ids, context_parent_id
            )
            collection_links.extend(dynamic_links)

        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)

        collection["links"] = collection_links

        # Deserialize assets from database format
        cls._deserialize_assets(collection)

        return stac_types.Collection(**collection)

    @classmethod
    def db_to_stac_in_catalog(
        cls,
        collection: dict,
        request: Request,
        catalog_id: str,
        extensions: list[str] | None = None,
    ) -> stac_types.Collection:
        """Transform database model to STAC collection within a catalog context.

        Args:
            collection: The database collection dictionary.
            request: The incoming starlette request.
            catalog_id: The ID of the specific catalog context.
            extensions: List of enabled API extensions.

        Returns:
            A stac_types.Collection object scoped to the provided catalog.
        """
        extensions = extensions or []
        collection = deepcopy(collection)
        parent_ids = collection.pop("parent_ids", [])
        collection.pop("bbox_shape", None)

        # Set default values for required STAC Collection fields
        cls._set_collection_defaults(collection)

        collection_id = collection.get("id")
        base_url = str(request.base_url)
        parent_url = f"{base_url}catalogs/{catalog_id}"
        self_url = f"{base_url}catalogs/{catalog_id}/collections/{collection_id}"

        collection_links = CollectionLinks(
            collection_id=collection_id,
            request=request,
            extensions=extensions,
            parent_url=parent_url,
            self_url=self_url,
        ).create_links()

        if "CatalogsExtension" in extensions:
            # Remove the default structural parent link (will be replaced by poly-hierarchy links)
            collection_links = [
                link for link in collection_links if link.get("rel") != "parent"
            ]

            # Generate poly-hierarchy links using helper method (catalog_id is the context)
            dynamic_links = cls.generate_poly_hierarchy_links(
                base_url, collection_id, "Collection", parent_ids, catalog_id
            )
            collection_links.extend(dynamic_links)

        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)

        collection["links"] = collection_links

        # Deserialize assets from database format
        cls._deserialize_assets(collection)

        return stac_types.Collection(**collection)


class CatalogSerializer(Serializer):
    """Serialization methods for STAC catalogs."""

    @classmethod
    def stac_to_db(cls, catalog: Catalog, request: Request) -> dict:
        """Transform STAC Catalog to database-ready STAC catalog.

        Args:
            catalog: The STAC Catalog object.
            request: The incoming starlette request.

        Returns:
            A dictionary representation of the catalog for the database.
        """
        catalog = deepcopy(catalog)
        catalog.links = resolve_links(catalog.links, str(request.base_url))
        return catalog

    @classmethod
    def db_to_stac(
        cls, catalog: dict, request: Request, extensions: list[str] | None = None
    ) -> stac_types.Catalog:
        """Transform database model to STAC catalog.

        Args:
            catalog: The database catalog dictionary.
            request: The incoming starlette request.
            extensions: List of enabled API extensions.

        Returns:
            A stac_types.Catalog object with dynamic links.
        """
        extensions = extensions or []
        catalog = deepcopy(catalog)
        parent_ids = catalog.pop("parent_ids", [])
        base_url = str(request.base_url)

        # Set defaults to prevent Pydantic validation errors on legacy records
        catalog.setdefault("type", "Catalog")
        catalog.setdefault("stac_extensions", [])
        catalog.setdefault("stac_version", "")
        catalog.setdefault("title", "")
        catalog.setdefault("description", "")

        catalog_links = resolve_links(catalog.get("links", []), base_url)

        # Generate poly-hierarchy links using helper method (catalog_id is the context)
        context_parent_id = request.path_params.get("parent_catalog_id")
        dynamic_links = cls.generate_poly_hierarchy_links(
            base_url, catalog.get("id"), "Catalog", parent_ids, context_parent_id
        )
        catalog_links.extend(dynamic_links)

        catalog["links"] = catalog_links
        return stac_types.Catalog(**catalog)
