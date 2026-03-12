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


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def stac_to_db(cls, stac_data: stac_types.Item, base_url: str) -> dict:
        """Transform STAC item to database-ready STAC item."""
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
    def db_to_stac(
        cls,
        item: dict,
        base_url: str,
        request: Request = None,
        extensions: list[str] | None = None,
    ) -> stac_types.Item:
        """Transform database-ready STAC item to STAC item."""
        item_id = item["id"]
        collection_id = item["collection"]
        extensions = extensions or []

        # 1. Base Item Links (standard flat STAC)
        item_links = ItemLinks(
            collection_id=collection_id, item_id=item_id, base_url=base_url
        ).create_links()

        # 2. Contextual Scoping (If accessed via /catalogs/{id}/collections/{id}/items/{id})
        if request and "CatalogsExtension" in extensions:
            path = request.url.path
            path_parts = path.strip("/").split("/")

            if "collections" in path_parts and "catalogs" in path_parts:
                coll_idx = path_parts.index("collections")
                # The catalog ID is immediately before the 'collections' segment
                cat_id = path_parts[coll_idx - 1]

                # Update the 'parent' and 'collection' links to point to the scoped route
                # to prevent the UI from jumping out of the current catalog theme
                scoped_collection_url = (
                    f"{base_url}catalogs/{cat_id}/collections/{collection_id}"
                )

                for link in item_links:
                    if link["rel"] in ["parent", "collection"]:
                        link["href"] = scoped_collection_url

        original_links = item.get("links", [])
        if original_links:
            item_links += resolve_links(original_links, base_url)

        if get_bool_env("STAC_INDEX_ASSETS"):
            assets = {a.pop("es_key"): a for a in item.get("assets", [])}
        else:
            assets = item.get("assets", {})

        stac_item = stac_types.Item(
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

        excluded_fields = os.getenv("EXCLUDED_FROM_ITEMS")
        if excluded_fields:
            for field_path in excluded_fields.split(","):
                if field_path := field_path.strip():
                    get_excluded_from_items(stac_item, field_path)

        return stac_item


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    def stac_to_db(cls, collection: stac_types.Collection, request: Request) -> dict:
        """Transform STAC Collection to database-ready STAC collection."""
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
        cls, collection: dict, request: Request, extensions: list[str] | None = None
    ) -> stac_types.Collection:
        """Transform database model to STAC collection."""
        extensions = extensions or []
        collection = deepcopy(collection)

        parent_ids = collection.get("parent_ids", [])
        collection.pop("bbox_shape", None)
        collection.pop("parent_ids", None)

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

        # 1. Base structural links (self, root, items)
        collection_links = CollectionLinks(
            collection_id=collection_id, request=request, extensions=extensions
        ).create_links()

        # 2. DYNAMIC LINK INJECTION (Beta.4 Compliance)
        base_url = str(request.base_url)
        path = request.url.path
        catalogs_enabled = "CatalogsExtension" in extensions

        if catalogs_enabled:
            context_parent_id = None
            path_parts = path.strip("/").split("/")
            if "collections" in path_parts:
                idx = path_parts.index("collections")
                if idx > 0 and path_parts[idx - 1] != "catalogs":
                    context_parent_id = path_parts[idx - 1]

            unique_parent_ids = list(set(parent_ids)) if parent_ids else []

            if context_parent_id in unique_parent_ids:
                unique_parent_ids.remove(context_parent_id)
                unique_parent_ids.insert(0, context_parent_id)

            if not unique_parent_ids:
                collection_links.append(
                    {
                        "rel": "parent",
                        "type": "application/json",
                        "href": base_url,
                        "title": "Root Catalog",
                    }
                )
            else:
                for idx, pid in enumerate(unique_parent_ids):
                    is_root = pid in ("stac-fastapi", "root")
                    href = base_url if is_root else f"{base_url}catalogs/{pid}"
                    rel_type = "parent" if idx == 0 else "related"

                    collection_links.append(
                        {
                            "rel": rel_type,
                            "type": "application/json",
                            "href": href,
                            "title": "Root Catalog" if is_root else pid,
                        }
                    )

            # 3. Canonical and Duplicate Links
            canonical_href = f"{base_url}collections/{collection_id}"
            collection_links.append(
                {
                    "rel": "canonical",
                    "type": "application/json",
                    "href": canonical_href,
                    "title": "Authoritative Global Endpoint",
                }
            )

            for pid in unique_parent_ids:
                if pid == context_parent_id:
                    continue
                dup_href = (
                    f"{base_url}collections/{collection_id}"
                    if pid in ("stac-fastapi", "root")
                    else f"{base_url}catalogs/{pid}/collections/{collection_id}"
                )
                collection_links.append(
                    {
                        "rel": "duplicate",
                        "type": "application/json",
                        "href": dup_href,
                        "title": f"Duplicate view via {pid}",
                    }
                )

        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)

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

        return stac_types.Collection(**collection)

    @classmethod
    def db_to_stac_in_catalog(
        cls,
        collection: dict,
        request: Request,
        catalog_id: str,
        extensions: list[str] | None = None,
    ) -> stac_types.Collection:
        """Transform database model to STAC collection within a catalog context."""
        extensions = extensions or []
        collection = deepcopy(collection)
        parent_ids = collection.get("parent_ids", [])
        collection.pop("bbox_shape", None)
        collection.pop("parent_ids", None)

        collection_id = collection.get("id")
        collection.setdefault("type", "Collection")
        # ... defaults logic is same as db_to_stac ...

        base_url = str(request.base_url)
        parent_url = f"{base_url}catalogs/{catalog_id}"

        collection_links = CollectionLinks(
            collection_id=collection_id,
            request=request,
            extensions=extensions,
            parent_url=parent_url,
        ).create_links()

        if "CatalogsExtension" in extensions:
            unique_parent_ids = list(set(parent_ids)) if parent_ids else []

            for pid in unique_parent_ids:
                if pid == catalog_id:
                    continue
                is_root = pid in ("stac-fastapi", "root")
                href = base_url if is_root else f"{base_url}catalogs/{pid}"
                collection_links.append(
                    {
                        "rel": "related",
                        "type": "application/json",
                        "href": href,
                        "title": "Root Catalog" if is_root else pid,
                    }
                )

            canonical_href = f"{base_url}collections/{collection_id}"
            collection_links.append(
                {"rel": "canonical", "type": "application/json", "href": canonical_href}
            )
            collection_links.append(
                {"rel": "duplicate", "type": "application/json", "href": canonical_href}
            )

        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, base_url)

        collection["links"] = collection_links
        # ... asset processing logic ...
        return stac_types.Collection(**collection)


class CatalogSerializer(Serializer):
    """Serialization methods for STAC catalogs."""

    @classmethod
    def stac_to_db(cls, catalog: Catalog, request: Request) -> dict:
        """Transform STAC Catalog to database-ready STAC catalog."""
        catalog = deepcopy(catalog)
        catalog.links = resolve_links(catalog.links, str(request.base_url))
        return catalog

    @classmethod
    def db_to_stac(
        cls, catalog: dict, request: Request, extensions: list[str] | None = None
    ) -> stac_types.Catalog:
        """Transform database model to STAC catalog."""
        extensions = extensions or []
        catalog = deepcopy(catalog)
        parent_ids = catalog.get("parent_ids", [])
        catalog.pop("parent_ids", None)

        catalog.setdefault("type", "Catalog")
        catalog.setdefault("stac_version", "1.0.0")

        original_links = catalog.get("links", [])
        catalog_links = (
            resolve_links(original_links, str(request.base_url))
            if original_links
            else []
        )

        base_url = str(request.base_url)
        path = request.url.path
        catalogs_enabled = "CatalogsExtension" in extensions

        if catalogs_enabled:
            context_parent_id = None
            path_parts = path.strip("/").split("/")
            if "catalogs" in path_parts:
                idx = path_parts.index("catalogs")
                if idx > 0 and path_parts[idx - 1] != "catalogs":
                    context_parent_id = path_parts[idx - 1]

            unique_parent_ids = list(set(parent_ids)) if parent_ids else []

            if not unique_parent_ids:
                catalog_links.append(
                    {"rel": "parent", "href": base_url, "title": "Root Catalog"}
                )
            else:
                if context_parent_id in unique_parent_ids:
                    unique_parent_ids.remove(context_parent_id)
                    unique_parent_ids.insert(0, context_parent_id)

                for idx, pid in enumerate(unique_parent_ids):
                    is_root = pid in ("stac-fastapi", "root")
                    href = base_url if is_root else f"{base_url}catalogs/{pid}"
                    rel = "parent" if idx == 0 else "related"
                    catalog_links.append(
                        {
                            "rel": rel,
                            "href": href,
                            "title": "Root Catalog" if is_root else pid,
                        }
                    )

            catalog_id = catalog.get("id")
            catalog_links.append(
                {"rel": "canonical", "href": f"{base_url}catalogs/{catalog_id}"}
            )

        catalog["links"] = catalog_links
        return stac_types.Catalog(**catalog)

    @classmethod
    def db_to_stac_in_catalog(
        cls,
        catalog: dict,
        request: Request,
        parent_id: str,
        extensions: list[str] | None = None,
    ) -> stac_types.Catalog:
        """Transform database model to STAC catalog within a specific parent context."""
        extensions = extensions or []
        catalog = deepcopy(catalog)
        catalog_id = catalog.get("id")
        parent_ids = catalog.get("parent_ids", [])
        catalog.pop("parent_ids", None)

        base_url = str(request.base_url)
        parent_href = (
            base_url
            if parent_id in ("stac-fastapi", "root")
            else f"{base_url}catalogs/{parent_id}"
        )

        catalog_links = [
            {
                "rel": "self",
                "href": f"{base_url}catalogs/{catalog_id}",
                "type": "application/json",
            },
            {"rel": "root", "href": base_url, "type": "application/json"},
            {
                "rel": "parent",
                "href": parent_href,
                "type": "application/json",
                "title": parent_id,
            },
        ]

        if "CatalogsExtension" in extensions:
            unique_parent_ids = list(set(parent_ids)) if parent_ids else []
            for pid in unique_parent_ids:
                if pid == parent_id:
                    continue
                href = (
                    base_url
                    if pid in ("stac-fastapi", "root")
                    else f"{base_url}catalogs/{pid}"
                )
                catalog_links.append({"rel": "related", "href": href, "title": pid})

            catalog_links.append(
                {"rel": "canonical", "href": f"{base_url}catalogs/{catalog_id}"}
            )

        catalog["links"] = catalog_links
        return stac_types.Catalog(**catalog)
