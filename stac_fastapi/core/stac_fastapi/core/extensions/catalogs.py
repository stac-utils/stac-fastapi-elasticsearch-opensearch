"""Catalogs extension."""
import asyncio
import base64
import json
import logging
from typing import Any, Dict, List, Optional, Type
from urllib.parse import urlencode

import attr
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from stac_pydantic import Catalog
from starlette.responses import Response
from typing_extensions import TypedDict

from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.extension import ApiExtension

logger = logging.getLogger(__name__)


class CollectionId(BaseModel):
    """Model for linking an existing collection by ID."""

    id: str


class Catalogs(TypedDict, total=False):
    """Catalogs endpoint response."""

    catalogs: List[Catalog]
    links: List[Dict[str, Any]]
    numberMatched: Optional[int]
    numberReturned: Optional[int]


def _decode_token(token: str | None) -> list | None:
    """Decode a Base64/JSON pagination token into a search_after list."""
    if not token:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(token.encode()).decode())
    except Exception:
        logger.debug(f"Invalid pagination token provided: {token}")
        return None


def _encode_token(search_after: list | None) -> str | None:
    """Encode a search_after list into a Base64/JSON pagination token."""
    if not search_after:
        return None
    return base64.urlsafe_b64encode(json.dumps(search_after).encode()).decode()


def _active_extensions(client: BaseCoreClient) -> list[str]:
    """Get list of active extensions, ensuring CatalogsExtension is included."""
    if hasattr(client.database, "extensions"):
        exts = [type(ext).__name__ for ext in client.database.extensions]
    else:
        exts = []

    if "CatalogsExtension" not in exts:
        exts.append("CatalogsExtension")
    return exts


def _create_child_link(base_url: str, parent_id: str, child: dict) -> dict:
    """Generate a STAC rel='child' link based on the child's resource type."""
    child_id = child.get("id")
    child_type = child.get("type", "Collection")
    child_title = child.get("title", child_id)

    if child_type == "Catalog":
        href = f"{base_url}/catalogs/{child_id}"
    else:
        href = f"{base_url}/catalogs/{parent_id}/collections/{child_id}"

    return {
        "rel": "child",
        "type": "application/json",
        "href": href,
        "title": child_title,
    }


@attr.s
class CatalogsExtension(ApiExtension):
    """Catalogs extension.

    This extension adds support for hierarchical catalogs.
    """

    client: BaseCoreClient = attr.ib(default=None)
    settings: dict = attr.ib(factory=dict)
    conformance_classes: list[str] = attr.ib(
        factory=lambda: [
            "https://api.stacspec.org/v1.0.0-rc.2/ogcapi-features/extensions/catalogs"
        ]
    )
    router: APIRouter = attr.ib(factory=APIRouter)
    response_class: Type[Response] = attr.ib(default=JSONResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application."""
        self.router.add_api_route(
            "/catalogs",
            self.catalogs,
            methods=["GET"],
            response_model=Catalogs,
            response_class=self.response_class,
            summary="List catalogs",
        )
        self.router.add_api_route(
            "/catalogs",
            self.create_catalog,
            methods=["POST"],
            response_model=Catalog,
            response_class=self.response_class,
            status_code=201,
            summary="Create catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog,
            methods=["GET"],
            response_model=Catalog,
            response_class=self.response_class,
            summary="Get catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.update_catalog,
            methods=["PUT"],
            response_model=Catalog,
            response_class=self.response_class,
            summary="Update catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.delete_catalog,
            methods=["DELETE"],
            response_class=self.response_class,
            status_code=204,
            summary="Delete catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/catalogs",
            self.get_catalog_catalogs,
            methods=["GET"],
            response_model=Catalogs,
            response_class=self.response_class,
            summary="List sub-catalogs",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/catalogs",
            self.create_catalog_catalog,
            methods=["POST"],
            response_model=Catalog,
            response_class=self.response_class,
            status_code=201,
            summary="Create sub-catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.get_catalog_collections,
            methods=["GET"],
            response_model=stac_types.Collections,
            response_class=self.response_class,
            summary="List collections in catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.create_catalog_collection,
            methods=["POST"],
            response_model=stac_types.Collection,
            response_class=self.response_class,
            status_code=201,
            summary="Create collection in catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_catalog_collection,
            methods=["GET"],
            response_model=stac_types.Collection,
            response_class=self.response_class,
            summary="Get collection from catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.delete_catalog_collection,
            methods=["DELETE"],
            response_class=self.response_class,
            status_code=204,
            summary="Delete Catalog Collection",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.get_catalog_collection_items,
            methods=["GET"],
            response_model=stac_types.ItemCollection,
            response_class=self.response_class,
            summary="Get items from collection in catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            self.get_catalog_collection_item,
            methods=["GET"],
            response_model=stac_types.Item,
            response_class=self.response_class,
            summary="Get item from collection in catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/children",
            self.get_catalog_children,
            methods=["GET"],
            response_class=self.response_class,
            summary="Get all children (catalogs and collections) of a catalog",
        )

        app.include_router(self.router, tags=["Catalogs"])

    async def _format_catalogs_with_links(
        self,
        catalogs_data: list[dict],
        request: Request,
        base_url: str,
    ) -> list[Catalog]:
        """Format catalog data with dynamic parent and child links."""
        unique_parent_ids = list(
            set(pid for c in catalogs_data for pid in c.get("parent_ids", []) if pid)
        )
        parent_id_to_title = {}

        if unique_parent_ids:

            async def fetch_title(pid):
                try:
                    parent_catalog = await self.client.database.find_catalog(pid)
                    return pid, parent_catalog.get("title", pid)
                except Exception as e:
                    logger.debug(
                        f"Could not fetch title for parent catalog {pid}, using ID as fallback: {e}"
                    )
                    return pid, pid

            title_results = await asyncio.gather(
                *(fetch_title(pid) for pid in unique_parent_ids)
            )
            parent_id_to_title = dict(title_results)

        async def fetch_children(catalog_id):
            try:
                children_data, _, _ = await self.client.database.get_catalog_children(
                    catalog_id=catalog_id,
                    limit=100,
                    token=None,
                    request=request,
                    resource_type=None,
                )
                return catalog_id, children_data
            except Exception as e:
                logger.warning(
                    f"Could not fetch children for catalog {catalog_id}: {e}"
                )
                return catalog_id, []

        children_results = await asyncio.gather(
            *(fetch_children(c.get("id")) for c in catalogs_data)
        )
        catalog_children_map = dict(children_results)

        catalog_stac_objects = []

        for catalog_data in catalogs_data:
            catalog_id = catalog_data.get("id")

            catalog_stac = self.client.catalog_serializer.db_to_stac(
                catalog_data,
                request,
                extensions=_active_extensions(self.client),
            )

            catalog_dict = (
                catalog_stac.model_dump()
                if hasattr(catalog_stac, "model_dump")
                else dict(catalog_stac)
            )
            catalog_dict.setdefault("links", [])

            for link in catalog_dict.get("links", []):
                if link.get("rel") in ["parent", "related"] and "title" not in link:
                    if (
                        link.get("href") == base_url
                        or link.get("href") == f"{base_url}/"
                    ):
                        link["title"] = "Root Catalog"
                        continue

                    for pid in catalog_data.get("parent_ids", []):
                        if (
                            pid in parent_id_to_title
                            and f"/catalogs/{pid}" in link.get("href", "")
                        ):
                            link["title"] = parent_id_to_title[pid]
                            break

            children_data = catalog_children_map.get(catalog_id, [])
            for child in children_data:
                if child.get("id"):
                    catalog_dict["links"].append(
                        _create_child_link(base_url, catalog_id, child)
                    )

            catalog_stac = Catalog(**catalog_dict)
            catalog_stac_objects.append(catalog_stac)

        return catalog_stac_objects

    async def catalogs(
        self,
        request: Request,
        limit: int = 10,
        token: str | None = None,
    ):
        """Get all catalogs."""
        base_url = str(request.base_url).rstrip("/")
        (
            catalogs_list,
            next_token,
            total_hits,
        ) = await self.client.database.get_all_catalogs(
            limit=limit,
            token=token,
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        catalog_stac_objects = await self._format_catalogs_with_links(
            catalogs_list, request, base_url
        )

        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": base_url,
            },
            {"rel": "self", "type": "application/json", "href": str(request.url)},
        ]

        if next_token:
            if isinstance(next_token, str):
                next_token_list = next_token.split("|")
                encoded_token = _encode_token(next_token_list)
            else:
                encoded_token = str(next_token)

            links.append(
                {
                    "rel": "next",
                    "href": f"{base_url}/catalogs?{urlencode({'limit': limit, 'token': encoded_token})}",
                    "type": "application/json",
                    "title": "Next page",
                }
            )

        return Catalogs(
            catalogs=catalog_stac_objects,
            links=links,
            numberReturned=len(catalog_stac_objects),
            numberMatched=total_hits,
        )

    async def create_catalog(self, request: Request, catalog: Catalog):
        """Create a catalog."""
        db_catalog = self.client.catalog_serializer.stac_to_db(catalog, request)
        db_catalog_dict = db_catalog.model_dump()
        db_catalog_dict["type"] = "Catalog"

        if "links" in db_catalog_dict:
            db_catalog_dict["links"] = [
                link
                for link in db_catalog_dict["links"]
                if link.get("rel") not in ("parent", "child", "children")
            ]

        if "parent_ids" not in db_catalog_dict:
            db_catalog_dict["parent_ids"] = []

        await self.client.database.create_catalog(catalog=db_catalog_dict, refresh=True)

        return self.client.catalog_serializer.db_to_stac(
            db_catalog_dict,
            request,
            extensions=_active_extensions(self.client),
        )

    async def get_catalog(self, request: Request, catalog_id: str):
        """Get a catalog."""
        db_catalog = await self.client.database.find_catalog(catalog_id)

        catalog = self.client.catalog_serializer.db_to_stac(
            db_catalog, request, extensions=_active_extensions(self.client)
        )

        base_url = str(request.base_url).rstrip("/")
        catalog_dict = (
            catalog.model_dump() if hasattr(catalog, "model_dump") else dict(catalog)
        )

        catalog_dict["links"] = [
            link
            for link in catalog_dict.get("links", [])
            if link.get("rel") not in ["self", "parent", "root", "child", "related"]
        ]

        catalog_dict["links"].extend(
            [
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}",
                },
                {"rel": "root", "type": "application/json", "href": base_url},
            ]
        )

        root_id = self.settings.get("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi")
        parent_ids = db_catalog.get("parent_ids", [])

        if not parent_ids:
            catalog_dict["links"].append(
                {"rel": "parent", "type": "application/json", "href": base_url}
            )
        else:
            primary_pid = parent_ids[0]
            parent_href = (
                base_url
                if primary_pid == root_id
                else f"{base_url}/catalogs/{primary_pid}"
            )
            catalog_dict["links"].append(
                {"rel": "parent", "type": "application/json", "href": parent_href}
            )

            for pid in parent_ids[1:]:
                related_href = (
                    base_url if pid == root_id else f"{base_url}/catalogs/{pid}"
                )
                catalog_dict["links"].append(
                    {
                        "rel": "related",
                        "type": "application/json",
                        "href": related_href,
                        "title": f"Parent context: {pid}",
                    }
                )

        try:
            children_data, _, _ = await self.client.database.get_catalog_children(
                catalog_id=catalog_id, limit=100, token=None, request=request
            )

            for child in children_data:
                if child.get("id"):
                    catalog_dict["links"].append(
                        _create_child_link(base_url, catalog_id, child)
                    )
        except Exception as e:
            logger.warning(f"Child link generation failed for {catalog_id}: {e}")

        catalog_dict["links"].append(
            {
                "rel": "children",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/children",
                "title": "Child catalogs and collections",
            }
        )

        return Catalog(**catalog_dict)

    async def update_catalog(self, request: Request, catalog_id: str, catalog: Catalog):
        """Update a catalog."""
        existing_catalog_db = await self.client.database.find_catalog(catalog_id)

        db_catalog = self.client.catalog_serializer.stac_to_db(catalog, request)
        db_catalog_dict = db_catalog.model_dump()
        db_catalog_dict["type"] = "Catalog"
        db_catalog_dict["id"] = catalog_id

        if "parent_ids" in existing_catalog_db:
            db_catalog_dict["parent_ids"] = existing_catalog_db["parent_ids"]

        await self.client.database.create_catalog(catalog=db_catalog_dict, refresh=True)

        updated_db_catalog = await self.client.database.find_catalog(catalog_id)
        return self.client.catalog_serializer.db_to_stac(
            updated_db_catalog,
            request,
            extensions=_active_extensions(self.client),
        )

    async def delete_catalog(self, request: Request, catalog_id: str):
        """Delete a catalog."""
        return await self.client.database.delete_catalog(
            catalog_id=catalog_id, refresh=True
        )

    async def get_catalog_catalogs(
        self,
        request: Request,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
    ):
        """Get sub-catalogs."""
        await self.client.database.find_catalog(catalog_id)

        catalogs_data, _, next_token = await self.client.database.get_catalog_catalogs(
            catalog_id=catalog_id, limit=limit, token=token, request=request
        )

        base_url = str(request.base_url).rstrip("/")
        catalogs_list = await self._format_catalogs_with_links(
            catalogs_data, request, base_url
        )

        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
            {"rel": "self", "type": "application/json", "href": str(request.url)},
        ]

        if next_token:
            if isinstance(next_token, str):
                encoded_token = _encode_token(next_token.split("|"))
            else:
                encoded_token = str(next_token)

            links.append(
                {
                    "rel": "next",
                    "href": f"{base_url}/catalogs/{catalog_id}/catalogs?{urlencode({'limit': limit, 'token': encoded_token})}",
                    "type": "application/json",
                }
            )

        return Catalogs(
            catalogs=catalogs_list,
            links=links,
            numberReturned=len(catalogs_list),
        )

    async def create_catalog_catalog(
        self, request: Request, catalog_id: str, catalog: Catalog
    ):
        """Create sub-catalog."""
        return await self.client.database.create_catalog_catalog(
            catalog_id=catalog_id, catalog=catalog, request=request
        )

    async def get_catalog_collections(
        self,
        request: Request,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
    ):
        """Get collections in catalog."""
        await self.client.database.find_catalog(catalog_id)

        (
            collections_data,
            total_hits,
            next_token,
        ) = await self.client.database.get_catalog_collections(
            catalog_id=catalog_id, limit=limit, token=token, request=request
        )

        collections = []
        for collection_db in collections_data:
            try:
                collection = self.client.collection_serializer.db_to_stac_in_catalog(
                    collection_db,
                    request,
                    catalog_id=catalog_id,
                    extensions=_active_extensions(self.client),
                )
                collections.append(collection)
            except Exception as e:
                logger.error(f"Error serializing collection: {e}")
                continue

        base_url = str(request.base_url).rstrip("/")
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": str(request.url),
            },
        ]

        if next_token:
            if isinstance(next_token, str):
                encoded_token = _encode_token(next_token.split("|"))
            else:
                encoded_token = str(next_token)

            links.append(
                {
                    "rel": "next",
                    "href": f"{base_url}/catalogs/{catalog_id}/collections?{urlencode({'limit': limit, 'token': encoded_token})}",
                    "type": "application/json",
                    "title": "Next page",
                }
            )

        return stac_types.Collections(
            collections=collections,
            links=links,
            numberMatched=total_hits,
            numberReturned=len(collections),
        )

    async def create_catalog_collection(
        self,
        request: Request,
        catalog_id: str,
        body: CollectionId | stac_types.Collection,
    ):
        """Create collection in catalog."""
        return await self.client.database.create_catalog_collection(
            catalog_id=catalog_id, collection=body, request=request
        )

    async def get_catalog_collection(
        self, request: Request, catalog_id: str, collection_id: str
    ):
        """Get collection in catalog."""
        return await self.client.database.get_catalog_collection(
            catalog_id=catalog_id, collection_id=collection_id, request=request
        )

    async def delete_catalog_collection(
        self, request: Request, catalog_id: str, collection_id: str
    ):
        """Delete collection from catalog (unlink)."""
        pass

    async def get_catalog_collection_items(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        limit: int = 10,
        token: str | None = None,
        bbox: str | None = None,
        datetime: str | None = None,
        sortby: str | None = None,
        filter: str | None = None,
        filter_lang: str | None = None,
        query: str | None = None,
        fields: list[str] | None = None,
    ):
        """Get items from collection in catalog."""
        bbox_list = None
        if bbox:
            bbox_list = [float(x) for x in bbox.split(",")]

        return await self.client.database.get_catalog_collection_items(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
            bbox=bbox_list,
            datetime=datetime,
            limit=limit,
            sortby=sortby,
            filter_expr=filter,
            filter_lang=filter_lang,
            token=token,
            query=query,
            fields=fields,
        )

    async def get_catalog_collection_item(
        self, request: Request, catalog_id: str, collection_id: str, item_id: str
    ):
        """Get item from collection in catalog."""
        return await self.client.database.get_catalog_collection_item(
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id=item_id,
            request=request,
        )

    async def get_catalog_children(
        self,
        request: Request,
        catalog_id: str,
        limit: int = 10,
        token: str | None = None,
        resource_type: str | None = None,
    ):
        """Get all children of a catalog."""
        await self.client.database.find_catalog(catalog_id)

        (
            children_data,
            total,
            next_token,
        ) = await self.client.database.get_catalog_children(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
            resource_type=resource_type,
        )

        base_url = str(request.base_url).rstrip("/")

        catalog_docs = [doc for doc in children_data if doc.get("type") == "Catalog"]
        formatted_catalogs_list = []
        if catalog_docs:
            formatted_catalogs_list = await self._format_catalogs_with_links(
                catalog_docs, request, base_url
            )

        catalog_lookup = {c.id: c for c in formatted_catalogs_list}

        formatted_children = []
        for doc in children_data:
            doc_id = doc.get("id")
            if doc.get("type") == "Catalog":
                formatted_catalog = catalog_lookup.get(doc_id)
                formatted_children.append(
                    formatted_catalog if formatted_catalog else doc
                )
            else:
                formatted_children.append(
                    self.client.collection_serializer.db_to_stac(doc, request)
                )

        links = [
            {"rel": "self", "type": "application/json", "href": str(request.url)},
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
        ]

        if next_token:
            if isinstance(next_token, str):
                encoded_token = _encode_token(next_token.split("|"))
            else:
                encoded_token = str(next_token)

            params = {"limit": limit, "token": encoded_token}
            if resource_type:
                params["type"] = resource_type
            links.append(
                {
                    "rel": "next",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/children?{urlencode(params)}",
                    "title": "Next page",
                }
            )

        return {
            "children": formatted_children,
            "links": links,
            "numberReturned": len(formatted_children),
            "numberMatched": total,
        }
