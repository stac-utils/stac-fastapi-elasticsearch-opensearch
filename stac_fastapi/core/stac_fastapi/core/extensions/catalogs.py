"""Catalogs extension."""

from typing import List, Type, Union

import attr
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.stac import LandingPage


@attr.s
class CatalogsExtension(ApiExtension):
    """Catalogs Extension.

    The Catalogs extension adds a /catalogs endpoint that returns the root catalog.
    """

    client: BaseCoreClient = attr.ib(default=None)
    settings: dict = attr.ib(default=attr.Factory(dict))
    conformance_classes: List[str] = attr.ib(default=attr.Factory(list))
    router: APIRouter = attr.ib(default=attr.Factory(APIRouter))
    response_class: Type[Response] = attr.ib(default=JSONResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        response_model = (
            self.settings.get("response_model")
            if isinstance(self.settings, dict)
            else getattr(self.settings, "response_model", None)
        )

        self.router.add_api_route(
            path="/catalogs",
            endpoint=self.catalogs,
            methods=["GET"],
            response_model=LandingPage if response_model else None,
            response_class=self.response_class,
            summary="Get Catalogs",
            description="Returns the root catalog.",
            tags=["Catalogs"],
        )
        app.include_router(self.router, tags=["Catalogs"])

    async def catalogs(self, request: Request) -> Union[LandingPage, Response]:
        """Get catalogs.

        Args:
            request: Request object.

        Returns:
            The root catalog (landing page).
        """
        return await self.client.landing_page(request=request)
