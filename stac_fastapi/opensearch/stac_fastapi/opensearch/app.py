"""FastAPI application."""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from starlette.middleware import Middleware

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import CORSMiddleware
from stac_fastapi.api.models import (
    ItemCollectionUri,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.core.core import (
    BulkTransactionsClient,
    CoreClient,
    TransactionsClient,
)
from stac_fastapi.core.extensions import QueryExtension
from stac_fastapi.core.extensions.aggregation import (
    EsAggregationExtensionGetRequest,
    EsAggregationExtensionPostRequest,
)
from stac_fastapi.core.extensions.catalogs import CatalogsExtension
from stac_fastapi.core.extensions.collections_search import (
    CollectionsSearchEndpointExtension,
)
from stac_fastapi.core.extensions.fields import FieldsExtension
from stac_fastapi.core.rate_limit import setup_rate_limit
from stac_fastapi.core.route_dependencies import get_route_dependencies
from stac_fastapi.core.session import Session
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.extensions.core import (
    AggregationExtension,
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    CollectionSearchPostExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.fields import FieldsConformanceClasses
from stac_fastapi.extensions.core.filter import FilterConformanceClasses
from stac_fastapi.extensions.core.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.core.query import QueryConformanceClasses
from stac_fastapi.extensions.core.sort import SortConformanceClasses
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.opensearch.config import OpensearchSettings
from stac_fastapi.opensearch.database_logic import (
    DatabaseLogic,
    create_collection_index,
    create_index_templates,
)
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSACTIONS_EXTENSIONS = get_bool_env("ENABLE_TRANSACTIONS_EXTENSIONS", default=True)
ENABLE_COLLECTIONS_SEARCH = get_bool_env("ENABLE_COLLECTIONS_SEARCH", default=True)
ENABLE_COLLECTIONS_SEARCH_ROUTE = get_bool_env(
    "ENABLE_COLLECTIONS_SEARCH_ROUTE", default=False
)
ENABLE_CATALOGS_ROUTE = get_bool_env("ENABLE_CATALOGS_ROUTE", default=False)
logger.info("TRANSACTIONS_EXTENSIONS is set to %s", TRANSACTIONS_EXTENSIONS)
logger.info("ENABLE_COLLECTIONS_SEARCH is set to %s", ENABLE_COLLECTIONS_SEARCH)
logger.info(
    "ENABLE_COLLECTIONS_SEARCH_ROUTE is set to %s", ENABLE_COLLECTIONS_SEARCH_ROUTE
)
logger.info("ENABLE_CATALOGS_ROUTE is set to %s", ENABLE_CATALOGS_ROUTE)

cors_middleware = Middleware(
    CORSMiddleware,
    allow_origins=("*",),
    allow_headers=("Content-Type", "Authorization"),
    allow_credentials=True,
    allow_methods=("OPTIONS", "POST", "GET", "PUT"),
)

settings = OpensearchSettings()
session = Session.create_from_settings(settings)

database_logic = DatabaseLogic()

filter_extension = FilterExtension(
    client=EsAsyncBaseFiltersClient(database=database_logic)
)
filter_extension.conformance_classes.append(
    FilterConformanceClasses.ADVANCED_COMPARISON_OPERATORS
)

aggregation_extension = AggregationExtension(
    client=EsAsyncBaseAggregationClient(
        database=database_logic, session=session, settings=settings
    )
)
aggregation_extension.POST = EsAggregationExtensionPostRequest
aggregation_extension.GET = EsAggregationExtensionGetRequest

fields_extension = FieldsExtension()
fields_extension.conformance_classes.append(FieldsConformanceClasses.ITEMS)

search_extensions = [
    fields_extension,
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    filter_extension,
    FreeTextExtension(),
]


if TRANSACTIONS_EXTENSIONS:
    search_extensions.insert(
        0,
        TransactionExtension(
            client=TransactionsClient(
                database=database_logic, session=session, settings=settings
            ),
            settings=settings,
        ),
    )
    search_extensions.insert(
        1,
        BulkTransactionExtension(
            client=BulkTransactionsClient(
                database=database_logic,
                session=session,
                settings=settings,
            )
        ),
    )

extensions = [aggregation_extension] + search_extensions

# Collection search related variables
collections_get_request_model = None

if ENABLE_COLLECTIONS_SEARCH or ENABLE_COLLECTIONS_SEARCH_ROUTE:
    # Create collection search extensions
    collection_search_extensions = [
        QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
        SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
        FieldsExtension(conformance_classes=[FieldsConformanceClasses.COLLECTIONS]),
        CollectionSearchFilterExtension(
            conformance_classes=[FilterConformanceClasses.COLLECTIONS]
        ),
        FreeTextExtension(conformance_classes=[FreeTextConformanceClasses.COLLECTIONS]),
    ]

    # Initialize collection search with its extensions
    collection_search_ext = CollectionSearchExtension.from_extensions(
        collection_search_extensions
    )
    collections_get_request_model = collection_search_ext.GET

    # Create a post request model for collection search
    collection_search_post_request_model = create_post_request_model(
        collection_search_extensions
    )

# Create collection search extensions if enabled
if ENABLE_COLLECTIONS_SEARCH:
    # Initialize collection search POST extension
    collection_search_post_ext = CollectionSearchPostExtension(
        client=CoreClient(
            database=database_logic,
            session=session,
            post_request_model=collection_search_post_request_model,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        ),
        settings=settings,
        POST=collection_search_post_request_model,
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
            QueryConformanceClasses.COLLECTIONS,
            FilterConformanceClasses.COLLECTIONS,
            FreeTextConformanceClasses.COLLECTIONS,
            SortConformanceClasses.COLLECTIONS,
            FieldsConformanceClasses.COLLECTIONS,
        ],
    )
    extensions.append(collection_search_ext)
    extensions.append(collection_search_post_ext)

if ENABLE_COLLECTIONS_SEARCH_ROUTE:
    # Initialize collections-search endpoint extension
    collections_search_endpoint_ext = CollectionsSearchEndpointExtension(
        client=CoreClient(
            database=database_logic,
            session=session,
            post_request_model=collection_search_post_request_model,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        ),
        settings=settings,
        GET=collections_get_request_model,
        POST=collection_search_post_request_model,
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
            QueryConformanceClasses.COLLECTIONS,
            FilterConformanceClasses.COLLECTIONS,
            FreeTextConformanceClasses.COLLECTIONS,
            SortConformanceClasses.COLLECTIONS,
            FieldsConformanceClasses.COLLECTIONS,
        ],
    )
    extensions.append(collections_search_endpoint_ext)


if ENABLE_CATALOGS_ROUTE:
    catalogs_extension = CatalogsExtension(
        client=CoreClient(
            database=database_logic,
            session=session,
            post_request_model=collection_search_post_request_model,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        ),
        settings=settings,
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-beta.1/catalogs-endpoint",
        ],
    )
    extensions.append(catalogs_extension)


database_logic.extensions = [type(ext).__name__ for ext in extensions]

post_request_model = create_post_request_model(search_extensions)

items_get_request_model = create_request_model(
    model_name="ItemCollectionUri",
    base_model=ItemCollectionUri,
    extensions=[
        SortExtension(
            conformance_classes=[SortConformanceClasses.ITEMS],
        ),
        QueryExtension(
            conformance_classes=[QueryConformanceClasses.ITEMS],
        ),
        filter_extension,
        FieldsExtension(conformance_classes=[FieldsConformanceClasses.ITEMS]),
    ],
    request_type="GET",
)

app_config = {
    "title": os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-opensearch"),
    "description": os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-opensearch"),
    "api_version": os.getenv("STAC_FASTAPI_VERSION", "6.0.0"),
    "settings": settings,
    "extensions": extensions,
    "client": CoreClient(
        database=database_logic,
        session=session,
        post_request_model=post_request_model,
        landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
    ),
    "collections_get_request_model": collections_get_request_model,
    "search_get_request_model": create_get_request_model(search_extensions),
    "search_post_request_model": post_request_model,
    "items_get_request_model": items_get_request_model,
    "route_dependencies": get_route_dependencies(),
    "middlewares": [cors_middleware],
}

# Add collections_get_request_model if it was created
if collections_get_request_model:
    app_config["collections_get_request_model"] = collections_get_request_model

api = StacApi(**app_config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for FastAPI app. Initializes index templates and collections at startup."""
    await create_index_templates()
    await create_collection_index()
    yield


app = api.app
app.router.lifespan_context = lifespan
app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")
# Add rate limit
setup_rate_limit(app, rate_limit=os.getenv("STAC_FASTAPI_RATE_LIMIT"))

# VECTOR TILES

core_client = app_config["client"]


async def tile_route(collection_id: str, z: int, x: int, y: int, request: Request):
    return await core_client.get_tile(collection_id, z, x, y, request)


async def tilejson_route(collection_id: str, request: Request):
    return await core_client.get_tilejson(collection_id, request)


async def clear_tile_cache_route():
    core_client.clear_tile_cache()
    return {"message": "Tile cache cleared successfully"}


async def stac_tile_route(z: int, x: int, y: int, request: Request):
    return await core_client.get_stac_tile(z, x, y, request)


app.add_api_route(
    "/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt",
    tile_route,
    methods=["GET"],
)

app.add_api_route(
    "/collections/{collection_id}/tiles/tilejson.json",
    tilejson_route,
    methods=["GET"],
)

app.add_api_route(
    "/admin/tiles/vector/cache/clear",
    clear_tile_cache_route,
    methods=["POST"],
)

app.add_api_route(
    "/data/tiles/{z}/{x}/{y}.mvt",
    stac_tile_route,
    methods=["GET"],
)


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.opensearch.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
