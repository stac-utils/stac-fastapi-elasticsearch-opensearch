"""FastAPI application."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
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
from stac_fastapi.core.extensions.fields import FieldsExtension
from stac_fastapi.core.rate_limit import setup_rate_limit
from stac_fastapi.core.route_dependencies import get_route_dependencies
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core import (
    AggregationExtension,
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
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

settings = OpensearchSettings()
session = Session.create_from_settings(settings)

database_logic = DatabaseLogic()

filter_extension = FilterExtension(
    client=EsAsyncBaseFiltersClient(database=database_logic)
)
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)

aggregation_extension = AggregationExtension(
    client=EsAsyncBaseAggregationClient(
        database=database_logic, session=session, settings=settings
    )
)
aggregation_extension.POST = EsAggregationExtensionPostRequest
aggregation_extension.GET = EsAggregationExtensionGetRequest

search_extensions = [
    TransactionExtension(
        client=TransactionsClient(
            database=database_logic, session=session, settings=settings
        ),
        settings=settings,
    ),
    BulkTransactionExtension(
        client=BulkTransactionsClient(
            database=database_logic,
            session=session,
            settings=settings,
        )
    ),
    CollectionSearchExtension(),
    FieldsExtension(),
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    filter_extension,
    FreeTextExtension(),
]

extensions = [aggregation_extension] + search_extensions

post_request_model = create_post_request_model(search_extensions)

# Define the collection search extensions map
cs_extensions_map = {
    "query": QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
    "sort": SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
    "fields": FieldsExtension(
        conformance_classes=[FieldsConformanceClasses.COLLECTIONS]
    ),
    "filter": CollectionSearchFilterExtension(
        conformance_classes=[FilterConformanceClasses.COLLECTIONS]
    ),
    "free_text": FreeTextExtension(
        conformance_classes=[FreeTextConformanceClasses.COLLECTIONS],
    ),
}

# Determine enabled extensions (customize as needed)
enabled_extensions = set(cs_extensions_map.keys())

# Build the enabled collection search extensions
cs_extensions = [
    extension
    for key, extension in cs_extensions_map.items()
    if key in enabled_extensions
]

# Create the CollectionSearchExtension from the enabled extensions
collection_search_extension = CollectionSearchExtension.from_extensions(cs_extensions)
collections_get_request_model = collection_search_extension.GET
extensions.append(collection_search_extension)

database_logic.extensions = [type(ext).__name__ for ext in extensions]

post_request_model = create_post_request_model(search_extensions)

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
    "search_get_request_model": create_get_request_model(search_extensions),
    "search_post_request_model": post_request_model,
    "route_dependencies": get_route_dependencies(),
}

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


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.elasticsearch.app:app",
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
