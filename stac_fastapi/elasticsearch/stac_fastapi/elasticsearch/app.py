"""FastAPI application."""

import logging
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
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import (
    DatabaseLogic,
    create_collection_index,
    create_index_templates,
)
from stac_fastapi.extensions.core import (
    AggregationExtension,
    CollectionSearchExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.core.filter import FilterConformanceClasses
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSACTIONS_EXTENSIONS = get_bool_env("ENABLE_TRANSACTIONS_EXTENSIONS", default=True)
logger.info("TRANSACTIONS_EXTENSIONS is set to %s", TRANSACTIONS_EXTENSIONS)

settings = ElasticsearchSettings()
session = Session.create_from_settings(settings)

database_logic = DatabaseLogic()

filter_extension = FilterExtension(
    client=EsAsyncBaseFiltersClient(database=database_logic)
)
filter_extension.conformance_classes.append(
    FilterConformanceClasses.ADVANCED_COMPARISON_OPERATORS
)

# Adding collection search extension for compatibility with stac-auth-proxy
# (https://github.com/developmentseed/stac-auth-proxy)
# The extension is not fully implemented yet but is required for collection filtering support
collection_search_extension = CollectionSearchExtension()
collection_search_extension.conformance_classes.append(
    "https://api.stacspec.org/v1.0.0-rc.1/collection-search#filter"
)

aggregation_extension = AggregationExtension(
    client=EsAsyncBaseAggregationClient(
        database=database_logic, session=session, settings=settings
    )
)
aggregation_extension.POST = EsAggregationExtensionPostRequest
aggregation_extension.GET = EsAggregationExtensionGetRequest

search_extensions = [
    FieldsExtension(),
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    filter_extension,
    FreeTextExtension(),
    collection_search_extension,
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

database_logic.extensions = [type(ext).__name__ for ext in extensions]

post_request_model = create_post_request_model(search_extensions)

app_config = {
    "title": os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-elasticsearch"),
    "description": os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-elasticsearch"),
    "api_version": os.getenv("STAC_FASTAPI_VERSION", "6.2.0"),
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
