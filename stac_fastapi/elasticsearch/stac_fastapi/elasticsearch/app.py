"""FastAPI application."""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (
    ItemCollectionUri,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.core.core import CoreClient
from stac_fastapi.core.exceptions import QueuedSuccess, queued_success_handler
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
from stac_fastapi.sfeos_helpers.database.utils import sentry_initialize
from stac_fastapi.sfeos_helpers.models.extensions import Extensions

sentry_enable = get_bool_env("SENTRY_ENABLE", default=False)

if sentry_enable:
    sentry_initialize(
        dsn=os.getenv("SENTRY_DSN"),
        environment=os.getenv("SENTRY_ENVIRONMENT", "staging"),
        traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
        ca_certs=os.getenv("SENTRY_CA_CERTS", None),
    )

logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


def _log_extension_flags(extensions_manager: Extensions) -> None:
    logger.info(
        "ENABLE_TRANSACTIONS_EXTENSIONS is set to %s",
        extensions_manager.transactions_enabled,
    )
    logger.info(
        "ENABLE_COLLECTIONS_SEARCH is set to %s",
        extensions_manager.collections_search_enabled,
    )
    logger.info(
        "ENABLE_COLLECTIONS_SEARCH_ROUTE is set to %s",
        extensions_manager.collections_search_route_enabled,
    )
    logger.info(
        "ENABLE_CATALOGS_ROUTE is set to %s",
        extensions_manager.catalogs_enabled,
    )
    logger.info(
        "HIDE_ALTERNATE_PARENTS is set to %s",
        extensions_manager.hide_alternate_parents,
    )
    logger.info(
        "ENABLE_STAC_VALIDATOR is set to %s",
        get_bool_env("ENABLE_STAC_VALIDATOR", default=False),
    )


def instantiate_api(
    settings: ElasticsearchSettings | None = None,
    database_logic: DatabaseLogic | None = None,
    extensions_manager: Extensions | None = None,
) -> StacApi:
    """Instantiate the Elasticsearch-backed STAC API."""
    settings = settings or ElasticsearchSettings()
    session = Session.create_from_settings(settings)
    database_logic = database_logic or DatabaseLogic()

    if extensions_manager is None:
        extensions_manager = Extensions(
            settings=settings,
            database_logic=database_logic,
            session=session,
        )

    _log_extension_flags(extensions_manager)

    search_extensions = extensions_manager.search
    application_extensions = [
        *extensions_manager.aggregation,
        *search_extensions,
        *extensions_manager.collection_search,
        *extensions_manager.collections_search_route,
        *extensions_manager.catalogs,
        *extensions_manager.extra,
    ]

    database_logic.extensions = [type(ext).__name__ for ext in application_extensions]

    post_request_model = create_post_request_model(search_extensions)
    get_request_model = create_get_request_model(search_extensions)
    collections_get_request_model = extensions_manager.collections_get_request_model

    items_get_request_model = create_request_model(
        model_name="ItemCollectionUri",
        base_model=ItemCollectionUri,
        extensions=extensions_manager.item_collection,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """Initialize index templates and the collections index at startup."""
        await create_index_templates()
        await create_collection_index()
        yield

    title = os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-elasticsearch")
    description = os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-elasticsearch")
    api_version = os.getenv("STAC_FASTAPI_VERSION", "6.19.0")

    app_config_local = {
        "title": title,
        "description": description,
        "api_version": api_version,
        "settings": settings,
        "extensions": application_extensions,
        "client": CoreClient(
            database=database_logic,
            session=session,
            extensions=application_extensions,
            title=title,
            description=description,
            post_request_model=post_request_model,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        ),
        "search_get_request_model": get_request_model,
        "search_post_request_model": post_request_model,
        "items_get_request_model": items_get_request_model,
        "route_dependencies": get_route_dependencies(),
        "app": FastAPI(
            title=title,
            description=description,
            version=api_version,
            openapi_url="/api",
            docs_url="/api.html",
            redoc_url=None,
            lifespan=lifespan,
        ),
    }
    if collections_get_request_model is not None:
        app_config_local[
            "collections_get_request_model"
        ] = collections_get_request_model

    stac_api = StacApi(**app_config_local)
    fastapi_app = stac_api.app

    fastapi_app.add_exception_handler(QueuedSuccess, queued_success_handler)
    fastapi_app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")

    # Make this available for serializers and tests that inspect app config.
    setattr(
        fastapi_app.state,
        "catalogs_hide_alternate_parents",
        extensions_manager.hide_alternate_parents,
    )
    app_config_for_state = app_config_local.copy()
    app_config_for_state.pop("app", None)
    setattr(fastapi_app.state, "app_config", app_config_for_state)

    try:
        from stac_fastapi.sfeos_helpers.metrics import get_instrumentator

        metrics = get_instrumentator()
        metrics.instrument(fastapi_app).expose(fastapi_app, endpoint="/metrics")
    except ImportError:
        logger.warning(
            "prometheus-fastapi-instrumentator not installed; metrics endpoint disabled"
        )

    setup_rate_limit(fastapi_app, rate_limit=os.getenv("STAC_FASTAPI_RATE_LIMIT"))

    return stac_api


def create_app() -> FastAPI:
    """Create a new FastAPI application instance using the factory pattern.

    This function is designed to be used with Uvicorn's --factory flag:
    uvicorn stac_fastapi.elasticsearch.app:create_app --factory

    Returns:
        FastAPI: A fresh FastAPI application instance with all routes configured.
    """
    api = instantiate_api()
    return api.app


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        settings = ElasticsearchSettings()

        uvicorn.run(
            "stac_fastapi.elasticsearch.app:create_app",
            factory=True,
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()


def create_handler(app: FastAPI):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


# For AWS Lambda and other serverless platforms
handler = create_handler(create_app())
