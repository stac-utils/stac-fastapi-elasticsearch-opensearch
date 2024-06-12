"""FastAPI application."""

import os

from pydantic import BaseModel

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import (
    EmptyRequest,
    create_get_catalog_request_model,
    create_get_collections_request_model,
    create_get_request_model,
    create_post_catalog_full_request_model,
    create_post_catalog_request_model,
    create_post_collections_request_model,
    create_post_request_model,
)
from stac_fastapi.core.core import (
    BulkTransactionsClient,
    CoreClient,
    EsAsyncBaseFiltersClient,
    EsAsyncCollectionSearchClient,
    EsAsyncDiscoverySearchClient,
    TransactionsClient,
)
from stac_fastapi.core.extensions import QueryExtension
from stac_fastapi.core.session import Session
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import (
    DatabaseLogic,
    create_base_catalog_index,
    create_catalog_index,
    create_collection_index,
    create_index_templates,
)
from stac_fastapi.extensions.core import (
    CollectionSearchExtension,
    ContextExtension,
    DiscoverySearchExtension,
    FieldsExtension,
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.types.search import (
    BaseCatalogSearchGetRequest,
    BaseCatalogSearchPostRequest,
)

settings = ElasticsearchSettings()
session = Session.create_from_settings(settings)

filter_extension = FilterExtension(client=EsAsyncBaseFiltersClient())
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)

database_logic = DatabaseLogic()

collection_search_extension = CollectionSearchExtension(
    client=EsAsyncCollectionSearchClient(database_logic)
)
collection_search_extension.conformance_classes.extend(
    [
        "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
        "https://api.stacspec.org/v1.0.0-rc.1/collection-search#free-text",
    ]
)

extensions = [
    FieldsExtension(),
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    ContextExtension(),
    collection_search_extension,
    filter_extension,
]

# Disable transaction extensions by default
# They are enabled by environment variable
if os.getenv("STAC_FASTAPI_ENABLE_TRANSACTIONS", "false") == "true":
    extensions.append(
        TransactionExtension(
            client=TransactionsClient(
                database=database_logic, session=session, settings=settings
            ),
            settings=settings,
        )
    )
    extensions.append(
        BulkTransactionExtension(
            client=BulkTransactionsClient(
                database=database_logic,
                session=session,
                settings=settings,
            )
        )
    )

post_request_model = create_post_request_model(extensions)
get_request_model = create_get_request_model(extensions)

# Includes catalog_id as a path attribute
catalog_post_full_request_model = create_post_catalog_full_request_model(
    extensions=extensions, base_model=BaseCatalogSearchPostRequest
)
# Does not include catalog_id as a path attribute
catalog_post_request_model = create_post_catalog_request_model(
    extensions=extensions, base_model=BaseCatalogSearchPostRequest
)

catalog_get_request_model = create_get_catalog_request_model(
    extensions=extensions, base_model=BaseCatalogSearchGetRequest
)

# TODO: combine into single create_post/get_request_model function
# could add another parameter here for the model name e.g. "CollectionsGetRequest" to combine
# these 'create_request_model' functions with those above
collections_get_request_model = create_get_collections_request_model([], EmptyRequest)
collections_post_request_model = create_post_collections_request_model([], BaseModel)

# Add discovery search here as it requires all other extensions to be passed to it for conformance classes to be identified
discovery_search_extension = DiscoverySearchExtension(
    client=EsAsyncDiscoverySearchClient(database=database_logic, extensions=extensions),
)
discovery_search_extension.conformance_classes.extend(
    ["/catalogues", "/discovery-search"]
)

extensions.append(discovery_search_extension)

# Check if collection search extension is selected
for extension in extensions:
    if isinstance(extension, CollectionSearchExtension):
        collections_get_request_model = create_get_collections_request_model(
            [extension], EmptyRequest
        )
        collections_post_request_model = create_post_collections_request_model(
            [extension], BaseModel
        )
        break

api = StacApi(
    title=os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-elasticsearch"),
    description=os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-elasticsearch"),
    api_version=os.getenv("STAC_FASTAPI_VERSION", "2.1"),
    settings=settings,
    extensions=extensions,
    client=CoreClient(
        database=database_logic,
        session=session,
        post_request_model=post_request_model,
        catalog_post_request_model=catalog_post_request_model,
    ),
    search_get_request_model=get_request_model,
    search_post_request_model=post_request_model,
    collections_get_request_model=collections_get_request_model,
    collections_post_request_model=collections_post_request_model,
    search_catalog_get_request_model=catalog_get_request_model,
    search_catalog_post_request_model=catalog_post_full_request_model,
)
app = api.app
app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")


@app.on_event("startup")
async def _startup_event() -> None:
    if os.getenv("STAC_FASTAPI_ENABLE_TRANSACTIONS", "false") == "true":
        await create_index_templates()
        await create_collection_index()
        await create_catalog_index()
        await create_base_catalog_index()


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
