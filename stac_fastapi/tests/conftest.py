import asyncio
import copy
import json
import os
from typing import Any, Callable, Dict, Optional

import pytest
import pytest_asyncio
from fastapi import Depends, HTTPException, security, status
from httpx import ASGITransport, AsyncClient
from pydantic import ConfigDict
from stac_pydantic import api

from stac_fastapi.api.app import StacApi
from stac_fastapi.core.basic_auth import BasicAuth
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
from stac_fastapi.core.rate_limit import setup_rate_limit
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.extensions.core import (
    AggregationExtension,
    FieldsExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX
from stac_fastapi.types.config import Settings

os.environ.setdefault("ENABLE_COLLECTIONS_SEARCH_ROUTE", "true")
os.environ.setdefault("ENABLE_CATALOGS_ROUTE", "false")

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from stac_fastapi.opensearch.app import app_config
    from stac_fastapi.opensearch.config import AsyncOpensearchSettings as AsyncSettings
    from stac_fastapi.opensearch.config import OpensearchSettings as SearchSettings
    from stac_fastapi.opensearch.database_logic import (
        DatabaseLogic,
        create_collection_index,
        create_index_templates,
    )
else:
    from stac_fastapi.elasticsearch.app import app_config
    from stac_fastapi.elasticsearch.config import (
        AsyncElasticsearchSettings as AsyncSettings,
    )
    from stac_fastapi.elasticsearch.config import (
        ElasticsearchSettings as SearchSettings,
    )
    from stac_fastapi.elasticsearch.database_logic import (
        DatabaseLogic,
        create_collection_index,
        create_index_templates,
    )

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class Context:
    def __init__(self, item, collection):
        self.item = item
        self.collection = collection


class MockRequest:
    base_url = "http://test-server"
    url = "http://test-server/test"
    headers = {}
    query_params = {}

    def __init__(
        self,
        method: str = "GET",
        url: str = "XXXX",
        app: Optional[Any] = None,
        query_params: Dict[str, Any] = {"limit": "10"},
        headers: Dict[str, Any] = {"content-type": "application/json"},
    ):
        self.method = method
        self.url = url
        self.app = app
        self.query_params = query_params
        self.headers = headers


class TestSettings(AsyncSettings):
    model_config = ConfigDict(env_file=".env.test")


settings = TestSettings()
Settings.set(settings)


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def _load_file(filename: str) -> Dict:
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)


_test_item_prototype = _load_file("test_item.json")
_test_collection_prototype = _load_file("test_collection.json")


@pytest.fixture
def load_test_data() -> Callable[[str], Dict]:
    return _load_file


@pytest.fixture
def test_item() -> Dict:
    return copy.deepcopy(_test_item_prototype)


@pytest.fixture
def test_collection() -> Dict:
    return copy.deepcopy(_test_collection_prototype)


async def create_collection(txn_client: TransactionsClient, collection: Dict) -> None:
    await txn_client.create_collection(
        api.Collection(**dict(collection)), request=MockRequest, refresh=True
    )


async def create_item(txn_client: TransactionsClient, item: Dict) -> None:
    if "collection" in item:
        await txn_client.create_item(
            collection_id=item["collection"],
            item=api.Item(**item),
            request=MockRequest,
            refresh=True,
        )
    else:
        await txn_client.create_item(
            collection_id=item["features"][0]["collection"],
            item=api.ItemCollection(**item),
            request=MockRequest,
            refresh=True,
        )


async def delete_collections_and_items(txn_client: TransactionsClient) -> None:
    await refresh_indices(txn_client)
    await txn_client.database.delete_items()
    await txn_client.database.delete_collections()
    await txn_client.database.client.indices.delete(index=f"{ITEMS_INDEX_PREFIX}*")
    await txn_client.database.async_index_selector.refresh_cache()


async def refresh_indices(txn_client: TransactionsClient) -> None:
    try:
        await txn_client.database.client.indices.refresh(index="_all")
    except Exception:
        pass


@pytest_asyncio.fixture()
async def ctx(txn_client: TransactionsClient, test_collection, test_item):
    # todo remove one of these when all methods use it
    await delete_collections_and_items(txn_client)

    await create_collection(txn_client, test_collection)
    await create_item(txn_client, test_item)

    yield Context(item=test_item, collection=test_collection)

    await delete_collections_and_items(txn_client)


database = DatabaseLogic()
settings = SearchSettings()


@pytest.fixture
def core_client():
    return CoreClient(database=database, session=None)


@pytest.fixture
def txn_client():
    return TransactionsClient(database=database, session=None, settings=settings)


@pytest.fixture
def bulk_txn_client():
    return BulkTransactionsClient(database=database, session=None, settings=settings)


@pytest_asyncio.fixture(scope="session")
async def app():
    return StacApi(**app_config).app


@pytest_asyncio.fixture(scope="session")
async def app_client(app):
    await create_index_templates()
    await create_collection_index()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test-server"
    ) as c:
        yield c


@pytest_asyncio.fixture(scope="session")
async def app_rate_limit():
    """Fixture to get the FastAPI app with test-specific rate limiting."""
    app = StacApi(**app_config).app
    setup_rate_limit(app, rate_limit="2/minute")

    return app


@pytest_asyncio.fixture(scope="session")
async def app_client_rate_limit(app_rate_limit):
    await create_index_templates()
    await create_collection_index()

    async with AsyncClient(
        transport=ASGITransport(app=app_rate_limit), base_url="http://test-server"
    ) as c:
        yield c


@pytest_asyncio.fixture(scope="session")
async def app_basic_auth():
    """Fixture to get the FastAPI app with basic auth configured."""

    # Create a copy of the app config
    test_config = app_config.copy()

    # Create basic auth dependency wrapped in Depends
    basic_auth = Depends(
        BasicAuth(credentials=[{"username": "admin", "password": "admin"}])
    )

    # Define public routes that don't require auth
    public_paths = {
        "/": ["GET"],
        "/conformance": ["GET"],
        "/collections/{collection_id}/items/{item_id}": ["GET"],
        "/search": ["GET", "POST"],
        "/collections": ["GET"],
        "/collections/{collection_id}": ["GET"],
        "/collections/{collection_id}/items": ["GET"],
        "/queryables": ["GET"],
        "/collections/{collection_id}/queryables": ["GET"],
        "/_mgmt/ping": ["GET"],
    }

    # Initialize route dependencies with public paths
    test_config["route_dependencies"] = [
        (
            [{"path": path, "method": method} for method in methods],
            [],  # No auth for public routes
        )
        for path, methods in public_paths.items()
    ]

    # Add catch-all route with basic auth
    test_config["route_dependencies"].extend(
        [
            (
                [{"path": "*", "method": "*"}],
                [basic_auth],
            )  # Require auth for all other routes
        ]
    )

    # Create the app with basic auth
    api = StacApi(**test_config)
    return api.app


@pytest_asyncio.fixture(scope="session")
async def app_client_basic_auth(app_basic_auth):
    await create_index_templates()
    await create_collection_index()

    async with AsyncClient(
        transport=ASGITransport(app=app_basic_auth), base_url="http://test-server"
    ) as c:
        yield c


def must_be_bob(
    credentials: security.HTTPBasicCredentials = Depends(security.HTTPBasic()),
):
    if credentials.username == "bob":
        return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="You're not Bob",
        headers={"WWW-Authenticate": "Basic"},
    )


@pytest_asyncio.fixture(scope="session")
async def route_dependencies_app():
    """Fixture to get the FastAPI app with custom route dependencies."""
    # Create a copy of the app config
    test_config = app_config.copy()

    # Define route dependencies
    test_config["route_dependencies"] = [
        ([{"method": "GET", "path": "/collections"}], [Depends(must_be_bob)])
    ]

    # Create the app with custom route dependencies
    api = StacApi(**test_config)
    return api.app


@pytest_asyncio.fixture(scope="session")
async def route_dependencies_client(route_dependencies_app):
    await create_index_templates()
    await create_collection_index()

    async with AsyncClient(
        transport=ASGITransport(app=route_dependencies_app),
        base_url="http://test-server",
    ) as c:
        yield c


def build_test_app():
    """Build a test app with configurable transaction extensions."""
    # Create a copy of the base config
    test_config = app_config.copy()

    # Get transaction extensions setting
    TRANSACTIONS_EXTENSIONS = get_bool_env(
        "ENABLE_TRANSACTIONS_EXTENSIONS", default=True
    )

    # Configure extensions
    settings = AsyncSettings()
    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest

    search_extensions = [
        FieldsExtension(),
        SortExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
    ]

    # Add transaction extension if enabled
    if TRANSACTIONS_EXTENSIONS:
        search_extensions.append(
            TransactionExtension(
                client=TransactionsClient(
                    database=database, session=None, settings=settings
                ),
                settings=settings,
            )
        )

    # Update extensions in config
    extensions = [aggregation_extension] + search_extensions
    test_config["extensions"] = extensions

    # Update client with new extensions
    test_config["client"] = CoreClient(
        database=database,
        session=None,
        extensions=extensions,
        post_request_model=test_config["search_post_request_model"],
    )

    # Create and return the app
    api = StacApi(**test_config)
    return api.app


def build_test_app_with_catalogs():
    """Build a test app with catalogs extension enabled."""
    from stac_fastapi.core.extensions.catalogs import CatalogsExtension

    # Get the base config
    test_config = app_config.copy()

    # Get database and settings (already imported above)
    test_database = DatabaseLogic()
    test_settings = AsyncSettings()

    # Add catalogs extension
    catalogs_extension = CatalogsExtension(
        client=CoreClient(
            database=test_database,
            session=None,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        ),
        settings=test_settings,
        conformance_classes=[
            "https://api.stacspec.org/v1.0.0-beta.1/catalogs-endpoint",
        ],
    )

    # Add to extensions if not already present
    if not any(isinstance(ext, CatalogsExtension) for ext in test_config["extensions"]):
        test_config["extensions"].append(catalogs_extension)

    # Update client with new extensions
    test_config["client"] = CoreClient(
        database=test_database,
        session=None,
        extensions=test_config["extensions"],
        post_request_model=test_config["search_post_request_model"],
        landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
    )

    # Create and return the app
    api = StacApi(**test_config)
    return api.app


@pytest_asyncio.fixture(scope="session")
async def catalogs_app():
    """Fixture to get the FastAPI app with catalogs extension enabled."""
    return build_test_app_with_catalogs()


@pytest_asyncio.fixture(scope="session")
async def catalogs_app_client(catalogs_app):
    """Fixture to get an async client for the app with catalogs extension enabled."""
    await create_index_templates()
    await create_collection_index()

    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app), base_url="http://test-server"
    ) as c:
        yield c
