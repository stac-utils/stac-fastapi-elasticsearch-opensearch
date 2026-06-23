import asyncio
import copy
import json
import os
from typing import Any, Callable, List

import pytest
import pytest_asyncio
from fastapi import Depends, HTTPException
from fastapi import params as fastapi_params
from fastapi import security, status
from fastapi.routing import BaseRoute
from httpx import ASGITransport, AsyncClient
from pydantic import ConfigDict
from stac_pydantic import api

import stac_fastapi.api.routes


def _patched_add_route_dependencies(
    routes: List[BaseRoute], scopes: List[dict], dependencies: List[Depends]
) -> None:
    """Add dependencies to routes, with FastAPI >= 0.137 _IncludedRouter support."""
    for route in routes:
        # 1. Safely recurse into FastAPI >= 0.137 _IncludedRouters
        if hasattr(route, "original_router"):
            _patched_add_route_dependencies(
                route.original_router.routes, scopes, dependencies
            )
            continue

        # 2. Recurse into Mounts or older Starlette sub-routers
        if hasattr(route, "routes") and route.routes:
            _patched_add_route_dependencies(route.routes, scopes, dependencies)
            continue

        # 3. Skip anything that isn't a standard endpoint with dependencies attribute
        if (
            not hasattr(route, "path")
            or not hasattr(route, "dependencies")
            or not hasattr(route, "methods")
        ):
            continue

        # 4. Check if route matches any scope and add dependencies
        for scope in scopes:
            # Check if route matches scope path
            scope_path_matches = scope["path"] == "*" or scope["path"] == route.path
            if not scope_path_matches:
                continue

            # Check if route matches scope method
            scope_method_matches = (
                scope["method"] == "*" or scope["method"] in route.methods
            )
            if not scope_method_matches:
                continue

            # Route matches this scope - add dependencies and stop checking other scopes
            for dep in dependencies:
                if isinstance(dep, fastapi_params.Depends):
                    route.dependencies.append(dep)
                else:
                    route.dependencies.append(fastapi_params.Depends(dep))
            break  # Only apply the first matching scope's dependencies


# Apply the monkey-patch BEFORE StacApi is imported
stac_fastapi.api.routes.add_route_dependencies = _patched_add_route_dependencies

from stac_fastapi.api.app import StacApi  # noqa: E402
from stac_fastapi.core.basic_auth import BasicAuth  # noqa: E402
from stac_fastapi.core.core import (  # noqa: E402
    BulkTransactionsClient,
    CoreClient,
    TransactionsClient,
)
from stac_fastapi.core.extensions import QueryExtension  # noqa: E402
from stac_fastapi.core.extensions.aggregation import (  # noqa: E402
    EsAggregationExtensionGetRequest,
    EsAggregationExtensionPostRequest,
)
from stac_fastapi.core.rate_limit import setup_rate_limit  # noqa: E402
from stac_fastapi.core.utilities import get_bool_env  # noqa: E402
from stac_fastapi.extensions import (  # noqa: E402
    AggregationExtension,
    FieldsExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.sfeos_helpers.aggregation import (  # noqa: E402
    EsAsyncBaseAggregationClient,
)
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX  # noqa: E402
from stac_fastapi.types.config import Settings  # noqa: E402

os.environ.setdefault("ENABLE_COLLECTIONS_SEARCH_ROUTE", "true")
os.environ.setdefault("ENABLE_CATALOGS_ROUTE", "false")
os.environ.setdefault("DATABASE_REFRESH", "true")

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


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "datetime_filtering: matches datetime_filtering mark"
    )
    config.addinivalue_line(
        "filterwarnings", "ignore:Duplicate Operation ID:UserWarning"
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
        app: Any | None = None,
        query_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ):
        self.method = method
        self.url = url
        self.app = app
        self.query_params = query_params or {"limit": "10"}
        self.headers = headers or {"content-type": "application/json"}


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


def _load_file(filename: str) -> dict:
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)


_test_item_prototype = _load_file("test_item.json")
_test_collection_prototype = _load_file("test_collection.json")


@pytest.fixture
def load_test_data() -> Callable[[str], dict]:
    return _load_file


@pytest.fixture
def test_item() -> dict:
    return copy.deepcopy(_test_item_prototype)


@pytest.fixture
def test_collection() -> dict:
    return copy.deepcopy(_test_collection_prototype)


async def create_collection(txn_client: TransactionsClient, collection: dict) -> None:
    await txn_client.create_collection(
        api.Collection(**dict(collection)), request=MockRequest, refresh=True
    )


async def create_item(txn_client: TransactionsClient, item: dict) -> None:
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
    # Ensure the main app fixture doesn't have any route dependencies
    # (no BasicAuth or other authentication)
    test_config = app_config.copy()
    test_config["route_dependencies"] = []
    api = StacApi(**test_config)
    return api.app


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
    # Create a shallow copy of the base config
    test_config = app_config.copy()

    # Explicitly clear route dependencies to prevent state leakage
    test_config["route_dependencies"] = []

    app = StacApi(**test_config).app
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
    """Fixture to get the FastAPI app with basic auth configured cleanly."""

    # 1. Create a shallow copy of the base config
    test_config = app_config.copy()

    # 2. CRITICAL FIX: Rebuild extensions and clients from scratch!
    # This ensures app_basic_auth gets its own fresh APIRouters.
    # If we share the global extensions, our monkey-patch will poison
    # the routes for the entire test suite (FastAPI >= 0.137 shared state leak).
    auth_settings = AsyncSettings()
    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=auth_settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest

    auth_extensions = [
        aggregation_extension,
        FieldsExtension(),
        SortExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=auth_settings
            ),
            settings=auth_settings,
        ),
    ]
    test_config["extensions"] = auth_extensions
    test_config["client"] = CoreClient(
        database=database,
        session=None,
        extensions=auth_extensions,
        post_request_model=test_config["search_post_request_model"],
    )

    # 3. Create basic auth dependency
    basic_auth = Depends(
        BasicAuth(credentials=[{"username": "admin", "password": "admin"}])
    )

    # 4. Define public routes that don't require auth
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

    test_config["route_dependencies"] = [
        ([{"path": path, "method": method} for method in methods], [])
        for path, methods in public_paths.items()
    ]

    # Add catch-all route with basic auth
    test_config["route_dependencies"].extend(
        [([{"path": "*", "method": "*"}], [basic_auth])]
    )

    # 5. Create the app with basic auth
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

    # Define route dependencies (explicitly override any from app_config)
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

    # CRITICAL FIX: Rebuild extensions and clients from scratch!
    # This ensures build_test_app gets its own fresh APIRouters and prevents
    # shared state leakage between tests (FastAPI >= 0.137 shared state leak).
    test_settings = AsyncSettings()
    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=test_settings
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
                    database=database, session=None, settings=test_settings
                ),
                settings=test_settings,
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

    # Ensure no route dependencies (no BasicAuth) for this test app
    test_config["route_dependencies"] = []

    # Create and return the app
    api = StacApi(**test_config)
    return api.app


def build_test_app_with_catalogs():
    """Build a test app with catalogs extension enabled."""
    from stac_fastapi_catalogs_extension import (
        CATALOGS_SEARCH_CONFORMANCE,
        CatalogsExtension,
        CatalogsSearchExtension,
        CatalogsTransactionExtension,
    )

    from stac_fastapi.core.catalogs_client import CatalogsClient

    # Get the base config
    test_config = app_config.copy()

    # CRITICAL FIX: Rebuild extensions and clients from scratch!
    # This ensures build_test_app_with_catalogs gets its own fresh APIRouters and prevents
    # shared state leakage between tests (FastAPI >= 0.137 shared state leak).
    test_settings = AsyncSettings()
    test_database = DatabaseLogic()

    # Rebuild all extensions from scratch (not reusing global ones)
    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=test_database, session=None, settings=test_settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest
    # Create core client for search delegation
    core_client = CoreClient(
        database=test_database,
        session=None,
        post_request_model=test_config["search_post_request_model"],
        landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
    )

    # Create shared catalogs client with core_client
    catalogs_client = CatalogsClient(database=test_database, core_client=core_client)

    # Create catalogs client and extensions
    catalogs_client = CatalogsClient(database=test_database)
    catalogs_extension = CatalogsExtension(
        client=catalogs_client,
        settings=test_settings.model_dump(),
    )
    catalogs_transaction_extension = CatalogsTransactionExtension(
        client=catalogs_client,
        settings=test_settings.model_dump(),
    )

    # Filter out CollectionsSearchExtension to avoid duplicate base classes
    from stac_fastapi.core.extensions.collections_search import (
        CollectionsSearchEndpointExtension,
    )

    filtered_extensions = [
        ext
        for ext in test_config["extensions"]
        if not isinstance(ext, CollectionsSearchEndpointExtension)
    ]

    # Remove CollectionsSearchEndpointExtension from test_config["extensions"]
    test_config["extensions"] = filtered_extensions

    # Add catalogs search extension
    # Use BaseSearchGetRequest directly to avoid duplicate base class issues
    from stac_fastapi.types.search import BaseSearchGetRequest

    catalogs_search_extension = CatalogsSearchExtension(
        client=catalogs_client,
        search_get_request_model=BaseSearchGetRequest,
        search_post_request_model=test_config["search_post_request_model"],
        conformance_classes=list(CATALOGS_SEARCH_CONFORMANCE),
    )

    # Add to extensions if not already present
    if not any(isinstance(ext, CatalogsExtension) for ext in test_config["extensions"]):
        test_config["extensions"].append(catalogs_extension)
    if not any(
        isinstance(ext, CatalogsTransactionExtension)
        for ext in test_config["extensions"]
    ):
        test_config["extensions"].append(catalogs_transaction_extension)
    if not any(
        isinstance(ext, CatalogsSearchExtension) for ext in test_config["extensions"]
    ):
        test_config["extensions"].append(catalogs_search_extension)

    # Update client with new extensions
    test_config["client"] = core_client

    # Ensure no route dependencies (no BasicAuth) for this test app
    test_config["route_dependencies"] = []

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


def get_flattened_routes(router_obj, prefix=""):
    """
    Recursively extracts all flattened routes from a FastAPI app,
    navigating through Mounts, APIRouters, and FastAPI >= 0.137 _IncludedRouters.
    """
    api_routes = set()
    routes = getattr(router_obj, "routes", [])

    for route in routes:
        # 1. Standard Endpoints (APIRoute)
        if hasattr(route, "methods") and route.methods:
            for m in route.methods:
                if m == "HEAD":
                    continue
                r_path = getattr(route, "path", "")
                full_path = f"{prefix}{r_path}".replace("//", "/")
                api_routes.add(f"{m} {full_path}")

        # 2. Recurse into Mounts (Starlette)
        if hasattr(route, "app") and hasattr(route.app, "routes"):
            r_path = getattr(route, "path", getattr(route, "prefix", ""))
            next_prefix = f"{prefix}{r_path}"
            api_routes.update(get_flattened_routes(route.app, next_prefix))

        # 3. Recurse into FastAPI >= 0.137 _IncludedRouter wrappers
        if hasattr(route, "original_router"):
            r_prefix = getattr(route, "prefix", "")
            if not r_prefix and hasattr(route, "include_context"):
                r_prefix = getattr(route.include_context, "prefix", "")
            next_prefix = f"{prefix}{r_prefix}"
            api_routes.update(get_flattened_routes(route.original_router, next_prefix))

        # 4. Recurse into classic FastAPI/Starlette Routers (< 0.137)
        elif hasattr(route, "routes") and route is not router_obj:
            r_path = getattr(route, "path", getattr(route, "prefix", ""))
            next_prefix = f"{prefix}{r_path}"
            api_routes.update(get_flattened_routes(route, next_prefix))

    return api_routes


@pytest_asyncio.fixture()
async def mock_datetime_env(txn_client, monkeypatch):
    """Set USE_DATETIME environment variable to False for testing."""
    monkeypatch.setenv("USE_DATETIME", "false")
    if hasattr(txn_client.database.async_index_selector, "cache_manager"):
        await txn_client.database.async_index_selector.cache_manager.clear_cache()
    yield
    monkeypatch.setenv("USE_DATETIME", "true")
