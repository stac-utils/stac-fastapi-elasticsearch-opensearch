import asyncio
import copy
import json
import os
import sys
from typing import Any, Callable, Dict, Optional

import pytest
import pytest_asyncio
from fastapi import Depends, HTTPException, security, status
from httpx import ASGITransport, AsyncClient
from pydantic import ConfigDict
from stac_pydantic import api

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
from stac_fastapi.core.rate_limit import setup_rate_limit
from stac_fastapi.core.route_dependencies import get_route_dependencies
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.extensions.core.filter import FilterConformanceClasses
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from stac_fastapi.opensearch.config import AsyncOpensearchSettings as AsyncSettings
    from stac_fastapi.opensearch.config import OpensearchSettings as SearchSettings
    from stac_fastapi.opensearch.database_logic import (
        DatabaseLogic,
        create_collection_index,
        create_index_templates,
    )
else:
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

from stac_fastapi.extensions.core import (
    AggregationExtension,
    FieldsExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.types.config import Settings

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class Context:
    def __init__(self, item, collection):
        self.item = item
        self.collection = collection


class MockRequest:
    base_url = "http://test-server"
    url = "http://test-server/test"
    query_params = {}

    def __init__(
        self,
        method: str = "GET",
        url: str = "XXXX",
        app: Optional[Any] = None,
        query_params: Dict[str, Any] = {"limit": "10"},
    ):
        self.method = method
        self.url = url
        self.app = app
        self.query_params = query_params


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
    settings = AsyncSettings()

    filter_extension = FilterExtension(
        client=EsAsyncBaseFiltersClient(database=database)
    )
    filter_extension.conformance_classes.append(
        FilterConformanceClasses.ADVANCED_COMPARISON_OPERATORS
    )

    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest

    search_extensions = [
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=settings
            ),
            settings=settings,
        ),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        filter_extension,
        FreeTextExtension(),
    ]

    extensions = [aggregation_extension] + search_extensions

    post_request_model = create_post_request_model(search_extensions)

    return StacApi(
        settings=settings,
        client=CoreClient(
            database=database,
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=create_get_request_model(search_extensions),
        search_post_request_model=post_request_model,
    ).app


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
    settings = AsyncSettings()

    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest

    search_extensions = [
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=settings
            ),
            settings=settings,
        ),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
    ]

    extensions = [aggregation_extension] + search_extensions

    post_request_model = create_post_request_model(search_extensions)

    app = StacApi(
        settings=settings,
        client=CoreClient(
            database=database,
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=create_get_request_model(search_extensions),
        search_post_request_model=post_request_model,
    ).app

    # Set up rate limit
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
    stac_fastapi_route_dependencies = """[
        {
            "routes":[{"method":"*","path":"*"}],
            "dependencies":[
                {
                    "method":"stac_fastapi.core.basic_auth.BasicAuth",
                    "kwargs":{"credentials":[{"username":"admin","password":"admin"}]}
                }
            ]
        },
        {
            "routes":[
                {"path":"/","method":["GET"]},
                {"path":"/conformance","method":["GET"]},
                {"path":"/collections/{collection_id}/items/{item_id}","method":["GET"]},
                {"path":"/search","method":["GET","POST"]},
                {"path":"/collections","method":["GET"]},
                {"path":"/collections/{collection_id}","method":["GET"]},
                {"path":"/collections/{collection_id}/items","method":["GET"]},
                {"path":"/queryables","method":["GET"]},
                {"path":"/queryables/collections/{collection_id}/queryables","method":["GET"]},
                {"path":"/_mgmt/ping","method":["GET"]}
            ],
            "dependencies":[
                {
                    "method":"stac_fastapi.core.basic_auth.BasicAuth",
                    "kwargs":{"credentials":[{"username":"reader","password":"reader"}]}
                }
            ]
        }
    ]"""

    settings = AsyncSettings()

    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest

    search_extensions = [
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=settings
            ),
            settings=settings,
        ),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
    ]

    extensions = [aggregation_extension] + search_extensions

    post_request_model = create_post_request_model(search_extensions)

    stac_api = StacApi(
        settings=settings,
        client=CoreClient(
            database=database,
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=create_get_request_model(search_extensions),
        search_post_request_model=post_request_model,
        route_dependencies=get_route_dependencies(stac_fastapi_route_dependencies),
    )

    return stac_api.app


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
    # Add file to python path to allow get_route_dependencies to import must_be_bob
    sys.path.append(os.path.dirname(__file__))

    stac_fastapi_route_dependencies = """[
            {
                "routes": [
                    {
                        "method": "GET",
                        "path": "/collections"
                    }
                ],
                "dependencies": [
                    {
                        "method": "conftest.must_be_bob"
                    }
                ]
            }
        ]"""

    settings = AsyncSettings()
    extensions = [
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=settings
            ),
            settings=settings,
        ),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
    ]

    post_request_model = create_post_request_model(extensions)

    return StacApi(
        settings=settings,
        client=CoreClient(
            database=database,
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=create_get_request_model(extensions),
        search_post_request_model=post_request_model,
        route_dependencies=get_route_dependencies(stac_fastapi_route_dependencies),
    ).app


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
    TRANSACTIONS_EXTENSIONS = get_bool_env(
        "ENABLE_TRANSACTIONS_EXTENSIONS", default=True
    )
    settings = AsyncSettings()
    aggregation_extension = AggregationExtension(
        client=EsAsyncBaseAggregationClient(
            database=database, session=None, settings=settings
        )
    )
    aggregation_extension.POST = EsAggregationExtensionPostRequest
    aggregation_extension.GET = EsAggregationExtensionGetRequest
    search_extensions = [
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
        FreeTextExtension(),
    ]
    if TRANSACTIONS_EXTENSIONS:
        search_extensions.insert(
            0,
            TransactionExtension(
                client=TransactionsClient(
                    database=database, session=None, settings=settings
                ),
                settings=settings,
            ),
        )
    extensions = [aggregation_extension] + search_extensions
    post_request_model = create_post_request_model(search_extensions)
    return StacApi(
        settings=settings,
        client=CoreClient(
            database=database,
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=create_get_request_model(search_extensions),
        search_post_request_model=post_request_model,
    ).app
