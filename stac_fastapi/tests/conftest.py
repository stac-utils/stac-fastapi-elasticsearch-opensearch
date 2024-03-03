import asyncio
import copy
import json
import os
from typing import Any, Callable, Dict, Optional

import pytest
import pytest_asyncio
from httpx import AsyncClient

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.core.core import (
    BulkTransactionsClient,
    CoreClient,
    TransactionsClient,
)
from stac_fastapi.core.extensions import QueryExtension

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from stac_fastapi.opensearch.config import AsyncOpensearchSettings as AsyncSettings
    from stac_fastapi.opensearch.config import OpensearchSettings as SearchSettings
    from stac_fastapi.opensearch.database_logic import (
        DatabaseLogic,
        create_collection_index,
    )
elif os.getenv("BACKEND", "elasticsearch").lower() == "mongo":
    from stac_fastapi.mongo.config import AsyncMongoDBSettings as AsyncSettings
    from stac_fastapi.mongo.config import MongoDBSettings as SearchSettings
    from stac_fastapi.mongo.database_logic import DatabaseLogic, create_collection_index
else:
    from stac_fastapi.elasticsearch.config import (
        ElasticsearchSettings as SearchSettings,
        AsyncElasticsearchSettings as AsyncSettings,
    )
    from stac_fastapi.elasticsearch.database_logic import (
        DatabaseLogic,
        create_collection_index,
    )

from stac_fastapi.extensions.core import (  # FieldsExtension,
    ContextExtension,
    FieldsExtension,
    FilterExtension,
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
    class Config:
        env_file = ".env.test"


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
        dict(collection), request=MockRequest, refresh=True
    )


async def create_item(txn_client: TransactionsClient, item: Dict) -> None:
    if "collection" in item:
        await txn_client.create_item(
            collection_id=item["collection"],
            item=item,
            request=MockRequest,
            refresh=True,
        )
    else:
        await txn_client.create_item(
            collection_id=item["features"][0]["collection"],
            item=item,
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
    extensions = [
        TransactionExtension(
            client=TransactionsClient(
                database=database, session=None, settings=settings
            ),
            settings=settings,
        ),
        ContextExtension(),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
        FilterExtension(),
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
    ).app


@pytest_asyncio.fixture(scope="session")
async def app_client(app):
    await create_collection_index()

    async with AsyncClient(app=app, base_url="http://test-server") as c:
        yield c
