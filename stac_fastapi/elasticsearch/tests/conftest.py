import copy
import json
import os
from typing import Callable, Dict

import pytest
from starlette.testclient import TestClient

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_request_model
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.core import (
    BulkTransactionsClient,
    CoreCrudClient,
    TransactionsClient,
)
from stac_fastapi.elasticsearch.database_logic import COLLECTIONS_INDEX, ITEMS_INDEX
from stac_fastapi.elasticsearch.extensions import QueryExtension
from stac_fastapi.elasticsearch.indexes import IndexesClient
from stac_fastapi.extensions.core import (
    ContextExtension,
    FieldsExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.types.config import Settings
from stac_fastapi.types.search import BaseSearchGetRequest, BaseSearchPostRequest

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestSettings(ElasticsearchSettings):
    class Config:
        env_file = ".env.test"


settings = TestSettings()
Settings.set(settings)


def _load_file(filename: str) -> Dict:
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)


@pytest.fixture
def load_test_data() -> Callable[[str], Dict]:
    return _load_file


_test_item_prototype = _load_file("test_item.json")
_test_collection_prototype = _load_file("test_collection.json")


@pytest.fixture
def test_item() -> Dict:
    return copy.deepcopy(_test_item_prototype)


@pytest.fixture
def test_collection() -> Dict:
    return copy.deepcopy(_test_collection_prototype)


def create_collection(es_txn_client: TransactionsClient, collection: Dict) -> None:
    es_txn_client.create_collection(
        dict(collection), request=MockStarletteRequest, refresh=True
    )


def create_item(es_txn_client: TransactionsClient, item: Dict) -> None:
    es_txn_client.create_item(dict(item), request=MockStarletteRequest, refresh=True)


def delete_collections_and_items(es_txn_client: TransactionsClient) -> None:
    refresh_indices(es_txn_client)
    # try:
    es_txn_client.database.delete_items()
    # except Exception:
    #     pass

    # try:
    es_txn_client.database.delete_collections()
    # except Exception:
    #     pass


def refresh_indices(es_txn_client: TransactionsClient) -> None:
    try:
        es_txn_client.database.client.indices.refresh(index=ITEMS_INDEX)
    except Exception:
        pass

    try:
        es_txn_client.database.client.indices.refresh(index=COLLECTIONS_INDEX)
    except Exception:
        pass


class Context:
    def __init__(self, item, collection):
        self.item = item
        self.collection = collection


@pytest.fixture()
def ctx(es_txn_client: TransactionsClient, test_collection, test_item):
    # todo remove one of these when all methods use it
    delete_collections_and_items(es_txn_client)

    create_collection(es_txn_client, test_collection)
    create_item(es_txn_client, test_item)

    yield Context(item=test_item, collection=test_collection)

    delete_collections_and_items(es_txn_client)


class MockStarletteRequest:
    base_url = "http://test-server"


@pytest.fixture
def es_core():
    return CoreCrudClient(session=None)


@pytest.fixture
def es_txn_client():
    return TransactionsClient(session=None)


@pytest.fixture
def es_bulk_transactions():
    return BulkTransactionsClient(session=None)


@pytest.fixture
def api_client():
    settings = ElasticsearchSettings()
    extensions = [
        TransactionExtension(
            client=TransactionsClient(session=None), settings=settings
        ),
        ContextExtension(),
        SortExtension(),
        FieldsExtension(),
        QueryExtension(),
        TokenPaginationExtension(),
    ]

    get_request_model = create_request_model(
        "SearchGetRequest",
        base_model=BaseSearchGetRequest,
        extensions=extensions,
        request_type="GET",
    )

    post_request_model = create_request_model(
        "SearchPostRequest",
        base_model=BaseSearchPostRequest,
        extensions=extensions,
        request_type="POST",
    )

    return StacApi(
        settings=settings,
        client=CoreCrudClient(
            session=None,
            extensions=extensions,
            post_request_model=post_request_model,
        ),
        extensions=extensions,
        search_get_request_model=get_request_model,
        search_post_request_model=post_request_model,
    )


@pytest.fixture
def app_client(api_client: StacApi):
    IndexesClient().create_indexes()

    with TestClient(api_client.app) as test_app:
        yield test_app
