"""Extension assembly helpers for SFEOS backends."""

import logging
import os
from dataclasses import dataclass, field
from typing import Any

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
from stac_fastapi.core.extensions.collections_search import (
    CollectionsSearchEndpointExtension,
)
from stac_fastapi.core.extensions.fields import FieldsExtension
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.extensions import (
    AggregationExtension,
    BulkTransactionExtension,
    CollectionSearchExtension,
    CollectionSearchFilterExtension,
    FilterExtension,
    FreeTextExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.fields import FieldsConformanceClasses
from stac_fastapi.extensions.filter import FilterConformanceClasses
from stac_fastapi.extensions.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.query import QueryConformanceClasses
from stac_fastapi.extensions.sort import (
    CollectionSearchSortExtension,
    ItemCollectionSortExtension,
    SearchSortExtension,
)
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient
from stac_fastapi.types.extension import ApiExtension

logger = logging.getLogger(__name__)


def get_default_extensions_map(
    key: str, database_logic: Any, session: Any, settings: Any
) -> dict[str, ApiExtension]:
    """Generate fresh default extensions per instance to avoid global mutations."""
    filter_extension = FilterExtension(
        client=EsAsyncBaseFiltersClient(database=database_logic, settings=settings)
    )
    filter_extension.conformance_classes.append(
        FilterConformanceClasses.ADVANCED_COMPARISON_OPERATORS
    )

    fields_extension = FieldsExtension()
    fields_extension.conformance_classes.append(FieldsConformanceClasses.ITEMS)

    DEFAULT_EXTENSIONS = {
        "search_map": {
            "fields": fields_extension,
            "query": QueryExtension(),
            "sort": SearchSortExtension(),
            "pagination": TokenPaginationExtension(),
            "filter": filter_extension,
            "free_text": FreeTextExtension(
                conformance_classes=[FreeTextConformanceClasses.SEARCH],
            ),
        },
        "collection_search_map": {
            "query": QueryExtension(
                conformance_classes=[QueryConformanceClasses.COLLECTIONS]
            ),
            "sort": CollectionSearchSortExtension(),
            "fields": FieldsExtension(
                conformance_classes=[FieldsConformanceClasses.COLLECTIONS]
            ),
            "filter": CollectionSearchFilterExtension(
                conformance_classes=[FilterConformanceClasses.COLLECTIONS]
            ),
            "free_text": FreeTextExtension(
                conformance_classes=[FreeTextConformanceClasses.COLLECTIONS]
            ),
        },
        "item_collection_map": {
            "sort": ItemCollectionSortExtension(),
            "query": QueryExtension(
                conformance_classes=[QueryConformanceClasses.ITEMS]
            ),
            "filter": filter_extension,
            "fields": FieldsExtension(
                conformance_classes=[FieldsConformanceClasses.ITEMS]
            ),
            "free_text": FreeTextExtension(
                conformance_classes=[FreeTextConformanceClasses.ITEMS]
            ),
        },
    }
    return DEFAULT_EXTENSIONS.get(key, {})


@dataclass
class Extensions:
    """Build and expose API extensions based on runtime settings and flags."""

    settings: Any
    database_logic: Any
    session: Any

    search_map: dict[str, ApiExtension] = field(default_factory=dict)
    collection_search_map: dict[str, ApiExtension] = field(default_factory=dict)
    item_collection_map: dict[str, ApiExtension] = field(default_factory=dict)
    extra_map: dict[str, ApiExtension] = field(default_factory=dict)

    def __post_init__(self):
        """Merge fresh defaults with user-provided overrides exactly once."""
        for key in [
            "search_map",
            "collection_search_map",
            "item_collection_map",
            "extra_map",
        ]:
            defaults = get_default_extensions_map(
                key,
                database_logic=self.database_logic,
                session=self.session,
                settings=self.settings,
            )
            merged = {**defaults, **getattr(self, key)}
            setattr(self, key, merged)

    def _flag(self, attr_name: str, env_name: str, default: bool) -> bool:
        """Read feature flags from settings first, then env vars for compatibility."""
        value = getattr(self.settings, attr_name, None)
        if value is not None:
            return bool(value)
        return get_bool_env(env_name, default=default)

    @property
    def transactions_enabled(self) -> bool:
        """Whether transaction extensions should be enabled."""
        return self._flag(
            "enable_transactions_extensions", "ENABLE_TRANSACTIONS_EXTENSIONS", True
        )

    @property
    def collections_search_enabled(self) -> bool:
        """Whether collection-search extension routes should be enabled."""
        return self._flag(
            "enable_collections_search", "ENABLE_COLLECTIONS_SEARCH", True
        )

    @property
    def collections_search_route_enabled(self) -> bool:
        """Whether the dedicated collections-search endpoint should be enabled."""
        return self._flag(
            "enable_collections_search_route", "ENABLE_COLLECTIONS_SEARCH_ROUTE", False
        )

    @property
    def catalogs_enabled(self) -> bool:
        """Whether multi-tenant catalogs extension routes should be enabled."""
        return self._flag("enable_catalogs_route", "ENABLE_CATALOGS_ROUTE", False)

    @property
    def hide_alternate_parents(self) -> bool:
        """Whether alternate parent links should be hidden in catalog responses."""
        return self._flag("hide_alternate_parents", "HIDE_ALTERNATE_PARENTS", False)

    def get_enabled_extensions(self, key: str) -> list[ApiExtension]:
        """Return the enabled extensions for the named endpoint mapping."""
        extensions_map = getattr(self, f"{key}_map")
        enabled_extensions_keys = getattr(self.settings, "enabled_extensions", None)

        if enabled_extensions_keys is None:
            return list(extensions_map.values())
        else:
            return [
                extension
                for k, extension in extensions_map.items()
                if k in enabled_extensions_keys
            ]

    @property
    def aggregation(self) -> list[ApiExtension]:
        """Return the aggregation extension."""
        aggregation_extension = AggregationExtension(
            client=EsAsyncBaseAggregationClient(
                database=self.database_logic,
                session=self.session,
                settings=self.settings,
            )
        )
        aggregation_extension.POST = EsAggregationExtensionPostRequest
        aggregation_extension.GET = EsAggregationExtensionGetRequest
        return [aggregation_extension]

    @property
    def search(self) -> list[ApiExtension]:
        """Return the complete item search extension set."""
        return [*self.transaction, *self.get_enabled_extensions("search")]

    @property
    def item_collection(self) -> list[ApiExtension]:
        """Return the item collection extensions."""
        return self.get_enabled_extensions("item_collection")

    @property
    def extra(self) -> list[ApiExtension]:
        """Return custom user-provided out-of-tree extensions."""
        return self.get_enabled_extensions("extra")

    @property
    def transaction(self) -> list[ApiExtension]:
        """Return transaction extensions when enabled."""
        if not self.transactions_enabled:
            return []

        return [
            TransactionExtension(
                client=TransactionsClient(
                    database=self.database_logic,
                    session=self.session,
                    settings=self.settings,
                ),
                settings=self.settings,
            ),
            BulkTransactionExtension(
                client=BulkTransactionsClient(
                    database=self.database_logic,
                    session=self.session,
                    settings=self.settings,
                )
            ),
        ]

    @property
    def collection_search_extension(self) -> CollectionSearchExtension | None:
        """Return the collection search extension when enabled."""
        collections_search = self.collections_search_enabled
        collections_route = self.collections_search_route_enabled

        if not (collections_search or collections_route):
            return None
        return CollectionSearchExtension.from_extensions(
            self.get_enabled_extensions("collection_search")
        )

    @property
    def collection_search_post_request_model(self) -> Any | None:
        """Return the collection search POST request model when enabled."""
        collections_search = self.collections_search_enabled
        collections_route = self.collections_search_route_enabled

        if not (collections_search or collections_route):
            return None
        return create_post_request_model(
            self.get_enabled_extensions("collection_search")
        )

    @property
    def collections_get_request_model(self) -> Any | None:
        """Return the collections GET request model when available."""
        if self.collection_search_extension is None:
            return None
        return self.collection_search_extension.GET

    @property
    def collection_search(self) -> list[ApiExtension]:
        """Return the collection search extension set when enabled.

        Note: We only return the GET handler (CollectionSearchExtension).
        POST /collections is exclusively handled by TransactionExtension
        when transactions are enabled. This prevents route conflicts and
        ensures POST /collections is only available for creating collections.
        """
        if not self.collections_search_enabled:
            return []

        ext = self.collection_search_extension

        if ext is None:
            return []

        # Only return the GET handler, not the POST handler
        # POST /collections is exclusively for TransactionExtension
        return [ext]

    @property
    def collections_search_route(self) -> list[ApiExtension]:
        """Return the collections search route extension when enabled."""
        if not self.collections_search_route_enabled:
            return []

        get_model = self.collections_get_request_model
        post_model = self.collection_search_post_request_model

        if get_model is None or post_model is None:
            return []

        # Dynamically build conformance classes from enabled extensions
        conformance = ["https://api.stacspec.org/v1.0.0-rc.1/collection-search"]
        for ext in self.get_enabled_extensions("collection_search"):
            conformance.extend(ext.conformance_classes)

        return [
            CollectionsSearchEndpointExtension(
                client=CoreClient(
                    database=self.database_logic,
                    session=self.session,
                    post_request_model=post_model,
                    landing_page_id=os.getenv(
                        "STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"
                    ),
                ),
                settings=self.settings,
                GET=get_model,
                POST=post_model,
                conformance_classes=conformance,
            )
        ]

    @property
    def catalogs(self) -> list[ApiExtension]:
        """Return catalog extensions when the catalogs route is enabled."""
        if not self.catalogs_enabled:
            return []

        try:
            from stac_fastapi_catalogs_extension import (
                CATALOGS_SEARCH_CONFORMANCE,
                CatalogsExtension,
                CatalogsSearchExtension,
                CatalogsTransactionExtension,
            )

            from stac_fastapi.core.catalogs_client import CatalogsClient
        except ImportError as exc:
            logger.warning(
                "ENABLE_CATALOGS_ROUTE is true, but stac_fastapi_catalogs_extension "
                "is not installed. Please install with: "
                "pip install stac-fastapi-core[catalogs]. Error: %s",
                exc,
            )
            return []

        search_post_request_model = create_post_request_model(self.search)
        core_client = CoreClient(
            database=self.database_logic,
            session=self.session,
            extensions=self.search,
            post_request_model=search_post_request_model,
            landing_page_id=os.getenv("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"),
        )
        catalogs_client = CatalogsClient(
            database=self.database_logic,
            core_client=core_client,
        )

        hide_parents = self._flag(
            "hide_alternate_parents", "HIDE_ALTERNATE_PARENTS", False
        )

        return [
            CatalogsExtension(
                client=catalogs_client,
                settings=self.settings.model_dump(),
                hide_alternate_parents=hide_parents,
            ),
            CatalogsTransactionExtension(
                client=catalogs_client,
                settings=self.settings.model_dump(),
            ),
            CatalogsSearchExtension(
                client=catalogs_client,
                search_get_request_model=create_get_request_model(self.search),
                search_post_request_model=search_post_request_model,
                conformance_classes=list(CATALOGS_SEARCH_CONFORMANCE),
            ),
        ]
