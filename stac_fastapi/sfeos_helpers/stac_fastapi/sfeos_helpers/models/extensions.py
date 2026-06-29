"""Extension assembly helpers for SFEOS backends."""

import logging
import os
from dataclasses import dataclass
from functools import cached_property
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
    CollectionSearchPostExtension,
    FilterExtension,
    FreeTextExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.fields import FieldsConformanceClasses
from stac_fastapi.extensions.filter import FilterConformanceClasses
from stac_fastapi.extensions.free_text import FreeTextConformanceClasses
from stac_fastapi.extensions.query import QueryConformanceClasses
from stac_fastapi.extensions.sort import SortConformanceClasses
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient
from stac_fastapi.types.extension import ApiExtension

logger = logging.getLogger(__name__)


@dataclass
class Extensions:
    """Build and expose API extensions based on runtime settings and flags."""

    settings: Any
    database_logic: Any
    session: Any

    def _flag(self, attr_name: str, env_name: str, default: bool) -> bool:
        """Read feature flags from settings first, then env vars for compatibility."""
        value = getattr(self.settings, attr_name, None)
        if value is not None:
            return bool(value)
        return get_bool_env(env_name, default=default)

    @cached_property
    def transactions_enabled(self) -> bool:
        """Whether transaction extensions should be enabled."""
        return self._flag(
            attr_name="enable_transactions_extensions",
            env_name="ENABLE_TRANSACTIONS_EXTENSIONS",
            default=True,
        )

    @cached_property
    def collections_search_enabled(self) -> bool:
        """Whether collection-search extension routes should be enabled."""
        return self._flag(
            attr_name="enable_collections_search",
            env_name="ENABLE_COLLECTIONS_SEARCH",
            default=True,
        )

    @cached_property
    def collections_search_route_enabled(self) -> bool:
        """Whether the dedicated collections-search endpoint should be enabled."""
        return self._flag(
            attr_name="enable_collections_search_route",
            env_name="ENABLE_COLLECTIONS_SEARCH_ROUTE",
            default=False,
        )

    @cached_property
    def catalogs_enabled(self) -> bool:
        """Whether multi-tenant catalogs extension routes should be enabled."""
        return self._flag(
            attr_name="enable_catalogs_route",
            env_name="ENABLE_CATALOGS_ROUTE",
            default=False,
        )

    @cached_property
    def hide_alternate_parents(self) -> bool:
        """Whether alternate parent links should be hidden in catalog responses."""
        return self._flag(
            attr_name="hide_alternate_parents",
            env_name="HIDE_ALTERNATE_PARENTS",
            default=False,
        )

    @cached_property
    def filter_extension(self) -> FilterExtension:
        """Filter extension configured with SFEOS filter backend capabilities."""
        filter_extension = FilterExtension(
            client=EsAsyncBaseFiltersClient(
                database=self.database_logic,
                settings=self.settings,
            )
        )
        filter_extension.conformance_classes.append(
            FilterConformanceClasses.ADVANCED_COMPARISON_OPERATORS
        )
        return filter_extension

    @cached_property
    def aggregation(self) -> list[ApiExtension]:
        """Build aggregation extension with SFEOS aggregation request models."""
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

    @cached_property
    def base_search(self) -> list[ApiExtension]:
        """Build base search extensions for item-search request model assembly."""
        fields_extension = FieldsExtension()
        fields_extension.conformance_classes.append(FieldsConformanceClasses.ITEMS)

        return [
            fields_extension,
            QueryExtension(),
            SortExtension(),
            TokenPaginationExtension(),
            self.filter_extension,
            FreeTextExtension(
                conformance_classes=[FreeTextConformanceClasses.SEARCH],
            ),
        ]

    @cached_property
    def transaction(self) -> list[ApiExtension]:
        """Transaction-related extensions enabled by runtime feature flags."""
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

    @cached_property
    def search(self) -> list[ApiExtension]:
        """Complete item search extension set including optional transactions."""
        return [*self.transaction, *self.base_search]

    @cached_property
    def collections_search_base(self) -> list[ApiExtension]:
        """Build base extensions for collections-search request and route models."""
        return [
            QueryExtension(conformance_classes=[QueryConformanceClasses.COLLECTIONS]),
            SortExtension(conformance_classes=[SortConformanceClasses.COLLECTIONS]),
            FieldsExtension(conformance_classes=[FieldsConformanceClasses.COLLECTIONS]),
            CollectionSearchFilterExtension(
                conformance_classes=[FilterConformanceClasses.COLLECTIONS]
            ),
            FreeTextExtension(
                conformance_classes=[FreeTextConformanceClasses.COLLECTIONS]
            ),
        ]

    @cached_property
    def collection_search_extension(self) -> CollectionSearchExtension | None:
        """Return collection search extension when any related feature flag is enabled."""
        if not (
            self.collections_search_enabled or self.collections_search_route_enabled
        ):
            return None
        return CollectionSearchExtension.from_extensions(self.collections_search_base)

    @cached_property
    def collection_search_post_request_model(self) -> Any | None:
        """POST request model for collections search when that feature is enabled."""
        if not (
            self.collections_search_enabled or self.collections_search_route_enabled
        ):
            return None
        return create_post_request_model(self.collections_search_base)

    @cached_property
    def collections_get_request_model(self) -> Any | None:
        """GET request model for collections search derived from extension metadata."""
        if self.collection_search_extension is None:
            return None
        return self.collection_search_extension.GET

    @cached_property
    def collection_search(self) -> list[ApiExtension]:
        """Return collection search extensions attached to standard STAC API routes."""
        if not self.collections_search_enabled:
            return []
        if (
            self.collection_search_extension is None
            or self.collection_search_post_request_model is None
        ):
            return []

        return [
            self.collection_search_extension,
            CollectionSearchPostExtension(
                client=CoreClient(
                    database=self.database_logic,
                    session=self.session,
                    post_request_model=self.collection_search_post_request_model,
                    landing_page_id=os.getenv(
                        "STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"
                    ),
                ),
                settings=self.settings,
                POST=self.collection_search_post_request_model,
                conformance_classes=[
                    "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
                    QueryConformanceClasses.COLLECTIONS,
                    FilterConformanceClasses.COLLECTIONS,
                    FreeTextConformanceClasses.COLLECTIONS,
                    SortConformanceClasses.COLLECTIONS,
                    FieldsConformanceClasses.COLLECTIONS,
                ],
            ),
        ]

    @cached_property
    def collections_search_route(self) -> list[ApiExtension]:
        """Dedicated `/collections-search` endpoint extension set, when enabled."""
        if not self.collections_search_route_enabled:
            return []
        if (
            self.collections_get_request_model is None
            or self.collection_search_post_request_model is None
        ):
            return []

        return [
            CollectionsSearchEndpointExtension(
                client=CoreClient(
                    database=self.database_logic,
                    session=self.session,
                    post_request_model=self.collection_search_post_request_model,
                    landing_page_id=os.getenv(
                        "STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"
                    ),
                ),
                settings=self.settings,
                GET=self.collections_get_request_model,
                POST=self.collection_search_post_request_model,
                conformance_classes=[
                    "https://api.stacspec.org/v1.0.0-rc.1/collection-search",
                    QueryConformanceClasses.COLLECTIONS,
                    FilterConformanceClasses.COLLECTIONS,
                    FreeTextConformanceClasses.COLLECTIONS,
                    SortConformanceClasses.COLLECTIONS,
                    FieldsConformanceClasses.COLLECTIONS,
                ],
            )
        ]

    @cached_property
    def catalogs(self) -> list[ApiExtension]:
        """Multi-tenant catalogs extensions when dependency and flag are available."""
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

        return [
            CatalogsExtension(
                client=catalogs_client,
                settings=self.settings.model_dump(),
                hide_alternate_parents=self.hide_alternate_parents,
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
