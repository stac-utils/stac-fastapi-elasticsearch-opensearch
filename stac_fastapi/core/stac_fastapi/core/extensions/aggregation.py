"""Request model for the Aggregation extension."""

from typing import Literal

import attr
from fastapi import Path
from typing_extensions import Annotated

from stac_fastapi.extensions.core.aggregation.request import (
    AggregationExtensionGetRequest,
    AggregationExtensionPostRequest,
)
from stac_fastapi.extensions.core.filter.request import (
    FilterExtensionGetRequest,
    FilterExtensionPostRequest,
)

FilterLang = Literal["cql-json", "cql2-json", "cql2-text"]


@attr.s
class EsAggregationExtensionGetRequest(
    AggregationExtensionGetRequest, FilterExtensionGetRequest
):
    """Implementation specific query parameters for aggregation precision."""

    collection_id: Annotated[str, Path(description="Collection ID")] | None = attr.ib(
        default=None
    )

    centroid_geohash_grid_frequency_precision: int | None = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: int | None = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: int | None = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: int | None = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: int | None = attr.ib(default=None)
    datetime_frequency_interval: str | None = attr.ib(default=None)


class EsAggregationExtensionPostRequest(
    AggregationExtensionPostRequest, FilterExtensionPostRequest
):
    """Implementation specific query parameters for aggregation precision."""

    centroid_geohash_grid_frequency_precision: int | None = None
    centroid_geohex_grid_frequency_precision: int | None = None
    centroid_geotile_grid_frequency_precision: int | None = None
    geometry_geohash_grid_frequency_precision: int | None = None
    geometry_geotile_grid_frequency_precision: int | None = None
    datetime_frequency_interval: str | None = None
