"""Request model for the Aggregation extension."""

from typing import Literal, Optional

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

    collection_id: Optional[
        Annotated[str, Path(description="Collection ID")]
    ] = attr.ib(default=None)

    centroid_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    datetime_frequency_interval: Optional[str] = attr.ib(default=None)


class EsAggregationExtensionPostRequest(
    AggregationExtensionPostRequest, FilterExtensionPostRequest
):
    """Implementation specific query parameters for aggregation precision."""

    centroid_geohash_grid_frequency_precision: Optional[int] = None
    centroid_geohex_grid_frequency_precision: Optional[int] = None
    centroid_geotile_grid_frequency_precision: Optional[int] = None
    geometry_geohash_grid_frequency_precision: Optional[int] = None
    geometry_geotile_grid_frequency_precision: Optional[int] = None
    datetime_frequency_interval: Optional[str] = None
