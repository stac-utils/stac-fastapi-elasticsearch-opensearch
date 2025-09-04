"""Client implementation for the STAC API Aggregation Extension."""

from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Request
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic.shared import BBox

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.datetime_utils import format_datetime_range
from stac_fastapi.core.extensions.aggregation import EsAggregationExtensionPostRequest
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core.aggregation.client import AsyncBaseAggregationClient
from stac_fastapi.extensions.core.aggregation.types import (
    Aggregation,
    AggregationCollection,
)
from stac_fastapi.types.rfc3339 import DateTimeType

from .format import frequency_agg, metric_agg


@attr.s
class EsAsyncBaseAggregationClient(AsyncBaseAggregationClient):
    """Defines a pattern for implementing the STAC aggregation extension with Elasticsearch/OpenSearch."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    # Default aggregations to use if none are specified
    DEFAULT_AGGREGATIONS = [
        {"name": "total_count", "data_type": "integer"},
        {"name": "datetime_max", "data_type": "datetime"},
        {"name": "datetime_min", "data_type": "datetime"},
        {
            "name": "datetime_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "datetime",
        },
        {
            "name": "collection_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        {
            "name": "geometry_geohash_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        {
            "name": "geometry_geotile_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
    ]

    # Geo point aggregations
    GEO_POINT_AGGREGATIONS = [
        {
            "name": "grid_code_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
    ]

    # Supported datetime intervals
    SUPPORTED_DATETIME_INTERVAL = [
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "hour",
        "minute",
        "second",
    ]

    # Default datetime interval
    DEFAULT_DATETIME_INTERVAL = "month"

    # Maximum precision values
    MAX_GEOHASH_PRECISION = 12
    MAX_GEOHEX_PRECISION = 15
    MAX_GEOTILE_PRECISION = 29

    async def get_aggregations(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the available aggregations for a catalog or collection defined in the STAC JSON.

        If no aggregations are defined, default aggregations are used.

        Args:
            collection_id: Optional collection ID to get aggregations for
            **kwargs: Additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the available aggregations
        """
        request: Request = kwargs.get("request")
        base_url = str(request.base_url) if request else ""
        links = [{"rel": "root", "type": "application/json", "href": base_url}]

        if collection_id is not None:
            collection_endpoint = urljoin(base_url, f"collections/{collection_id}")
            links.extend(
                [
                    {
                        "rel": "collection",
                        "type": "application/json",
                        "href": collection_endpoint,
                    },
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": urljoin(collection_endpoint + "/", "aggregations"),
                    },
                ]
            )
            if await self.database.check_collection_exists(collection_id) is None:
                collection = await self.database.find_collection(collection_id)
                aggregations = collection.get(
                    "aggregations", self.DEFAULT_AGGREGATIONS.copy()
                )
            else:
                raise IndexError(f"Collection {collection_id} does not exist")
        else:
            links.append(
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": urljoin(base_url, "aggregations"),
                }
            )
            aggregations = self.DEFAULT_AGGREGATIONS.copy()

        return {
            "type": "AggregationCollection",
            "aggregations": aggregations,
            "links": links,
        }

    def extract_precision(
        self, precision: Union[int, None], min_value: int, max_value: int
    ) -> int:
        """Ensure that the aggregation precision value is within a valid range.

        Args:
            precision: The precision value to validate
            min_value: The minimum allowed precision value
            max_value: The maximum allowed precision value

        Returns:
            int: A validated precision value

        Raises:
            HTTPException: If the precision is outside the valid range
        """
        if precision is None:
            return min_value
        if precision < min_value or precision > max_value:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid precision value. Must be between {min_value} and {max_value}",
            )
        return precision

    def extract_date_histogram_interval(self, value: Optional[str]) -> str:
        """Ensure that the interval for the date histogram is valid.

        If no value is provided, the default will be returned.

        Args:
            value: The interval value to validate

        Returns:
            str: A validated date histogram interval

        Raises:
            HTTPException: If the supplied value is not in the supported intervals
        """
        if value is not None:
            if value not in self.SUPPORTED_DATETIME_INTERVAL:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid datetime interval. Must be one of {self.SUPPORTED_DATETIME_INTERVAL}",
                )
            else:
                return value
        else:
            return self.DEFAULT_DATETIME_INTERVAL

    def get_filter(self, filter, filter_lang):
        """Format the filter parameter in cql2-json or cql2-text.

        Args:
            filter: The filter expression
            filter_lang: The filter language (cql2-json or cql2-text)

        Returns:
            dict: A formatted filter expression

        Raises:
            HTTPException: If the filter language is not supported
        """
        if filter_lang == "cql2-text":
            return orjson.loads(to_cql2(parse_cql2_text(filter)))
        elif filter_lang == "cql2-json":
            if isinstance(filter, str):
                return orjson.loads(unquote_plus(filter))
            else:
                return filter
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown filter-lang: {filter_lang}. Only cql2-json or cql2-text are supported.",
            )

    async def aggregate(
        self,
        aggregate_request: Optional[EsAggregationExtensionPostRequest] = None,
        collection_id: Optional[
            Annotated[str, Path(description="Collection ID")]
        ] = None,
        collections: Optional[List[str]] = [],
        datetime: Optional[DateTimeType] = None,
        intersects: Optional[str] = None,
        filter_lang: Optional[str] = None,
        filter_expr: Optional[str] = None,
        aggregations: Optional[str] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        centroid_geohash_grid_frequency_precision: Optional[int] = None,
        centroid_geohex_grid_frequency_precision: Optional[int] = None,
        centroid_geotile_grid_frequency_precision: Optional[int] = None,
        geometry_geohash_grid_frequency_precision: Optional[int] = None,
        geometry_geotile_grid_frequency_precision: Optional[int] = None,
        datetime_frequency_interval: Optional[str] = None,
        **kwargs,
    ) -> Union[Dict, Exception]:
        """Get aggregations from the database."""
        request: Request = kwargs["request"]
        base_url = str(request.base_url)
        path = request.url.path
        search = self.database.make_search()

        if aggregate_request is None:

            base_args = {
                "collections": collections,
                "ids": ids,
                "bbox": bbox,
                "aggregations": aggregations,
                "centroid_geohash_grid_frequency_precision": centroid_geohash_grid_frequency_precision,
                "centroid_geohex_grid_frequency_precision": centroid_geohex_grid_frequency_precision,
                "centroid_geotile_grid_frequency_precision": centroid_geotile_grid_frequency_precision,
                "geometry_geohash_grid_frequency_precision": geometry_geohash_grid_frequency_precision,
                "geometry_geotile_grid_frequency_precision": geometry_geotile_grid_frequency_precision,
                "datetime_frequency_interval": datetime_frequency_interval,
            }

            if collection_id:
                collections = [str(collection_id)]

            if intersects:
                base_args["intersects"] = orjson.loads(unquote_plus(intersects))

            if datetime:
                base_args["datetime"] = format_datetime_range(datetime)

            if filter_expr:
                base_args["filter"] = self.get_filter(filter_expr, filter_lang)
            aggregate_request = EsAggregationExtensionPostRequest(**base_args)
        else:
            # Workaround for optional path param in POST requests
            if "collections" in path:
                collection_id = path.split("/")[2]

            filter_lang = "cql2-json"
            if aggregate_request.filter_expr:
                aggregate_request.filter_expr = self.get_filter(
                    aggregate_request.filter_expr, filter_lang
                )

        if collection_id:
            if aggregate_request.collections:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot query multiple collections when executing '/collections/<collection_id>/aggregate'. Use '/aggregate' and the collections field instead",
                )
            else:
                aggregate_request.collections = [collection_id]

        if (
            aggregate_request.aggregations is None
            or aggregate_request.aggregations == []
        ):
            raise HTTPException(
                status_code=400,
                detail="No 'aggregations' found. Use '/aggregations' to return available aggregations",
            )

        if aggregate_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=aggregate_request.ids
            )

        if aggregate_request.datetime:
            search, datetime_search = self.database.apply_datetime_filter(
                search=search, datetime=aggregate_request.datetime
            )
        else:
            datetime_search = {"gte": None, "lte": None}

        if aggregate_request.bbox:
            bbox = aggregate_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if aggregate_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=aggregate_request.intersects
            )

        if aggregate_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=aggregate_request.collections
            )
            # validate that aggregations are supported for all collections
            for collection_id in aggregate_request.collections:
                aggregation_info = await self.get_aggregations(
                    collection_id=collection_id, request=request
                )
                supported_aggregations = (
                    aggregation_info["aggregations"] + self.DEFAULT_AGGREGATIONS
                )

                for agg_name in aggregate_request.aggregations:
                    if agg_name not in set([x["name"] for x in supported_aggregations]):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Aggregation {agg_name} not supported by collection {collection_id}",
                        )
        else:
            # Validate that the aggregations requested are supported by the catalog
            aggregation_info = await self.get_aggregations(request=request)
            supported_aggregations = aggregation_info["aggregations"]
            for agg_name in aggregate_request.aggregations:
                if agg_name not in [x["name"] for x in supported_aggregations]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Aggregation {agg_name} not supported at catalog level",
                    )

        if aggregate_request.filter_expr:
            try:
                search = await self.database.apply_cql2_filter(
                    search, aggregate_request.filter_expr
                )
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2 filter: {e}"
                )

        centroid_geohash_grid_precision = self.extract_precision(
            aggregate_request.centroid_geohash_grid_frequency_precision,
            1,
            self.MAX_GEOHASH_PRECISION,
        )

        centroid_geohex_grid_precision = self.extract_precision(
            aggregate_request.centroid_geohex_grid_frequency_precision,
            0,
            self.MAX_GEOHEX_PRECISION,
        )

        centroid_geotile_grid_precision = self.extract_precision(
            aggregate_request.centroid_geotile_grid_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
        )

        geometry_geohash_grid_precision = self.extract_precision(
            aggregate_request.geometry_geohash_grid_frequency_precision,
            1,
            self.MAX_GEOHASH_PRECISION,
        )

        geometry_geotile_grid_precision = self.extract_precision(
            aggregate_request.geometry_geotile_grid_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
        )

        datetime_frequency_interval = self.extract_date_histogram_interval(
            aggregate_request.datetime_frequency_interval,
        )

        try:
            db_response = await self.database.aggregate(
                collections,
                aggregate_request.aggregations,
                search,
                centroid_geohash_grid_precision,
                centroid_geohex_grid_precision,
                centroid_geotile_grid_precision,
                geometry_geohash_grid_precision,
                geometry_geotile_grid_precision,
                datetime_frequency_interval,
                datetime_search,
            )
        except Exception as error:
            if not isinstance(error, IndexError):
                raise error
        aggs: List[Aggregation] = []
        if db_response:
            result_aggs = db_response.get("aggregations", {})
            for agg in {
                frozenset(item.items()): item
                for item in supported_aggregations + self.GEO_POINT_AGGREGATIONS
            }.values():
                if agg["name"] in aggregate_request.aggregations:
                    if agg["name"].endswith("_frequency"):
                        aggs.append(
                            frequency_agg(result_aggs, agg["name"], agg["data_type"])
                        )
                    else:
                        aggs.append(
                            metric_agg(result_aggs, agg["name"], agg["data_type"])
                        )
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
        ]

        if collection_id:
            collection_endpoint = urljoin(base_url, f"collections/{collection_id}")
            links.extend(
                [
                    {
                        "rel": "collection",
                        "type": "application/json",
                        "href": collection_endpoint,
                    },
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": urljoin(collection_endpoint, "aggregate"),
                    },
                ]
            )
        else:
            links.append(
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": urljoin(base_url, "aggregate"),
                }
            )
        results = AggregationCollection(
            type="AggregationCollection", aggregations=aggs, links=links
        )

        return results
