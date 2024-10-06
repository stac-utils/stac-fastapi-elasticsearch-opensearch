"""Request model for the Aggregation extension."""

from datetime import datetime
from datetime import datetime as datetime_type
from typing import Dict, List, Literal, Optional, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Path, Request
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic.shared import BBox
from typing_extensions import Annotated

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.datetime_utils import datetime_to_str
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core.aggregation.client import AsyncBaseAggregationClient
from stac_fastapi.extensions.core.aggregation.request import (
    AggregationExtensionGetRequest,
    AggregationExtensionPostRequest,
)
from stac_fastapi.extensions.core.aggregation.types import (
    Aggregation,
    AggregationCollection,
)
from stac_fastapi.extensions.core.filter.request import (
    FilterExtensionGetRequest,
    FilterExtensionPostRequest,
)
from stac_fastapi.types.rfc3339 import DateTimeType

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


@attr.s
class EsAsyncAggregationClient(AsyncBaseAggregationClient):
    """Defines a pattern for implementing the STAC aggregation extension."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

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

    GEO_POINT_AGGREGATIONS = [
        {
            "name": "grid_code_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        {
            "name": "centroid_geohash_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        {
            "name": "centroid_geohex_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        {
            "name": "centroid_geotile_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
    ]

    MAX_GEOHASH_PRECISION = 12
    MAX_GEOHEX_PRECISION = 15
    MAX_GEOTILE_PRECISION = 29
    SUPPORTED_DATETIME_INTERVAL = {"day", "month", "year"}
    DEFAULT_DATETIME_INTERVAL = "month"

    async def get_aggregations(self, collection_id: Optional[str] = None, **kwargs):
        """Get the available aggregations for a catalog or collection defined in the STAC JSON. If no aggregations, default aggregations are used."""
        request: Request = kwargs["request"]
        base_url = str(request.base_url)
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

            aggregations = self.DEFAULT_AGGREGATIONS
        return AggregationCollection(
            type="AggregationCollection", aggregations=aggregations, links=links
        )

    def extract_precision(
        self, precision: Union[int, None], min_value: int, max_value: int
    ) -> Optional[int]:
        """Ensure that the aggregation precision value is withing the a valid range, otherwise return the minumium value."""
        if precision is not None:
            if precision < min_value or precision > max_value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid precision. Must be a number between {min_value} and {max_value} inclusive",
                )
            return precision
        else:
            return min_value

    def extract_date_histogram_interval(self, value: Optional[str]) -> str:
        """
        Ensure that the interval for the date histogram is valid. If no value is provided, the default will be returned.

        Args:
            value: value entered by the user

        Returns:
            string containing the date histogram interval to use.

        Raises:
            HTTPException: if the supplied value is not in the supported intervals
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

    @staticmethod
    def _return_date(
        interval: Optional[Union[DateTimeType, str]]
    ) -> Dict[str, Optional[str]]:
        """
        Convert a date interval.

        (which may be a datetime, a tuple of one or two datetimes a string
        representing a datetime or range, or None) into a dictionary for filtering
        search results with Elasticsearch.

        This function ensures the output dictionary contains 'gte' and 'lte' keys,
        even if they are set to None, to prevent KeyError in the consuming logic.

        Args:
            interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
                a tuple with one or two datetimes, a string, or None.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results,
                always containing 'gte' and 'lte' keys.
        """
        result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

        if interval is None:
            return result

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = parts[0] if parts[0] != ".." else None
                result["lte"] = (
                    parts[1] if len(parts) > 1 and parts[1] != ".." else None
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            datetime_iso = interval.isoformat()
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                result["gte"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if end:
                result["lte"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        return result

    def frequency_agg(self, es_aggs, name, data_type):
        """Format an aggregation for a frequency distribution aggregation."""
        buckets = []
        for bucket in es_aggs.get(name, {}).get("buckets", []):
            bucket_data = {
                "key": bucket.get("key_as_string") or bucket.get("key"),
                "data_type": data_type,
                "frequency": bucket.get("doc_count"),
                "to": bucket.get("to"),
                "from": bucket.get("from"),
            }
            buckets.append(bucket_data)
        return Aggregation(
            name=name,
            data_type="frequency_distribution",
            overflow=es_aggs.get(name, {}).get("sum_other_doc_count", 0),
            buckets=buckets,
        )

    def metric_agg(self, es_aggs, name, data_type):
        """Format an aggregation for a metric aggregation."""
        value = es_aggs.get(name, {}).get("value_as_string") or es_aggs.get(
            name, {}
        ).get("value")
        # ES 7.x does not return datetimes with a 'value_as_string' field
        if "datetime" in name and isinstance(value, float):
            value = datetime_to_str(datetime.fromtimestamp(value / 1e3))
        return Aggregation(
            name=name,
            data_type=data_type,
            value=value,
        )

    def get_filter(self, filter, filter_lang):
        """Format the filter parameter in cql2-json or cql2-text."""
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

    def _format_datetime_range(self, date_tuple: DateTimeType) -> str:
        """
        Convert a tuple of datetime objects or None into a formatted string for API requests.

        Args:
            date_tuple (tuple): A tuple containing two elements, each can be a datetime object or None.

        Returns:
            str: A string formatted as 'YYYY-MM-DDTHH:MM:SS.sssZ/YYYY-MM-DDTHH:MM:SS.sssZ', with '..' used if any element is None.
        """

        def format_datetime(dt):
            """Format a single datetime object to the ISO8601 extended format with 'Z'."""
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if dt else ".."

        start, end = date_tuple
        return f"{format_datetime(start)}/{format_datetime(end)}"

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
        filter: Optional[str] = None,
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
                base_args["datetime"] = self._format_datetime_range(datetime)

            if filter:
                base_args["filter"] = self.get_filter(filter, filter_lang)
            aggregate_request = EsAggregationExtensionPostRequest(**base_args)
        else:
            # Workaround for optional path param in POST requests
            if "collections" in path:
                collection_id = path.split("/")[2]

            filter_lang = "cql2-json"
            if aggregate_request.filter:
                aggregate_request.filter = self.get_filter(
                    aggregate_request.filter, filter_lang
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
            datetime_search = self._return_date(aggregate_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

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
                aggs = await self.get_aggregations(
                    collection_id=collection_id, request=request
                )
                supported_aggregations = (
                    aggs["aggregations"] + self.DEFAULT_AGGREGATIONS
                )

                for agg_name in aggregate_request.aggregations:
                    if agg_name not in set([x["name"] for x in supported_aggregations]):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Aggregation {agg_name} not supported by collection {collection_id}",
                        )
        else:
            # Validate that the aggregations requested are supported by the catalog
            aggs = await self.get_aggregations(request=request)
            supported_aggregations = aggs["aggregations"]
            for agg_name in aggregate_request.aggregations:
                if agg_name not in [x["name"] for x in supported_aggregations]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Aggregation {agg_name} not supported at catalog level",
                    )

        if aggregate_request.filter:
            try:
                search = self.database.apply_cql2_filter(
                    search, aggregate_request.filter
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
            )
        except Exception as error:
            if not isinstance(error, IndexError):
                raise error
        aggs = []
        if db_response:
            result_aggs = db_response.get("aggregations", {})
            for agg in {
                frozenset(item.items()): item
                for item in supported_aggregations + self.GEO_POINT_AGGREGATIONS
            }.values():
                if agg["name"] in aggregate_request.aggregations:
                    if agg["name"].endswith("_frequency"):
                        aggs.append(
                            self.frequency_agg(
                                result_aggs, agg["name"], agg["data_type"]
                            )
                        )
                    else:
                        aggs.append(
                            self.metric_agg(result_aggs, agg["name"], agg["data_type"])
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
