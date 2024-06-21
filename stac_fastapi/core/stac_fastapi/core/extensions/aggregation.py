"""Request model for the Aggregation extension."""

import json
import re
from typing import Dict, List, Optional, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Request
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic.shared import BBox

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
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
from stac_fastapi.types.rfc3339 import DateTimeType


@attr.s
class OpenSearchAggregationExtensionGetRequest(AggregationExtensionGetRequest):
    """Add implementation specific query parameters to AggregationExtensionGetRequest for aggrgeation precision."""

    grid_geohex_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geohash_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geotile_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)


@attr.s
class OpenSearchAggregationExtensionPostRequest(AggregationExtensionPostRequest):
    """Add implementation specific query parameters to AggregationExtensionPostRequest for aggrgeation precision."""

    grid_geohex_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geohash_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geotile_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)


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
    ]

    ALL_AGGREGATION_NAMES = [agg["name"] for agg in DEFAULT_AGGREGATIONS] + [
        "collection_frequency",
        "grid_code_frequency",
        "grid_geohash_frequency",
        "grid_geohex_frequency",
        "grid_geotile_frequency",
        "centroid_geohash_grid_frequency",
        "centroid_geohex_grid_frequency",
        "centroid_geotile_grid_frequency",
        "geometry_geohash_grid_frequency",
        # 'geometry_geohex_grid_frequency',
        "geometry_geotile_grid_frequency",
        "platform_frequency",
        "sun_elevation_frequency",
        "sun_azimuth_frequency",
        "off_nadir_frequency",
        "cloud_cover_frequency",
    ]

    MAX_GEOHASH_PRECISION = 12
    MAX_GEOHEX_PRECISION = 15
    MAX_GEOTILE_PRECISION = 29

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
                        "href": urljoin(collection_endpoint, "aggregations"),
                    },
                ]
            )
            if self.database.check_collection_exists(collection_id):
                collection = await self.database.find_collection(collection_id)
                aggregations = collection.get(
                    "aggregations", self.DEFAULT_AGGREGATIONS.copy()
                )
            else:
                raise IndexError("Collection does not exist")
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

    def extract_aggregations(
        self, aggregations_value: Union[str, List[str]]
    ) -> List[str]:
        """Parse the aggregations from a comma separated string or a list of strings."""
        if aggregations_value:
            try:
                if isinstance(aggregations_value, str):
                    if "," in aggregations_value:
                        aggs = aggregations_value.split(",")
                    else:
                        aggs = [aggregations_value]
                else:
                    aggs = list(aggregations_value)
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(
                    status_code=400, detail="Invalid aggregations value"
                )

            return aggs
        return []

    def extract_ids(self, ids_value: Union[str, List[str]]) -> List[str]:
        """Parse item ids from a comma separated string or a list of strings."""
        if ids_value:
            try:
                if isinstance(ids_value, str):
                    if "," in ids_value:
                        ids_rules = ids_value.split(",")
                    else:
                        ids_rules = [ids_value]
                else:
                    ids_rules = list(ids_value)
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(status_code=400, detail="Invalid ids value")

            return ids_rules
        else:
            return []

    # TEST NUMBER OUTSIDE OF RANGE
    # TEST NONE PRECISION
    def extract_precision(
        self, precision: int, min_value: int, max_value: int
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

    def agg(self, es_aggs, name, data_type):
        """Format aggregations results Buckets."""
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

    async def aggregate(
        self,
        collection_id: Optional[str] = None,
        collections: Optional[List[str]] = None,
        datetime: Optional[DateTimeType] = None,
        intersects: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        aggregations: Optional[str] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        grid_geohex_frequency_precision: Optional[int] = None,
        grid_geohash_frequency_precision: Optional[int] = None,
        grid_geotile_frequency_precision: Optional[int] = None,
        centroid_geohash_grid_frequency_precision: Optional[int] = None,
        centroid_geohex_grid_frequency_precision: Optional[int] = None,
        centroid_geotile_grid_frequency_precision: Optional[int] = None,
        geometry_geohash_grid_frequency_precision: Optional[int] = None,
        geometry_geotile_grid_frequency_precision: Optional[int] = None,
        **kwargs,
    ) -> Union[Dict, Exception]:
        """Get aggregations from the database."""
        request: Request = kwargs["request"]
        base_url = str(request.base_url)
        search = self.database.make_search()

        # this is borrowed from stac-fastapi-pgstac
        # Kludgy fix because using factory does not allow alias for filter-lan
        query_params = str(request.query_params)
        if filter_lang is None:
            match = re.search(r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE)
            if match:
                filter_lang = match.group(1)

        if bbox and intersects:
            raise ValueError("Expected bbox OR intersects, not both")

        if intersects:
            intersects_geometry = orjson.loads(unquote_plus(intersects))
            search = self.database.apply_intersects_filter(
                search=search, intersects=intersects_geometry
            )

        if bbox:
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if collection_id:
            collection_endpoint = urljoin(base_url, f"collections/{collection_id}")

            if self.database.check_collection_exists(collection_id):
                collection = await self.database.find_collection(collection_id)
                search = self.database.apply_collections_filter(
                    search=search, collection_ids=[collection_id]
                )
            if isinstance(collection, Exception):
                return collection

        elif collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=self.extract_collection_ids(collections)
            )

        if datetime:
            datetime_search = self._format_datetime_range(datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if ids:
            ids = self.extract_ids(ids)
            search = self.database.apply_ids_filter(search=search, item_ids=ids)

        if filter:
            # filter_lang = "cql2-json"
            filter = orjson.loads(
                unquote_plus(filter)
                if filter_lang == "cql2-json"
                else to_cql2(parse_cql2_text(filter))
            )
            try:
                search = self.database.apply_cql2_filter(search, filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        aggregations_requested = self.extract_aggregations(aggregations)

        # validate that aggregations are supported by collection
        # if aggregations are not defined for a collection, any aggregation may be requested
        if collection and collection.get("aggregations"):
            supported_aggregations = [x.name for x in collection.get("aggregations")]
            for agg_name in aggregations_requested:
                if agg_name not in supported_aggregations:
                    raise HTTPException(
                        status_code=415,
                        detail=f"Aggregation {agg_name} not supported by collection {collection_id}",
                    )
        else:
            for agg_name in aggregations_requested:
                if agg_name not in self.ALL_AGGREGATION_NAMES:
                    raise HTTPException(
                        status_code=415,
                        detail=f"Aggregation {agg_name} not supported at catalog level",
                    )

        geohash_precision = self.extract_precision(
            grid_geohash_frequency_precision, 1, self.MAX_GEOHASH_PRECISION
        )

        geohex_precision = self.extract_precision(
            grid_geohex_frequency_precision, 0, self.MAX_GEOHEX_PRECISION
        )

        geotile_precision = self.extract_precision(
            grid_geotile_frequency_precision, 0, self.MAX_GEOTILE_PRECISION
        )

        centroid_geohash_grid_precision = self.extract_precision(
            centroid_geohash_grid_frequency_precision, 1, self.MAX_GEOHASH_PRECISION
        )

        centroid_geohex_grid_precision = self.extract_precision(
            centroid_geohex_grid_frequency_precision, 0, self.MAX_GEOHEX_PRECISION
        )

        centroid_geotile_grid_precision = self.extract_precision(
            centroid_geotile_grid_frequency_precision, 0, self.MAX_GEOTILE_PRECISION
        )

        geometry_geohash_grid_precision = self.extract_precision(
            geometry_geohash_grid_frequency_precision, 1, self.MAX_GEOHASH_PRECISION
        )

        # geometry_geohex_grid_frequency_precision = self.extract_precision(
        #     geometry_geohex_grid_frequency_precision,
        #     0,
        #     max_geohex_precision
        # )

        geometry_geotile_grid_precision = self.extract_precision(
            geometry_geotile_grid_frequency_precision, 0, self.MAX_GEOTILE_PRECISION
        )

        try:
            db_response = await self.database.aggregate(
                collection_id,
                aggregations_requested,
                search,
                geohash_precision,
                geohex_precision,
                geotile_precision,
                centroid_geohash_grid_precision,
                centroid_geohex_grid_precision,
                centroid_geotile_grid_precision,
                geometry_geohash_grid_precision,
                # geometry_geohex_grid_precision,
                geometry_geotile_grid_precision,
            )
        except Exception as error:
            if not isinstance(error, IndexError):
                raise error
        aggs = []
        if db_response:
            result_aggs = db_response.get("aggregations")

            if "total_count" in aggregations_requested:
                aggs.append(
                    Aggregation(
                        name="total_count",
                        data_type="integer",
                        value=result_aggs.get("total_count", {}).get("value", None),
                    )
                )

            if "datetime_max" in aggregations_requested:
                aggs.append(
                    Aggregation(
                        name="datetime_max",
                        data_type="datetime",
                        value=result_aggs.get("datetime_max", {}).get(
                            "value_as_string", None
                        ),
                    )
                )

            if "datetime_min" in aggregations_requested:
                aggs.append(
                    Aggregation(
                        name="datetime_min",
                        data_type="datetime",
                        value=result_aggs.get("datetime_min", {}).get(
                            "value_as_string", None
                        ),
                    )
                )

            other_aggregations = {
                "collection_frequency": "string",
                "grid_code_frequency": "string",
                "grid_geohash_frequency": "string",
                "grid_geohex_frequency": "string",
                "grid_geotile_frequency": "string",
                "centroid_geohash_grid_frequency": "string",
                "centroid_geohex_grid_frequency": "string",
                "centroid_geotile_grid_frequency": "string",
                "geometry_geohash_grid_frequency": "string",
                "geometry_geotile_grid_frequency": "string",
                "platform_frequency": "string",
                "sun_elevation_frequency": "string",
                "sun_azimuth_frequency": "string",
                "off_nadir_frequency": "string",
                "datetime_frequency": "datetime",
                "cloud_cover_frequency": "numeric",
            }

            for agg_name, data_type in other_aggregations.items():
                if agg_name in aggregations_requested:
                    aggs.append(self.agg(result_aggs, agg_name, data_type))
        links = [
            {
                "rel": "self",
                "type": "application/json",
                "href": urljoin(base_url, "aggregate"),
            },
            {"rel": "root", "type": "application/json", "href": base_url},
        ]
        if collection_endpoint:
            links.append(
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": collection_endpoint,
                }
            )
        results = AggregationCollection(
            type="AggregationCollection", aggregations=aggs, links=links
        )

        return results
