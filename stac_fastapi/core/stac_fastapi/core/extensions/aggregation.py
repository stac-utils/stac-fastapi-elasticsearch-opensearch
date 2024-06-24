"""Request model for the Aggregation extension."""

import json
import re
from typing import Dict, List, Literal, Optional, Union
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

FilterLang = Literal["cql-json", "cql2-json", "cql2-text"]


@attr.s
class OpenSearchAggregationExtensionGetRequest(AggregationExtensionGetRequest):
    """Add implementation specific query parameters to AggregationExtensionGetRequest for aggrgeation precision."""

    filter_lang: Optional[FilterLang] = attr.ib(default="cql2-text")

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

    grid_geohex_frequency_precision: Optional[int] = None
    grid_geohash_frequency_precision: Optional[int] = None
    grid_geotile_frequency_precision: Optional[int] = None
    centroid_geohash_grid_frequency_precision: Optional[int] = None
    centroid_geohex_grid_frequency_precision: Optional[int] = None
    centroid_geotile_grid_frequency_precision: Optional[int] = None
    geometry_geohash_grid_frequency_precision: Optional[int] = None
    geometry_geotile_grid_frequency_precision: Optional[int] = None


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
                    aggs = [aggregations_value]
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(
                    status_code=400, detail="Invalid aggregations value"
                )

            return aggs
        return []

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

    async def aggregate(
        self,
        search_request: Optional[OpenSearchAggregationExtensionPostRequest] = None,
        collections: Optional[List[str]] = [],
        datetime: Optional[DateTimeType] = None,
        intersects: Optional[str] = None,
        filter_lang: Optional[FilterLang] = None,
        filter: Optional[str] = None,
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

        if search_request is None:
            # this is borrowed from stac-fastapi-pgstac
            # Kludgy fix because using factory does not allow alias for filter-lang
            # If the value is the default, check if the request is different.
            query_params = str(request.query_params)
            if filter_lang == "cql2-text":
                match = re.search(
                    r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE
                )
                if match and match.group(1) != "cql2-text":
                    filter_lang = match.group(1)

            search_request = OpenSearchAggregationExtensionGetRequest(
                collections=",".join(collections) if collections else None,
                datetime=datetime,
                intersects=intersects,
                filter=filter,
                filter_lang=filter_lang,
                aggregations=aggregations,
                ids=ids,
                bbox=bbox,
                grid_geohex_frequency_precision=grid_geohex_frequency_precision,
                grid_geohash_frequency_precision=grid_geohash_frequency_precision,
                grid_geotile_frequency_precision=grid_geotile_frequency_precision,
                centroid_geohash_grid_frequency_precision=centroid_geohash_grid_frequency_precision,
                centroid_geohex_grid_frequency_precision=centroid_geohex_grid_frequency_precision,
                centroid_geotile_grid_frequency_precision=centroid_geotile_grid_frequency_precision,
                geometry_geohash_grid_frequency_precision=geometry_geohash_grid_frequency_precision,
                geometry_geotile_grid_frequency_precision=geometry_geotile_grid_frequency_precision,
            )

        if search_request.aggregations:
            aggregations_requested = self.extract_aggregations(
                search_request.aggregations
            )
        else:
            aggregations_requested = []

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )
            # validate that aggregations are supported for all collections
            # if aggregations are not defined for a collection, any aggregation may be requested
            for collection_id in search_request.collections:
                if self.database.check_collection_exists(collection_id):
                    collection = await self.database.find_collection(collection_id)
                if isinstance(collection, Exception):
                    return collection

                if (
                    collection
                    and collection.get("aggregations")
                    and aggregations_requested
                ):
                    supported_aggregations = [
                        x["name"] for x in collection.get("aggregations")
                    ]
                    for agg_name in aggregations_requested:
                        if agg_name not in supported_aggregations:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Aggregation {agg_name} not supported by collection {collection_id}",
                            )
                else:
                    for agg_name in aggregations_requested:
                        if agg_name not in self.ALL_AGGREGATION_NAMES:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Aggregation {agg_name} not supported at catalog level",
                            )

        if search_request.filter:
            if search_request.filter_lang == "cql2-text":
                search_request.filter = orjson.loads(
                    unquote_plus(to_cql2(parse_cql2_text(search_request.filter)))
                )
            elif search_request.filter_lang == "cql2-json":
                if isinstance(search_request.filter, str):
                    search_request.filter = orjson.loads(
                        unquote_plus(search_request.filter)
                    )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown filter-lang: {search_request.filter_lang}. Filter-lang or cql2-json and cql2-text are supported.",
                )
            try:
                search = self.database.apply_cql2_filter(search, search_request.filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        geohash_precision = self.extract_precision(
            search_request.grid_geohash_frequency_precision,
            1,
            self.MAX_GEOHASH_PRECISION,
        )

        geohex_precision = self.extract_precision(
            search_request.grid_geohex_frequency_precision, 0, self.MAX_GEOHEX_PRECISION
        )

        geotile_precision = self.extract_precision(
            search_request.grid_geotile_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
        )

        centroid_geohash_grid_precision = self.extract_precision(
            search_request.centroid_geohash_grid_frequency_precision,
            1,
            self.MAX_GEOHASH_PRECISION,
        )

        centroid_geohex_grid_precision = self.extract_precision(
            search_request.centroid_geohex_grid_frequency_precision,
            0,
            self.MAX_GEOHEX_PRECISION,
        )

        centroid_geotile_grid_precision = self.extract_precision(
            search_request.centroid_geotile_grid_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
        )

        geometry_geohash_grid_precision = self.extract_precision(
            search_request.geometry_geohash_grid_frequency_precision,
            1,
            self.MAX_GEOHASH_PRECISION,
        )

        # geometry_geohex_grid_frequency_precision = self.extract_precision(
        #     search_request.geometry_geohex_grid_frequency_precision, 0, self.MAX_GEOHASH_PRECISION
        # )

        geometry_geotile_grid_precision = self.extract_precision(
            search_request.geometry_geotile_grid_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
        )

        try:
            db_response = await self.database.aggregate(
                collections,
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
            result_aggs = db_response.get("aggregations", {})

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

            for agg_name in self.ALL_AGGREGATION_NAMES:
                if agg_name in aggregations_requested:
                    aggs.append(
                        Aggregation(
                            name=agg_name,
                            data_type="string",
                            value=result_aggs.get(agg_name, {}).get(
                                "value_as_string", None
                            ),
                        )
                    )

        links = [
            {
                "rel": "self",
                "type": "application/json",
                "href": urljoin(base_url, "aggregate"),
            },
            {"rel": "root", "type": "application/json", "href": base_url},
        ]
        # if collection_endpoint:
        #     links.append(
        #         {
        #             "rel": "collection",
        #             "type": "application/json",
        #             "href": collection_endpoint,
        #         }
        #     )
        results = AggregationCollection(
            type="AggregationCollection", aggregations=aggs, links=links
        )

        return results
