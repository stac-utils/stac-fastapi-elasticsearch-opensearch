"""Request model for the Aggregation extension."""

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
    """Add implementation specific query parameters to AggregationExtensionGetRequest for aggrgeation precision."""

    centroid_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)


class EsAggregationExtensionPostRequest(
    AggregationExtensionPostRequest, FilterExtensionPostRequest
):
    """Add implementation specific query parameters to AggregationExtensionPostRequest for aggrgeation precision."""

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
        {
            "name": "collection_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
    ]

    GEO_AGGREGATIONS = [
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
        {
            "name": "geometry_geohash_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
        # {
        #     "name": "geometry_geohex_grid_frequency",
        #     "data_type": "frequency_distribution",
        #     "frequency_distribution_data_type": "string",
        # },
        {
            "name": "geometry_geotile_grid_frequency",
            "data_type": "frequency_distribution",
            "frequency_distribution_data_type": "string",
        },
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
        return Aggregation(
            name=name,
            data_type=data_type,
            value=es_aggs.get(name, {}).get("value_as_string")
            or es_aggs.get(name, {}).get("value"),
        )

    async def aggregate(
        self,
        aggregate_request: Optional[EsAggregationExtensionPostRequest] = None,
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
        **kwargs,
    ) -> Union[Dict, Exception]:
        """Get aggregations from the database."""
        request: Request = kwargs["request"]
        base_url = str(request.base_url)
        search = self.database.make_search()
        if aggregate_request is None:
            # this is borrowed from stac-fastapi-pgstac
            # Kludgy fix because using factory does not allow alias for filter-lang
            # If the value is the default, check if the request is different.
            query_params = str(request.query_params)
            if filter_lang is None:
                match = re.search(
                    r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE
                )
                if match:
                    filter_lang = match.group(1)
                else:
                    filter_lang = "cql2-text"

            aggregate_request = EsAggregationExtensionGetRequest(
                collections=",".join(collections) if collections else None,  # type: ignore [call-arg]
                datetime=datetime,  # type: ignore [call-arg]
                intersects=intersects,  # type: ignore [call-arg]
                filter=filter,  # type: ignore [call-arg]
                aggregations=",".join(aggregations) if aggregations else None,  # type: ignore [call-arg]
                ids=ids,  # type: ignore [call-arg]
                bbox=bbox,  # type: ignore [call-arg]
                centroid_geohash_grid_frequency_precision=centroid_geohash_grid_frequency_precision,
                centroid_geohex_grid_frequency_precision=centroid_geohex_grid_frequency_precision,
                centroid_geotile_grid_frequency_precision=centroid_geotile_grid_frequency_precision,
                geometry_geohash_grid_frequency_precision=geometry_geohash_grid_frequency_precision,
                geometry_geotile_grid_frequency_precision=geometry_geotile_grid_frequency_precision,
            )
        else:
            filter_lang = "cql2-json"
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
                # supported_aggregations = set([x["name"] for x in aggs["aggregations"]] + [x["name"] for x in self.DEFAULT_AGGREGATIONS])

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
            if filter_lang == "cql2-text":
                aggregate_request.filter = orjson.loads(
                    unquote_plus(to_cql2(parse_cql2_text(aggregate_request.filter)))
                )
            elif filter_lang == "cql2-json":
                if isinstance(aggregate_request.filter, str):
                    aggregate_request.filter = orjson.loads(
                        unquote_plus(aggregate_request.filter)
                    )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown filter-lang: {aggregate_request.filter_lang}. Filter-lang or cql2-json and cql2-text are supported.",
                )
            try:
                search = self.database.apply_cql2_filter(
                    search, aggregate_request.filter
                )
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
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

        # geometry_geohex_grid_frequency_precision = self.extract_precision(
        #     aggregate_request.geometry_geohex_grid_frequency_precision, 0, self.MAX_GEOHASH_PRECISION
        # )

        geometry_geotile_grid_precision = self.extract_precision(
            aggregate_request.geometry_geotile_grid_frequency_precision,
            0,
            self.MAX_GEOTILE_PRECISION,
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
                # geometry_geohex_grid_precision,
                geometry_geotile_grid_precision,
            )
        except Exception as error:
            if not isinstance(error, IndexError):
                raise error
        aggs = []
        if db_response:
            result_aggs = db_response.get("aggregations", {})

            for agg in supported_aggregations + self.GEO_AGGREGATIONS:
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
