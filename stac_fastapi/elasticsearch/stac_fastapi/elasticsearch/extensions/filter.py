"""
Implements Filter Extension.

Basic CQL2 (AND, OR, NOT), comparison operators (=, <>, <, <=, >, >=), and IS NULL.
The comparison operators are allowed against string, numeric, boolean, date, and datetime types.

Basic Spatial Operators (http://www.opengis.net/spec/cql2/1.0/conf/basic-spatial-operators)
defines the intersects operator (S_INTERSECTS).
"""
from __future__ import annotations

import datetime
from enum import Enum
from typing import List, Union

from geojson_pydantic import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from pydantic import BaseModel

queryables_mapping = {
    "id": "id",
    "collection": "collection",
    "geometry": "geometry",
    "datetime": "properties.datetime",
    "created": "properties.created",
    "updated": "properties.updated",
    "cloud_cover": "properties.eo:cloud_cover",
    "cloud_shadow_percentage": "properties.s2:cloud_shadow_percentage",
    "nodata_pixel_percentage": "properties.s2:nodata_pixel_percentage",
}


class LogicalOp(str, Enum):
    """Logical operator.

    CQL2 logical operators and, or, and not.
    """

    _and = "and"
    _or = "or"
    _not = "not"


class ComparisonOp(str, Enum):
    """Comparison operator.

    CQL2 comparison operators =, <>, <, <=, >, >=, and isNull.
    """

    eq = "="
    neq = "<>"
    lt = "<"
    lte = "<="
    gt = ">"
    gte = ">="
    is_null = "isNull"

    def to_es(self):
        """Generate an Elasticsearch term operator."""
        if self == ComparisonOp.lt:
            return "lt"
        elif self == ComparisonOp.lte:
            return "lte"
        elif self == ComparisonOp.gt:
            return "gt"
        elif self == ComparisonOp.gte:
            return "gte"
        else:
            raise RuntimeError(
                f"Comparison op {self.value} does not have an Elasticsearch term operator equivalent."
            )


class SpatialIntersectsOp(str, Enum):
    """Spatial intersections operator s_intersects."""

    s_intersects = "s_intersects"


class PropertyReference(BaseModel):
    """Property reference."""

    property: str

    def to_es(self):
        """Produce a term value for this, possibly mapped by a queryable."""
        return queryables_mapping.get(self.property, self.property)


class Timestamp(BaseModel):
    """Representation of an RFC 3339 datetime value object."""

    timestamp: datetime.datetime

    def to_es(self):
        """Produce an RFC 3339 datetime string."""
        return self.timestamp.isoformat()


class Date(BaseModel):
    """Representation of an ISO 8601 date value object."""

    date: datetime.date

    def to_es(self):
        """Produce an ISO 8601 date string."""
        return self.date.isoformat()


Arg = Union[
    "Clause",
    PropertyReference,
    Timestamp,
    Date,
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
    int,
    float,
    str,
    bool,
]


class Clause(BaseModel):
    """Filter extension clause."""

    op: Union[LogicalOp, ComparisonOp, SpatialIntersectsOp]
    args: List[Arg]

    def to_es(self):
        """Generate an Elasticsearch expression for this Clause."""
        if self.op == LogicalOp._and:
            return {"bool": {"filter": [to_es(arg) for arg in self.args]}}
        elif self.op == LogicalOp._or:
            return {"bool": {"should": [to_es(arg) for arg in self.args]}}
        elif self.op == LogicalOp._not:
            return {"bool": {"must_not": [to_es(arg) for arg in self.args]}}
        elif self.op == ComparisonOp.eq:
            return {"term": {to_es(self.args[0]): to_es(self.args[1])}}
        elif self.op == ComparisonOp.neq:
            return {
                "bool": {
                    "must_not": [{"term": {to_es(self.args[0]): to_es(self.args[1])}}]
                }
            }
        elif (
            self.op == ComparisonOp.lt
            or self.op == ComparisonOp.lte
            or self.op == ComparisonOp.gt
            or self.op == ComparisonOp.gte
        ):
            return {
                "range": {to_es(self.args[0]): {to_es(self.op): to_es(self.args[1])}}
            }
        elif self.op == ComparisonOp.is_null:
            return {"bool": {"must_not": {"exists": {"field": to_es(self.args[0])}}}}
        elif self.op == SpatialIntersectsOp.s_intersects:
            return {
                "geo_shape": {
                    to_es(self.args[0]): {
                        "shape": to_es(self.args[1]),
                        "relation": "intersects",
                    }
                }
            }


def to_es(arg: Arg):
    """Generate an Elasticsearch expression for this Arg."""
    if (to_es_method := getattr(arg, "to_es", None)) and callable(to_es_method):
        return to_es_method()
    elif gi := getattr(arg, "__geo_interface__", None):
        return gi
    elif isinstance(arg, GeometryCollection):
        return arg.dict()
    elif (
        isinstance(arg, int)
        or isinstance(arg, float)
        or isinstance(arg, str)
        or isinstance(arg, bool)
    ):
        return arg
    else:
        raise RuntimeError(f"unknown arg {repr(arg)}")
