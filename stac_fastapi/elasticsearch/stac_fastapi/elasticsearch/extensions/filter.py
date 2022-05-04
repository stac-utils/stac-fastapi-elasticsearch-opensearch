from __future__ import annotations
from typing import List
from pydantic import BaseModel
from enum import Enum
from typing import Union
from datetime import datetime, date
from geojson_pydantic import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection


# Basic CQL2
# AND, OR, NOT), comparison operators (=, <>, <, <=, >, >=), and IS NULL.
# The comparison operators are allowed against string, numeric, boolean, date, and datetime types.

# Basic Spatial Operators (http://www.opengis.net/spec/cql2/1.0/conf/basic-spatial-operators)
# defines the intersects operator (S_INTERSECTS).

class LogicalOp(str, Enum):
    _and = "and"
    _or = "or"
    _not = "not"


class ComparisonOp(str, Enum):
    eq = "="
    neq = "<>"
    lt = "<"
    lte = "<="
    gt = ">"
    gte = ">="
    is_null = "isNull"

    def to_es(self):
        if self == ComparisonOp.lt:
            return "lt"

        if self == ComparisonOp.lte:
            return "lte"

        if self == ComparisonOp.gt:
            return "gt"

        if self == ComparisonOp.gte:
            return "gte"


class SpatialIntersectsOp(str, Enum):
    s_intersects = "s_intersects"


class PropertyReference(BaseModel):
    property: str

    def to_es(self):
        return self.property


class Timestamp(BaseModel):
    timestamp: datetime

    def to_es(self):
        return self.timestamp.isoformat()


class Date(BaseModel):
    date: date

    def to_es(self):
        return self.date.isoformat()


class Clause(BaseModel):
    op: Union[LogicalOp, ComparisonOp, SpatialIntersectsOp]
    args: List["Arg"]

    def to_es(self, **kwargs):
        if self.op == LogicalOp._and:
            return {"bool": {"filter": [to_es(arg) for arg in self.args]}}
        elif self.op == LogicalOp._or:
            return {"bool": {"should": [to_es(arg) for arg in self.args]}}
        elif self.op == LogicalOp._not:
            return {"bool": {"must_not": [to_es(arg) for arg in self.args]}}
        elif self.op == ComparisonOp.eq:
            return {"term": {to_es(self.args[0]): to_es(self.args[1])}}
        elif self.op == ComparisonOp.neq:
            return {"bool": {"must_not": [{"term": {to_es(self.args[0]): to_es(self.args[1])}}]}}
        elif self.op == ComparisonOp.lt or self.op == ComparisonOp.lte or \
                self.op == ComparisonOp.gt or self.op == ComparisonOp.gte:
            return {"range": {to_es(self.args[0]): {to_es(self.op): to_es(self.args[1])}}}
        elif self.op == ComparisonOp.is_null:
            return {"bool": {"must_not": {"exists": {"field": to_es(self.args[0])}}}}
        elif self.op == SpatialIntersectsOp.s_intersects:
            return {
                "geo_shape": {
                    to_es(self.args[0]): {
                        "shape": to_es(self.args[1]),
                        "relation": "intersects"
                    }
                }
            }


Arg = Union[Clause, PropertyReference, Timestamp, Date,
            Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection,
            int, float, str, bool]


def to_es(arg: Arg):
    if (to_es_method := getattr(arg, "to_es", None)) and callable(to_es_method):
        return to_es_method()
    elif gi := getattr(arg, "__geo_interface__", None):
        return gi
    elif isinstance(arg, GeometryCollection):
        return arg.dict()
    elif isinstance(arg, int) or isinstance(arg, float) or isinstance(arg, str) or isinstance(arg, bool):
        return arg
    else:
        raise RuntimeError(f"unknown arg {repr(arg)}")
