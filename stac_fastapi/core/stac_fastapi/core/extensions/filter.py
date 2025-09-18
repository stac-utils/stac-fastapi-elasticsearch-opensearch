"""Filter extension logic for conversion."""

# """
# Implements Filter Extension.

# Basic CQL2 (AND, OR, NOT), comparison operators (=, <>, <, <=, >, >=), and IS NULL.
# The comparison operators are allowed against string, numeric, boolean, date, and datetime types.

# Advanced comparison operators (http://www.opengis.net/spec/cql2/1.0/req/advanced-comparison-operators)
# defines the LIKE, IN, and BETWEEN operators.

# Basic Spatial Operators (http://www.opengis.net/spec/cql2/1.0/conf/basic-spatial-operators)
# defines spatial operators (S_INTERSECTS, S_CONTAINS, S_WITHIN, S_DISJOINT).
# """

from enum import Enum
from typing import Any, Dict

DEFAULT_QUERYABLES: Dict[str, Dict[str, Any]] = {
    "id": {
        "description": "ID",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id",
    },
    "collection": {
        "description": "Collection",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/then/properties/collection",
    },
    "geometry": {
        "description": "Geometry",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/1/oneOf/0/properties/geometry",
    },
    "datetime": {
        "description": "Acquisition Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/datetime",
    },
    "created": {
        "description": "Creation Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/created",
    },
    "updated": {
        "description": "Creation Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/updated",
    },
}
"""Queryables that are present in all collections."""

OPTIONAL_QUERYABLES: Dict[str, Dict[str, Any]] = {
    "platform": {
        "$enum": True,
        "description": "Satellite platform identifier",
    },
}
"""Queryables that are present in some collections."""

ALL_QUERYABLES: Dict[str, Dict[str, Any]] = DEFAULT_QUERYABLES | OPTIONAL_QUERYABLES


class LogicalOp(str, Enum):
    """Enumeration for logical operators used in constructing Elasticsearch queries."""

    AND = "and"
    OR = "or"
    NOT = "not"


class ComparisonOp(str, Enum):
    """Enumeration for comparison operators used in filtering queries according to CQL2 standards."""

    EQ = "="
    NEQ = "<>"
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    IS_NULL = "isNull"


class AdvancedComparisonOp(str, Enum):
    """Enumeration for advanced comparison operators like 'like', 'between', and 'in'."""

    LIKE = "like"
    BETWEEN = "between"
    IN = "in"


class SpatialOp(str, Enum):
    """Enumeration for spatial operators as per CQL2 standards."""

    S_INTERSECTS = "s_intersects"
    S_CONTAINS = "s_contains"
    S_WITHIN = "s_within"
    S_DISJOINT = "s_disjoint"
