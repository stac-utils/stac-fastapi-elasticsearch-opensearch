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

import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any

import cql2

from stac_fastapi.types.rfc3339 import rfc3339_str_to_datetime

DEFAULT_QUERYABLES: dict[str, dict[str, Any]] = {
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

OPTIONAL_QUERYABLES: dict[str, dict[str, Any]] = {
    "platform": {
        "$enum": True,
        "description": "Satellite platform identifier",
    },
}
"""Queryables that are present in some collections."""

ALL_QUERYABLES: dict[str, dict[str, Any]] = DEFAULT_QUERYABLES | OPTIONAL_QUERYABLES

_FULL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

MAX_CQL2_DEPTH = 16
"""How deep CQL2 text may nest parentheses and operators."""

MAX_CQL2_AND_OR = 1000
"""How many AND and OR operators CQL2 text may hold."""

# Quoted strings and names, which _check_depth skips, then what it counts.
_CQL2_TOKEN = re.compile(
    r"""'(?:[^']|'')*'|"(?:[^"]|"")*"|[(),]|[-+*/%^=<>!]|\b(?:and|or|not|like|in|div)\b"""
)


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


@dataclass
class CqlNode:
    """Base class."""

    pass


@dataclass
class LogicalNode(CqlNode):
    """Logical operators (AND, OR, NOT)."""

    op: LogicalOp
    children: list["CqlNode"]


@dataclass
class ComparisonNode(CqlNode):
    """Comparison operators (=, <>, <, <=, >, >=, is null)."""

    op: ComparisonOp
    field: str
    value: Any


@dataclass
class AdvancedComparisonNode(CqlNode):
    """Advanced comparison operators (like, between, in)."""

    op: AdvancedComparisonOp
    field: str
    value: Any


@dataclass
class SpatialNode(CqlNode):
    """Spatial operators."""

    op: SpatialOp
    field: str
    geometry: dict[str, Any]


class CQL2TextError(ValueError):
    """A CQL2 text filter that does not parse, or holds an invalid literal."""


def cql2_text_to_json(cql2_text: str) -> dict[str, Any]:
    """Convert a CQL2 text filter to CQL2 JSON.

    The cql2 library writes every number as a float. Whole numbers are written
    back as integers, as CQL2 JSON clients send them: Elasticsearch and
    OpenSearch compare a number with a keyword field as text, where 161.0 does
    not match "161". cql2 does not check what a DATE or TIMESTAMP holds, so
    this does: an RFC 3339 full-date and date-time, as CQL2 requires.

    Args:
        cql2_text (str): The CQL2 text filter.

    Returns:
        dict[str, Any]: The filter as CQL2 JSON.

    Raises:
        CQL2TextError: If the text is not valid CQL2 text, nests too deep, or
            a DATE, TIMESTAMP or INTERVAL holds no valid date or timestamp.
    """
    _check_depth(cql2_text)
    try:
        expr = cql2.parse_text(cql2_text)
    except cql2.ParseError as e:
        raise CQL2TextError("expected valid CQL2 text") from e
    return _checked(expr.to_json())


def _check_depth(cql2_text: str) -> None:
    """Raise CQL2TextError for CQL2 text that nests too deep for cql2.

    cql2 has no depth limit yet. It parses recursively, so deep nesting
    overflows the stack and kills the process, and each nested parenthesis
    can double the parse time. Each parenthesis and each operator is one
    level deeper, up to the next comma, AND or OR at its own level. cql2 also
    nests AND and OR one level each before it flattens them, so their number
    is capped too.
    """
    outer: list[int] = []
    depth = and_or = 0
    for token in _CQL2_TOKEN.findall(cql2_text.lower()):
        if token == "(":
            outer.append(depth)
            depth += 1
        elif token == ")":
            depth = outer.pop() if outer else 0
        elif token in (",", "and", "or"):
            depth = outer[-1] + 1 if outer else 0
            and_or += token != ","
        elif token[0] not in "'\"":
            depth += 1
        if depth > MAX_CQL2_DEPTH or and_or > MAX_CQL2_AND_OR:
            raise CQL2TextError(
                f"expected CQL2 text nested at most {MAX_CQL2_DEPTH} levels deep"
                f" and at most {MAX_CQL2_AND_OR} AND and OR"
            )


def _checked(value: Any) -> Any:
    """Write whole numbers as integers and check temporal literals."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 2**53:
        return int(value)
    if isinstance(value, list):
        return [_checked(element) for element in value]
    if isinstance(value, dict):
        if value.keys() == {"date"}:
            _check_instant("DATE", value["date"], date_only=True)
        elif value.keys() == {"timestamp"}:
            _check_instant("TIMESTAMP", value["timestamp"], date_only=False)
        elif value.keys() == {"interval"} and isinstance(value["interval"], list):
            for bound in value["interval"]:
                if bound != "..":
                    _check_instant("INTERVAL", bound, date_only=None)
        return {key: _checked(element) for key, element in value.items()}
    return value


def _check_instant(kind: str, literal: Any, date_only: bool | None) -> None:
    """Raise CQL2TextError unless literal is an RFC 3339 date or date-time.

    date_only is True for a date, False for a date-time, None for either.
    """
    if isinstance(literal, str):
        if date_only is not False and _FULL_DATE.fullmatch(literal):
            try:
                date.fromisoformat(literal)
                return
            except ValueError:
                pass
        elif date_only is not True:
            try:
                rfc3339_str_to_datetime(literal)
                return
            except ValueError:
                pass
    expected = {True: "date", False: "date-time", None: "date or date-time"}
    raise CQL2TextError(f"{kind}({literal!r}) is not an RFC 3339 {expected[date_only]}")
