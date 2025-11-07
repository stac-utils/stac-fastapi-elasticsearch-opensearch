"""Elasticsearch/OpenSearch-specific datetime utilities.

This module provides datetime utility functions specifically designed for
Elasticsearch and OpenSearch query formatting.
"""

import logging
import math
import re
from datetime import date
from datetime import datetime as datetime_type
from datetime import timezone
from typing import Dict, Optional, Union

from stac_fastapi.types.rfc3339 import DateTimeType, rfc3339_str_to_datetime

logger = logging.getLogger(__name__)


def return_date(
    interval: Optional[Union[DateTimeType, str]],
) -> Dict[str, Optional[str]]:
    """
    Convert a date interval to an Elasticsearch/OpenSearch query format.

    This function converts a date interval (which may be a datetime, a tuple of one or two datetimes,
    a string representing a datetime or range, or None) into a dictionary for filtering
    search results with Elasticsearch/OpenSearch.

    This function ensures the output dictionary contains 'gte' and 'lte' keys,
    even if they are set to None, to prevent KeyError in the consuming logic.

    Args:
        interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
            a tuple with one or two datetimes, a string, or None.

    Returns:
        dict: A dictionary representing the date interval for use in filtering search results,
            always containing 'gte' and 'lte' keys.
    """

    def normalize_datetime(dt_str):
        """Normalize datetime string and preserve millisecond precision."""
        if not dt_str or dt_str == "..":
            return dt_str
        dt_obj = rfc3339_str_to_datetime(dt_str)
        dt_utc = dt_obj.astimezone(timezone.utc)
        rounded_dt = dt_utc.replace(
            microsecond=math.floor(dt_utc.microsecond / 1000 + 0.5) * 1000
        )
        return rounded_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

    if interval is None:
        return result

    if isinstance(interval, str):
        if "/" in interval:
            parts = interval.split("/")
            gte_value = (
                parts[0] if parts[0] != ".." else datetime_type.min.isoformat() + "Z"
            )
            lte_value = (
                parts[1]
                if len(parts) > 1 and parts[1] != ".."
                else datetime_type.max.isoformat() + "Z"
            )

            result["gte"] = (
                normalize_datetime(gte_value)
                if gte_value != datetime_type.min.isoformat() + "Z"
                else gte_value
            )
            result["lte"] = (
                normalize_datetime(lte_value)
                if lte_value != datetime_type.max.isoformat() + "Z"
                else lte_value
            )
        else:
            converted_time = normalize_datetime(interval) if interval != ".." else ".."
            result["gte"] = result["lte"] = converted_time
        return result

    if isinstance(interval, datetime_type):
        datetime_str = interval.isoformat()
        normalized_datetime = normalize_datetime(datetime_str)
        result["gte"] = result["lte"] = normalized_datetime
    elif isinstance(interval, tuple):
        start, end = interval
        # Ensure datetimes are converted to UTC and formatted with 'Z'
        if start:
            dt_utc = start.astimezone(timezone.utc)
            rounded_dt = dt_utc.replace(
                microsecond=round(dt_utc.microsecond / 1000) * 1000
            )
            result["gte"] = rounded_dt.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            )
        if end:
            dt_utc = end.astimezone(timezone.utc)
            rounded_dt = dt_utc.replace(
                microsecond=round(dt_utc.microsecond / 1000) * 1000
            )
            result["lte"] = rounded_dt.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            )

    return result


def extract_date(date_str: str) -> date:
    """Extract date from ISO format string.

    Args:
        date_str: ISO format date string

    Returns:
        A date object extracted from the input string.
    """
    date_str = date_str.replace("Z", "+00:00")
    return datetime_type.fromisoformat(date_str).date()


def extract_first_date_from_index(index_name: str) -> date:
    """Extract the first date from an index name containing date patterns.

    Searches for date patterns (YYYY-MM-DD) within the index name string
    and returns the first found date as a date object.

    Args:
        index_name: Index name containing date patterns.

    Returns:
        A date object extracted from the first date pattern found in the index name.

    """
    date_pattern = r"\d{4}-\d{2}-\d{2}"
    match = re.search(date_pattern, index_name)

    if not match:
        logger.error(f"No date pattern found in index name: '{index_name}'")
        raise ValueError(
            f"No date pattern (YYYY-MM-DD) found in index name: '{index_name}'"
        )

    date_string = match.group(0)

    try:
        extracted_date = datetime_type.strptime(date_string, "%Y-%m-%d").date()
        return extracted_date
    except ValueError as e:
        logger.error(
            f"Invalid date format found in index name '{index_name}': "
            f"'{date_string}' - {str(e)}"
        )
        raise ValueError(
            f"Invalid date format in index name '{index_name}': '{date_string}'"
        ) from e
