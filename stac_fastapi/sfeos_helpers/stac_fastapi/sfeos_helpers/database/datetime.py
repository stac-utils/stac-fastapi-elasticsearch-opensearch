"""Elasticsearch/OpenSearch-specific datetime utilities.

This module provides datetime utility functions specifically designed for
Elasticsearch and OpenSearch query formatting.
"""

from datetime import datetime as datetime_type
from typing import Dict, Optional, Union

from stac_fastapi.types.rfc3339 import DateTimeType


def return_date(
    interval: Optional[Union[DateTimeType, str]]
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
    result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

    if interval is None:
        return result

    if isinstance(interval, str):
        if "/" in interval:
            parts = interval.split("/")
            result["gte"] = parts[0] if parts[0] != ".." else None
            result["lte"] = parts[1] if len(parts) > 1 and parts[1] != ".." else None
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
