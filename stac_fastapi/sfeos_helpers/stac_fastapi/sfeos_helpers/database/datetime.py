"""Elasticsearch/OpenSearch-specific datetime utilities.

This module provides datetime utility functions specifically designed for
Elasticsearch and OpenSearch query formatting.
"""

import logging
import re
from datetime import date
from datetime import datetime as datetime_type
from datetime import timezone
from typing import Dict, Optional, Union

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.types.rfc3339 import DateTimeType

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
    result: Dict[str, Optional[str]] = {"gte": None, "lte": None}
    use_datetime_nanos = get_bool_env("USE_DATETIME_NANOS", default=True)
    if interval is None:
        return result

    if use_datetime_nanos:
        MIN_DATE_NANOS = datetime_type(1970, 1, 1, tzinfo=timezone.utc)
        MAX_DATE_NANOS = datetime_type(
            2262, 4, 11, 23, 47, 16, 854775, tzinfo=timezone.utc
        )

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = (
                    parts[0] if parts[0] != ".." else MIN_DATE_NANOS.isoformat()
                )
                result["lte"] = (
                    parts[1]
                    if len(parts) > 1 and parts[1] != ".."
                    else MAX_DATE_NANOS.isoformat()
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            dt_utc = (
                interval.astimezone(timezone.utc)
                if interval.tzinfo
                else interval.replace(tzinfo=timezone.utc)
            )
            if dt_utc < MIN_DATE_NANOS:
                dt_utc = MIN_DATE_NANOS
            elif dt_utc > MAX_DATE_NANOS:
                dt_utc = MAX_DATE_NANOS
            datetime_iso = dt_utc.isoformat().replace("+00:00", "Z")
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                if start_utc < MIN_DATE_NANOS:
                    start_utc = MIN_DATE_NANOS
                elif start_utc > MAX_DATE_NANOS:
                    start_utc = MAX_DATE_NANOS
                result["gte"] = start_utc.isoformat().replace("+00:00", "Z")
            if end:
                end_utc = (
                    end.astimezone(timezone.utc)
                    if end.tzinfo
                    else end.replace(tzinfo=timezone.utc)
                )
                if end_utc < MIN_DATE_NANOS:
                    end_utc = MIN_DATE_NANOS
                elif end_utc > MAX_DATE_NANOS:
                    end_utc = MAX_DATE_NANOS
                result["lte"] = end_utc.isoformat().replace("+00:00", "Z")

        return result

    else:
        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = (
                    parts[0]
                    if parts[0] != ".."
                    else datetime_type.min.isoformat() + "Z"
                )
                result["lte"] = (
                    parts[1]
                    if len(parts) > 1 and parts[1] != ".."
                    else datetime_type.max.isoformat() + "Z"
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
                result["gte"] = start.isoformat().replace("+00:00", "Z")
            if end:
                result["lte"] = end.isoformat().replace("+00:00", "Z")

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
