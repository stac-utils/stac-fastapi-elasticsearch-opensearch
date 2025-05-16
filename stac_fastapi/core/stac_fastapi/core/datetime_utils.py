"""Utility functions to handle datetime parsing."""
from datetime import datetime
from datetime import datetime as datetime_type
from datetime import timezone
from typing import Dict, Optional, Union

from stac_fastapi.types.rfc3339 import DateTimeType, rfc3339_str_to_datetime


def return_date(
    interval: Optional[Union[DateTimeType, str]]
) -> Dict[str, Optional[str]]:
    """
    Convert a date interval.

    (which may be a datetime, a tuple of one or two datetimes a string
    representing a datetime or range, or None) into a dictionary for filtering
    search results with Elasticsearch.

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


def format_datetime_range(date_str: str) -> str:
    """
    Convert a datetime range string into a normalized UTC string for API requests using rfc3339_str_to_datetime.

    Args:
        date_str (str): A string containing two datetime values separated by a '/'.

    Returns:
        str: A string formatted as 'YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ', with '..' used if any element is None.
    """

    def normalize(dt):
        dt = dt.strip()
        if not dt or dt == "..":
            return ".."
        dt_obj = rfc3339_str_to_datetime(dt)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    if not isinstance(date_str, str):
        return "../.."
    if "/" not in date_str:
        return f"{normalize(date_str)}/{normalize(date_str)}"
    try:
        start, end = date_str.split("/", 1)
    except Exception:
        return "../.."
    return f"{normalize(start)}/{normalize(end)}"


# Borrowed from pystac - https://github.com/stac-utils/pystac/blob/f5e4cf4a29b62e9ef675d4a4dac7977b09f53c8f/pystac/utils.py#L370-L394
def datetime_to_str(dt: datetime, timespec: str = "auto") -> str:
    """Convert a :class:`datetime.datetime` instance to an ISO8601 string in the `RFC 3339, section 5.6.

    <https://datatracker.ietf.org/doc/html/rfc3339#section-5.6>`__ format required by
    the :stac-spec:`STAC Spec <master/item-spec/common-metadata.md#date-and-time>`.

    Args:
        dt : The datetime to convert.
        timespec: An optional argument that specifies the number of additional
            terms of the time to include. Valid options are 'auto', 'hours',
            'minutes', 'seconds', 'milliseconds' and 'microseconds'. The default value
            is 'auto'.
    Returns:
        str: The ISO8601 (RFC 3339) formatted string representing the datetime.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    timestamp = dt.isoformat(timespec=timespec)
    zulu = "+00:00"
    if timestamp.endswith(zulu):
        timestamp = f"{timestamp[: -len(zulu)]}Z"

    return timestamp


def now_in_utc() -> datetime:
    """Return a datetime value of now with the UTC timezone applied."""
    return datetime.now(timezone.utc)


def now_to_rfc3339_str() -> str:
    """Return an RFC 3339 string representing now."""
    return datetime_to_str(now_in_utc())
