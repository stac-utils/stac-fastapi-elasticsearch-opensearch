"""Utility functions to handle datetime parsing."""

import re
from datetime import datetime, timezone

from fastapi import HTTPException

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.types.rfc3339 import rfc3339_str_to_datetime


def _to_utc(value: str) -> datetime:
    try:
        return rfc3339_str_to_datetime(value).astimezone(timezone.utc)
    except OverflowError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid datetime: {value} is outside the range that can be represented in UTC.",
        )


def format_datetime_range(date_str: str) -> str:
    """
    Convert a datetime range string into a normalized UTC string for API requests using rfc3339_str_to_datetime.

    Args:
        date_str (str): A string containing two datetime values separated by a '/'.

    Returns:
        str: A string formatted as 'YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ', with '..' used if any element is None.
    """
    use_datetime_nanos = get_bool_env("USE_DATETIME_NANOS", default=True)

    if use_datetime_nanos:
        # Bounds outside 1970-2262 are kept; return_date clamps or empties them.

        def normalize(dt):
            """Normalize datetime string and preserve nano second precision."""
            dt = dt.strip()
            if not dt or dt == "..":
                return ".."
            dt_utc = _to_utc(dt)
            fraction = re.search(r":\d{2}\.(\d+)", dt)
            if fraction and len(fraction.group(1)) > 6:
                # Engines read up to nine digits; a UTC offset leaves the fraction as is.
                return f"{dt_utc:%Y-%m-%dT%H:%M:%S}.{fraction.group(1)[:9]}Z"
            dt_normalized = dt_utc.isoformat(timespec="auto").replace("+00:00", "Z")
            if "." not in dt_normalized:
                dt_normalized = dt_normalized.replace("Z", ".0Z")
            return dt_normalized

        if not isinstance(date_str, str):
            return "../.."

        if "/" not in date_str:
            return f"{normalize(date_str)}/{normalize(date_str)}"

        try:
            start, end = date_str.split("/", 1)
        except Exception:
            return "../.."
        return f"{normalize(start)}/{normalize(end)}"

    else:

        def normalize(dt):
            """Normalize datetime string and preserve millisecond precision."""
            dt = dt.strip()
            if not dt or dt == "..":
                return ".."
            dt_utc = _to_utc(dt)
            return dt_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

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
