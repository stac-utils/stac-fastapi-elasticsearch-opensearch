"""Utility functions to handle datetime parsing."""

from datetime import datetime, timezone

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.types.rfc3339 import rfc3339_str_to_datetime


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
        MIN_DATE_NANOS = datetime(1970, 1, 1, tzinfo=timezone.utc)
        MAX_DATE_NANOS = datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=timezone.utc)

        def normalize(dt):
            """Normalize datetime string and preserve nano second precision."""
            dt = dt.strip()
            if not dt or dt == "..":
                return ".."
            dt_utc = rfc3339_str_to_datetime(dt).astimezone(timezone.utc)
            if dt_utc < MIN_DATE_NANOS:
                dt_utc = MIN_DATE_NANOS
            if dt_utc > MAX_DATE_NANOS:
                dt_utc = MAX_DATE_NANOS
            dt_normalized = dt_utc.isoformat(timespec="auto").replace("+00:00", "Z")
            if "." not in dt_normalized:
                dt_normalized = dt_normalized.replace("Z", ".0Z")
            return dt_normalized

        if not isinstance(date_str, str):
            return f"{MIN_DATE_NANOS.isoformat(timespec='auto').replace('+00:00','Z')}/{MAX_DATE_NANOS.isoformat(timespec='auto').replace('+00:00','Z')}"

        if "/" not in date_str:
            return f"{normalize(date_str)}/{normalize(date_str)}"

        try:
            start, end = date_str.split("/", 1)
        except Exception:
            return f"{MIN_DATE_NANOS.isoformat(timespec='auto').replace('+00:00','Z')}/{MAX_DATE_NANOS.isoformat(timespec='auto').replace('+00:00','Z')}"

        normalized_start = normalize(start)
        normalized_end = normalize(end)

        if normalized_start == "..":
            normalized_start = MIN_DATE_NANOS.isoformat(timespec="auto").replace(
                "+00:00", "Z"
            )
        if normalized_end == "..":
            normalized_end = MAX_DATE_NANOS.isoformat(timespec="auto").replace(
                "+00:00", "Z"
            )

        return f"{normalized_start}/{normalized_end}"

    else:

        def normalize(dt):
            """Normalize datetime string and preserve millisecond precision."""
            dt = dt.strip()
            if not dt or dt == "..":
                return ".."
            dt_obj = rfc3339_str_to_datetime(dt)
            dt_utc = dt_obj.astimezone(timezone.utc)
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
