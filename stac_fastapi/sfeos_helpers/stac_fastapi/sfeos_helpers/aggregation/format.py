"""Formatting functions for aggregation responses."""

from datetime import datetime
from typing import Any, Dict

from stac_fastapi.core.datetime_utils import datetime_to_str
from stac_fastapi.extensions.core.aggregation.types import Aggregation


def frequency_agg(es_aggs: Dict[str, Any], name: str, data_type: str) -> Aggregation:
    """Format an aggregation for a frequency distribution aggregation.

    Args:
        es_aggs: The Elasticsearch/OpenSearch aggregation response
        name: The name of the aggregation
        data_type: The data type of the aggregation

    Returns:
        Aggregation: A formatted aggregation response
    """
    buckets = []
    for bucket in es_aggs.get(name, {}).get("buckets", []):
        bucket_data = {
            "key": bucket.get("key_as_string") or bucket.get("key"),
            "data_type": data_type,
            "frequency": bucket.get("doc_count"),
            "to": bucket.get("to"),
            "from": bucket.get("from"),
        }
        buckets.append(bucket_data)
    return Aggregation(
        name=name,
        data_type="frequency_distribution",
        overflow=es_aggs.get(name, {}).get("sum_other_doc_count", 0),
        buckets=buckets,
    )


def metric_agg(es_aggs: Dict[str, Any], name: str, data_type: str) -> Aggregation:
    """Format an aggregation for a metric aggregation.

    Args:
        es_aggs: The Elasticsearch/OpenSearch aggregation response
        name: The name of the aggregation
        data_type: The data type of the aggregation

    Returns:
        Aggregation: A formatted aggregation response
    """
    value = es_aggs.get(name, {}).get("value_as_string") or es_aggs.get(name, {}).get(
        "value"
    )
    # ES 7.x does not return datetimes with a 'value_as_string' field
    if "datetime" in name and isinstance(value, float):
        value = datetime_to_str(datetime.fromtimestamp(value / 1e3))
    return Aggregation(
        name=name,
        data_type=data_type,
        value=value,
    )
