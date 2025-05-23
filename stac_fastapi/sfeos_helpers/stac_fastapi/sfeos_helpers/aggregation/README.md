# STAC FastAPI Aggregation Package

This package contains shared aggregation functionality used by both the Elasticsearch and OpenSearch implementations of STAC FastAPI. It helps reduce code duplication and ensures consistent behavior between the two implementations.

## Package Structure

The aggregation package is organized into three main modules:

- **client.py**: Contains the base aggregation client implementation
  - `EsAsyncBaseAggregationClient`: The main class that implements the STAC aggregation extension for Elasticsearch/OpenSearch
  - Methods for handling aggregation requests, validating parameters, and formatting responses

- **format.py**: Contains functions for formatting aggregation responses
  - `frequency_agg`: Formats frequency distribution aggregation responses
  - `metric_agg`: Formats metric aggregation responses

- **__init__.py**: Package initialization and exports
  - Exports the main classes and functions for use by other modules

## Features

The aggregation package provides the following features:

- Support for various aggregation types:
  - Datetime frequency
  - Collection frequency
  - Property frequency
  - Geospatial grid aggregations (geohash, geohex, geotile)
  - Metric aggregations (min, max, etc.)

- Parameter validation:
  - Precision validation for geospatial aggregations
  - Interval validation for datetime aggregations

- Response formatting:
  - Consistent response structure
  - Proper typing and documentation

## Usage

The aggregation package is used by the Elasticsearch and OpenSearch implementations to provide aggregation functionality for STAC API. The main entry point is the `EsAsyncBaseAggregationClient` class, which is instantiated in the respective app.py files.

Example:
```python
from stac_fastapi.sfeos_helpers.aggregation import EsAsyncBaseAggregationClient

# Create an instance of the aggregation client
aggregation_client = EsAsyncBaseAggregationClient(database)

# Register the aggregation extension with the API
api = StacApi(
    ...,
    extensions=[
        ...,
        AggregationExtension(client=aggregation_client),
    ],
)