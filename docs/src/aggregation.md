## Aggregation

Stac-fastapi-elasticsearch-opensearch supports the STAC API [Aggregation Extension](https://github.com/stac-api-extensions/aggregation). This enables aggregation of points and geometries, as well as frequency distribution aggregation of any other property including dates. Aggregations can be defined at the root Catalog level (`/aggregations`) and at the Collection level (`/<collection_id>/aggregations`). The [Filter Extension](https://github.com/stac-api-extensions/filter) is also fully supported, enabling aggregated returns of search queries. Any query made with `/search` may also be executed with `/aggregate`, provided that the relevant aggregation fields are available,

The root `/aggregations` advertises every aggregation below, and the root `/aggregate` accepts any of them, with or without `collections`. A collection's `/aggregations` returns the aggregations declared in its `aggregations` field (or the defaults when it has none), and `/collections/{collection_id}/aggregate` accepts only those.

On Elasticsearch, `centroid_geohex_grid_frequency` and the `geometry_*` grid aggregations need a licence above basic; on a basic licence `/aggregate` answers `400` with the engine's reason. OpenSearch runs all of them.

A field named `aggregations` should be added to the Collection object for the collection for which the aggregations are available, for example:

Available aggregations are:

- total_count (count of total items)
- collection_frequency (Item `collection` field)
- platform_frequency (Item.Properties.platform)
- cloud_cover_frequency (Item.Properties.eo:cloud_cover)
- datetime_frequency (Item.Properties.datetime, monthly interval)
- datetime_min (earliest Item.Properties.datetime)
- datetime_max (latest Item.Properties.datetime)
- sun_elevation_frequency (Item.Properties.view:sun_elevation)
- sun_azimuth_frequency (Item.Properties.view:sun_azimuth)
- off_nadir_frequency (Item.Properties.view:off_nadir)
- grid_code_frequency (Item.Properties.grid:code)
- centroid_geohash_grid_frequency ([geohash grid](https://opensearch.org/docs/latest/aggregations/bucket/geohash-grid/)  on Item.Properties.proj:centroid)
- centroid_geohex_grid_frequency ([geohex grid](https://opensearch.org/docs/latest/aggregations/bucket/geohex-grid/) on Item.Properties.proj:centroid)
- centroid_geotile_grid_frequency (geotile on Item.Properties.proj:centroid)
- geometry_geohash_grid_frequency ([geohash grid](https://opensearch.org/docs/latest/aggregations/bucket/geohash-grid/) on Item.geometry)
- geometry_geotile_grid_frequency ([geotile grid](https://opensearch.org/docs/latest/aggregations/bucket/geotile-grid/) on Item.geometry)

### Adding aggregations through configuration

The aggregations the server can run are defined in `AGGREGATION_MAPPING`. Additional ones can be declared without changing the code with two environment variables:

- `STAC_FASTAPI_ES_CUSTOM_AGGREGATIONS`: a JSON object mapping an aggregation name to an Elasticsearch/OpenSearch aggregation body.
- `STAC_FASTAPI_ES_AGGREGATIONS_FILE`: path to a JSON file with the same content, used when the variable above is not set.

For example:

```json
{
  "asset_file_size_sum": {"sum": {"field": "assets.file:size"}},
  "per_week_frequency": {"date_histogram": {"field": "properties.datetime", "calendar_interval": "week"}}
}
```

Rules:

- Each entry holds exactly one aggregation type. An invalid entry is logged and ignored.
- The response shape depends on the name. A name ending in `_frequency` is returned as buckets, so its type must be one of `terms`, `date_histogram`, `histogram`, `range`, `date_range`, `geohash_grid`, `geotile_grid` or `geohex_grid`. Any other name is returned as a single value, so its type must be one of `sum`, `avg`, `min`, `max`, `value_count` or `cardinality`.
- A `date_histogram` sets exactly one of `calendar_interval` or `fixed_interval`. That interval applies unless the request sends `datetime_frequency_interval`, which then replaces it.
- An entry with the name of a built-in aggregation replaces it.
- The aggregation must be listed in the `aggregations` field of each collection that uses it (`name`, `data_type` and, for a distribution, `frequency_distribution_data_type`). The root `/aggregations` does not list it, and the root `/aggregate` accepts it only when `collections` names a collection that lists it.
- The definitions come from the server configuration, never from a request. They can use any feature the backend supports, including scripts, so only trusted operators should set them.
- A `min` or `max` on a date field is converted to a date value only when the aggregation is declared with `data_type: "datetime"` in the collection's `aggregations` configuration.
- Keyed buckets (`"keyed": true`) are not supported. Such aggregation entries are ignored and a warning is logged during configuration loading.
- A grid aggregation's configured precision is used by default. A request-level *_frequency_precision parameter overrides it only when it matches the aggregation's actual grid type:

    ..._geohash_grid_frequency → geohash_grid
    ..._geotile_grid_frequency → geotile_grid
    ..._geohex_grid_frequency → geohex_grid

Precision parameters for other grid types are ignored and the configured precision is preserved.


