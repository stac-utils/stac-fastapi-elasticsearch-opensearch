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

**Adding aggregations through configuration**

The aggregations the server can run are defined in AGGREGATION_MAPPING. Additional ones can be declared without changing the code:

`STAC_FASTAPI_ES_CUSTOM_AGGREGATIONS`: a JSON object mapping an aggregation name to an Elasticsearch/OpenSearch aggregation body.
`STAC_FASTAPI_ES_AGGREGATIONS_FILE`: path to a JSON file with the same content, used when the variable above is not set.
```json
{
  "asset_file_size_sum": {"sum": {"field": "assets.file:size"}},
  "platform_family_frequency": {"terms": {"field": "properties.platform", "size": 100}}
}
```

Each entry must hold exactly one aggregation type. An invalid entry is logged and ignored.
An entry with the name of a built-in aggregation replaces it.
The aggregation must be listed in the aggregations field of each collection that uses it (name, data_type and, for a distribution, frequency_distribution_data_type). It is not listed by the root /aggregations.
A name ending with `_frequency` is returned as a frequency distribution (buckets); any other name is returned as a single value.
For a `date_histogram`, the interval comes from the `datetime_frequency_interval` request parameter.
These definitions come from the server configuration, never from a request; they can use any feature the backend supports, including scripts, so only trusted operators should set them.


