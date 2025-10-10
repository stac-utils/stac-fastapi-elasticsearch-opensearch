"""SFEOS CLI Tools - Utilities for managing stac-fastapi-elasticsearch-opensearch deployments.

This tool provides various utilities for managing and maintaining SFEOS deployments,
including database migrations, maintenance tasks, and more.

Usage:
    sfeos-tools add-bbox-shape --backend elasticsearch
    sfeos-tools add-bbox-shape --backend opensearch
"""

import asyncio
import logging
import sys

import click

from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def add_bbox_shape_to_collection(client, collection_doc, backend):
    """Add bbox_shape field to a single collection document.

    Args:
        client: Elasticsearch/OpenSearch client
        collection_doc: Collection document from database
        backend: Backend type ('elasticsearch' or 'opensearch')

    Returns:
        bool: True if collection was updated, False if no update was needed
    """
    collection = collection_doc["_source"]
    collection_id = collection.get("id", collection_doc["_id"])

    # Check if bbox_shape already exists
    if "bbox_shape" in collection:
        logger.info(
            f"Collection '{collection_id}' already has bbox_shape field, skipping"
        )
        return False

    # Check if collection has spatial extent
    if "extent" not in collection or "spatial" not in collection["extent"]:
        logger.warning(f"Collection '{collection_id}' has no spatial extent, skipping")
        return False

    spatial_extent = collection["extent"]["spatial"]
    if "bbox" not in spatial_extent or not spatial_extent["bbox"]:
        logger.warning(
            f"Collection '{collection_id}' has no bbox in spatial extent, skipping"
        )
        return False

    # Get the first bbox (collections can have multiple bboxes, but we use the first one)
    bbox = (
        spatial_extent["bbox"][0]
        if isinstance(spatial_extent["bbox"][0], list)
        else spatial_extent["bbox"]
    )

    if len(bbox) < 4:
        logger.warning(
            f"Collection '{collection_id}': bbox has insufficient coordinates (length={len(bbox)}), expected at least 4"
        )
        return False

    # Extract 2D coordinates (bbox can be 2D [minx, miny, maxx, maxy] or 3D [minx, miny, minz, maxx, maxy, maxz])
    # For 2D polygon, we only need the x,y coordinates and discard altitude (z) values
    minx, miny = bbox[0], bbox[1]
    if len(bbox) == 4:
        # 2D bbox: [minx, miny, maxx, maxy]
        maxx, maxy = bbox[2], bbox[3]
    else:
        # 3D bbox: [minx, miny, minz, maxx, maxy, maxz]
        # Extract indices 3,4 for maxx,maxy - discarding altitude at indices 2 (minz) and 5 (maxz)
        maxx, maxy = bbox[3], bbox[4]

    # Convert bbox to GeoJSON polygon
    bbox_polygon_coords = bbox2polygon(minx, miny, maxx, maxy)
    collection["bbox_shape"] = {
        "type": "Polygon",
        "coordinates": bbox_polygon_coords,
    }

    # Update the collection in the database
    if backend == "elasticsearch":
        await client.index(
            index=COLLECTIONS_INDEX,
            id=collection_id,
            document=collection,
            refresh=True,
        )
    else:  # opensearch
        await client.index(
            index=COLLECTIONS_INDEX,
            id=collection_id,
            body=collection,
            refresh=True,
        )

    logger.info(f"Collection '{collection_id}': Added bbox_shape field")
    return True


async def run_add_bbox_shape(backend):
    """Add bbox_shape field to all existing collections.

    Args:
        backend: Backend type ('elasticsearch' or 'opensearch')
    """
    import os

    logger.info(
        f"Starting migration: Adding bbox_shape to existing collections ({backend})"
    )

    # Log connection info (showing what will be used by the client)
    es_host = os.getenv("ES_HOST", "localhost")
    es_port = os.getenv(
        "ES_PORT", "9200"
    )  # Both backends default to 9200 in their config
    es_use_ssl = os.getenv("ES_USE_SSL", "true")
    logger.info(f"Connecting to {backend} at {es_host}:{es_port} (SSL: {es_use_ssl})")

    # Create client based on backend
    if backend == "elasticsearch":
        from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings

        settings = AsyncElasticsearchSettings()
    else:  # opensearch
        from stac_fastapi.opensearch.config import AsyncOpensearchSettings

        settings = AsyncOpensearchSettings()

    client = settings.create_client

    try:
        # Get all collections
        response = await client.search(
            index=COLLECTIONS_INDEX,
            body={
                "query": {"match_all": {}},
                "size": 10000,
            },  # Adjust size if you have more collections
        )

        total_collections = response["hits"]["total"]["value"]
        logger.info(f"Found {total_collections} collections to process")

        updated_count = 0
        skipped_count = 0

        for hit in response["hits"]["hits"]:
            was_updated = await add_bbox_shape_to_collection(client, hit, backend)
            if was_updated:
                updated_count += 1
            else:
                skipped_count += 1

        logger.info(
            f"Migration complete: {updated_count} collections updated, {skipped_count} skipped"
        )

    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        raise
    finally:
        await client.close()


@click.group()
@click.version_option(version="0.1.0", prog_name="sfeos-tools")
def cli():
    """SFEOS Tools - Utilities for managing stac-fastapi-elasticsearch-opensearch deployments."""
    pass


@cli.command("add-bbox-shape")
@click.option(
    "--backend",
    type=click.Choice(["elasticsearch", "opensearch"], case_sensitive=False),
    required=True,
    help="Database backend to use",
)
@click.option(
    "--host",
    type=str,
    default=None,
    help="Database host (default: localhost or ES_HOST env var)",
)
@click.option(
    "--port",
    type=int,
    default=None,
    help="Database port (default: 9200 for ES, 9202 for OS, or ES_PORT env var)",
)
@click.option(
    "--use-ssl/--no-ssl",
    default=None,
    help="Use SSL connection (default: true or ES_USE_SSL env var)",
)
@click.option(
    "--user",
    type=str,
    default=None,
    help="Database username (default: ES_USER env var)",
)
@click.option(
    "--password",
    type=str,
    default=None,
    help="Database password (default: ES_PASS env var)",
)
def add_bbox_shape(backend, host, port, use_ssl, user, password):
    """Add bbox_shape field to existing collections for spatial search support.

    This migration is required for collections created before spatial search
    was added. Collections created or updated after this feature will
    automatically have the bbox_shape field.

    Examples:
        sfeos_tools.py add-bbox-shape --backend elasticsearch
        sfeos_tools.py add-bbox-shape --backend opensearch --host db.example.com --port 9200
        sfeos_tools.py add-bbox-shape --backend elasticsearch --no-ssl --host localhost
    """
    import os

    # Set environment variables from CLI options if provided
    if host:
        os.environ["ES_HOST"] = host
    if port:
        os.environ["ES_PORT"] = str(port)
    if use_ssl is not None:
        os.environ["ES_USE_SSL"] = "true" if use_ssl else "false"
    if user:
        os.environ["ES_USER"] = user
    if password:
        os.environ["ES_PASS"] = password

    try:
        asyncio.run(run_add_bbox_shape(backend.lower()))
        click.echo(click.style("✓ Migration completed successfully", fg="green"))
    except KeyboardInterrupt:
        click.echo(click.style("\n✗ Migration interrupted by user", fg="yellow"))
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        click.echo(click.style(f"✗ Migration failed: {error_msg}", fg="red"))

        # Provide helpful hints for common errors
        if "TLS" in error_msg or "SSL" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: If you're connecting to a local Docker Compose instance, "
                    "try adding --no-ssl flag",
                    fg="yellow",
                )
            )
        elif "Connection refused" in error_msg:
            click.echo(
                click.style(
                    "\n💡 Hint: Make sure your database is running and accessible at the specified host:port",
                    fg="yellow",
                )
            )
        sys.exit(1)


if __name__ == "__main__":
    cli()
