#!/usr/bin/env python3
"""
Script to update the collections index mapping to add the bbox_shape field.

This script will:
1. Add the bbox_shape field to the existing collections index
2. Reindex all collections to populate the bbox_shape field

Usage:
    python update_collections_mapping.py
"""

import asyncio
import os
from unittest.mock import Mock

from stac_fastapi.core.serializers import CollectionSerializer
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

# Determine which backend to use
BACKEND = os.getenv("BACKEND", "elasticsearch").lower()

if BACKEND == "opensearch":
    from stac_fastapi.opensearch.config import (
        AsyncOpensearchSettings as AsyncSearchSettings,
    )
else:
    from stac_fastapi.elasticsearch.config import (
        AsyncElasticsearchSettings as AsyncSearchSettings,
    )


async def update_mapping():
    """Update the collections index mapping to add bbox_shape field."""
    settings = AsyncSearchSettings()
    client = settings.create_client

    print(f"Connecting to {BACKEND}...")

    # Check if index exists
    index_name = f"{COLLECTIONS_INDEX}-000001"
    exists = await client.indices.exists(index=index_name)

    if not exists:
        print(f"Index {index_name} does not exist. Creating it...")
        from stac_fastapi.elasticsearch.database_logic import create_collection_index

        await create_collection_index()
        print("Index created successfully!")
        return

    print(f"Index {index_name} exists. Updating mapping...")

    # Add the bbox_shape field to the mapping
    try:
        await client.indices.put_mapping(
            index=index_name, body={"properties": {"bbox_shape": {"type": "geo_shape"}}}
        )
        print("✓ Mapping updated successfully!")
    except Exception as e:
        print(f"✗ Error updating mapping: {e}")
        return

    # Now reindex all collections to populate bbox_shape
    print("\nReindexing collections to populate bbox_shape field...")

    try:
        # Get all collections
        response = await client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "size": 1000,  # Adjust if you have more collections
            },
        )

        collections = response["hits"]["hits"]
        print(f"Found {len(collections)} collections to update")

        if len(collections) == 0:
            print("No collections to update.")
            return

        # Create a mock request for the serializer
        mock_request = Mock()
        mock_request.base_url = "http://localhost:8080/"

        updated_count = 0
        error_count = 0

        for hit in collections:
            collection = hit["_source"]
            collection_id = collection.get("id", "unknown")

            try:
                # Use the serializer to convert bbox to bbox_shape
                updated_collection = CollectionSerializer.stac_to_db(
                    collection, mock_request
                )

                # Check if bbox_shape was created
                if "bbox_shape" in updated_collection:
                    # Update the document
                    await client.update(
                        index=index_name,
                        id=hit["_id"],
                        body={"doc": {"bbox_shape": updated_collection["bbox_shape"]}},
                        refresh=True,
                    )
                    print(f"  ✓ Updated collection '{collection_id}'")
                    updated_count += 1
                else:
                    print(f"  ⊘ Collection '{collection_id}' has no bbox to convert")
            except Exception as e:
                print(f"  ✗ Error updating collection '{collection_id}': {e}")
                error_count += 1

        print("\n" + "=" * 60)
        print("Summary:")
        print(f"  Total collections: {len(collections)}")
        print(f"  Successfully updated: {updated_count}")
        print(f"  Errors: {error_count}")
        print(f"  Skipped (no bbox): {len(collections) - updated_count - error_count}")
        print("=" * 60)

    except Exception as e:
        print(f"✗ Error during reindexing: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await client.close()


if __name__ == "__main__":
    print(f"Using backend: {BACKEND}")
    asyncio.run(update_mapping())
