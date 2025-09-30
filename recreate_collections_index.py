#!/usr/bin/env python3
"""
Script to delete and recreate the collections index.

WARNING: This will DELETE all existing collections!
Only use this in development environments.

Usage:
    python recreate_collections_index.py
"""

import asyncio
import os
import sys

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

# Determine which backend to use
BACKEND = os.getenv("BACKEND", "elasticsearch").lower()

if BACKEND == "opensearch":
    from stac_fastapi.opensearch.config import (
        AsyncOpensearchSettings as AsyncSearchSettings,
    )
    from stac_fastapi.opensearch.database_logic import (
        create_collection_index,
        create_index_templates,
    )
else:
    from stac_fastapi.elasticsearch.config import (
        AsyncElasticsearchSettings as AsyncSearchSettings,
    )
    from stac_fastapi.elasticsearch.database_logic import (
        create_collection_index,
        create_index_templates,
    )


async def recreate_index():
    """Delete and recreate the collections index."""
    settings = AsyncSearchSettings()
    client = settings.create_client

    print(f"Using backend: {BACKEND}")
    print(f"\n{'=' * 60}")
    print("WARNING: This will DELETE all existing collections!")
    print(f"{'=' * 60}\n")

    # Check if running in production
    env = os.getenv("ENVIRONMENT", "development").lower()
    if env == "production":
        print("ERROR: This script should not be run in production!")
        print("Use update_collections_mapping.py instead.")
        sys.exit(1)

    response = input("Are you sure you want to continue? (yes/no): ")
    if response.lower() != "yes":
        print("Aborted.")
        sys.exit(0)

    try:
        # Delete the collections index
        index_name = f"{COLLECTIONS_INDEX}-000001"
        alias_name = COLLECTIONS_INDEX

        print(f"\nDeleting index {index_name}...")
        exists = await client.indices.exists(index=index_name)
        if exists:
            await client.indices.delete(index=index_name)
            print(f"✓ Deleted index {index_name}")
        else:
            print(f"⊘ Index {index_name} does not exist")

        # Check if alias exists and delete it
        alias_exists = await client.indices.exists_alias(name=alias_name)
        if alias_exists:
            print(f"Deleting alias {alias_name}...")
            await client.indices.delete_alias(
                index="_all", name=alias_name, ignore=[404]
            )
            print(f"✓ Deleted alias {alias_name}")

        # Recreate index templates
        print("\nRecreating index templates...")
        await create_index_templates()
        print("✓ Index templates created")

        # Recreate the collections index
        print("\nRecreating collections index...")
        await create_collection_index()
        print("✓ Collections index created")

        # Verify the mapping includes bbox_shape
        print("\nVerifying mapping...")
        mapping = await client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]["mappings"]["properties"]

        if "bbox_shape" in properties:
            print(
                f"✓ bbox_shape field is present in mapping: {properties['bbox_shape']}"
            )
        else:
            print("✗ WARNING: bbox_shape field is NOT in the mapping!")

        print("\n" + "=" * 60)
        print("Collections index successfully recreated!")
        print("You can now create collections with bbox_shape support.")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(recreate_index())
