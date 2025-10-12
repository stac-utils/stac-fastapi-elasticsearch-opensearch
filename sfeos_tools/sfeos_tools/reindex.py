"""Reindex ES/OS database for mapping update."""

import asyncio
import time
from typing import Any, Dict

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX, ITEMS_INDEX_PREFIX


async def _reindex_single_index(
    client, index: str, new_index: str, aliases: Dict[str, Any]
):
    """Reindex a single index to a new version and switch aliases."""
    print(f"reindexing {index} to {new_index}")

    await client.options(ignore_status=400).indices.create(index=new_index)

    # Asset migration script for data compatibility
    script = {
        "source": (
            "if (ctx._source.containsKey('assets')){List l = new ArrayList();"
            "for (key in ctx._source.assets.keySet()) {def item = ctx._source.assets[key];"
            " item['es_key'] = key; l.add(item)}ctx._source.assets=l} "
            "if (ctx._source.containsKey('item_assets')){ List a = new ArrayList();"
            " for (key in ctx._source.item_assets.keySet()) {def item = ctx._source.item_assets[key];"
            " item['es_key'] = key; a.add(item)}ctx._source.item_assets=a}"
        ),
        "lang": "painless",
    }

    reindex_resp = await client.reindex(
        dest={"index": new_index},
        source={"index": [index]},
        wait_for_completion=False,
        script=script,
    )

    task_id = reindex_resp["task"]

    reindex_complete = False
    while not reindex_complete:
        task_resp = await client.tasks.get(task_id=task_id)

        if "completed" in task_resp and task_resp["completed"]:
            reindex_complete = True
        elif "error" in task_resp:
            reindex_complete = True
            print(f"Reindex failed for {index} with error: {task_resp['error']}")
        else:
            time.sleep(60)

    actions = []
    for alias in aliases["aliases"]:
        actions.extend(
            [
                {"add": {"index": new_index, "alias": alias}},
                {"remove": {"index": index, "alias": alias}},
            ]
        )

    if actions:
        await client.indices.update_aliases(actions=actions)


async def run(backend: str = "elasticsearch"):
    """Reindex all STAC indexes for mapping update for the given backend.

    backend: 'elasticsearch' or 'opensearch'
    """
    backend = backend.lower()

    # Lazy imports so the package dependencies remain optional per backend
    if backend == "elasticsearch":
        from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
        from stac_fastapi.elasticsearch.database_logic import create_index_templates

        settings = AsyncElasticsearchSettings()
    elif backend == "opensearch":
        from stac_fastapi.opensearch.config import AsyncOpensearchSettings
        from stac_fastapi.opensearch.database_logic import create_index_templates

        settings = AsyncOpensearchSettings()
    else:
        raise ValueError(f"Unsupported backend: {backend}")

    client = settings.create_client

    try:
        # Ensure latest templates are applied
        await create_index_templates()

        # Collections index
        collection_response = await client.indices.get_alias(name=COLLECTIONS_INDEX)
        collections = await client.search(index=COLLECTIONS_INDEX)

        collection_index, collection_aliases = next(iter(collection_response.items()))
        collection_index_name, version = collection_index.rsplit("-", 1)
        new_collection_index = (
            f"{collection_index_name}-{str(int(version) + 1).zfill(6)}"
        )

        await _reindex_single_index(
            client, collection_index, new_collection_index, collection_aliases
        )

        # Items per collection
        for collection in collections["hits"]["hits"]:
            item_indexes = await client.indices.get_alias(
                name=f"{ITEMS_INDEX_PREFIX}{collection['_id']}*"
            )

            for item_index, aliases in item_indexes.items():
                item_index_name, version = item_index.rsplit("-", 1)
                new_item_index = f"{item_index_name}-{str(int(version) + 1).zfill(6)}"

                await _reindex_single_index(client, item_index, new_item_index, aliases)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(run("elasticsearch"))
