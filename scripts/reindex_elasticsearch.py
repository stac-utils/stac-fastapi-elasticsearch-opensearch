import time

from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import create_index_templates
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX, ITEMS_INDEX_PREFIX


async def reindex():
    """Reindex all STAC indexes for mapping update"""
    client = AsyncElasticsearchSettings().create_client

    indexes = await client.indices.get_alias(name=COLLECTIONS_INDEX)
    indexes.update(await client.indices.get_alias(name=f"{ITEMS_INDEX_PREFIX}*"))

    create_index_templates()

    for index, aliases in indexes.items():
        name, version = index.rsplit("-", 1)
        new_index = f"{name}-{str(int(version) + 1).zfill(6)}"
        await client.options(ignore_status=400).indices.create(index=new_index)

        reindex_resp = await client.reindex(
            dest={"index": new_index}, source={"index": [index]}, wait_for_completion=False
        )

        task_id = reindex_resp["task"]

        old_count = await client.count(index=index)
        reindex_complete = False
        while not reindex_complete:
            task_resp = await client.tasks.get(task_id=task_id)

            if "completed" in task_resp and task_resp["completed"]:
                new_count = await client.count(index=new_index)

                if new_count["count"] == old_count["count"]:
                    reindex_complete = True

                else:
                    print(f"Reindex failed for {index} with error: mismatch count")

            elif "error" in task_resp:
                reindex_complete = True
                print(f"Reindex failed for {index} with error: {task_resp['error']}")

            time.sleep(60)

        actions = []
        for alias in aliases["aliases"]:
            actions.extend(
                [
                    {"add": {"index": new_index, "alias": alias}},
                    {"remove": {"index": index, "alias": alias}},
                ]
            )

        await client.indices.update_aliases(actions=actions)


if __name__ == "__main__":
    reindex()
