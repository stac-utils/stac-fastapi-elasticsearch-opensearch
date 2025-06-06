"""Data Loader CLI STAC_API Ingestion Tool."""

import os
from typing import Any

import click
import orjson
from httpx import Client


def load_data(data_dir: str, filename: str) -> dict[str, Any]:
    """Load json data from a file within the specified data directory."""
    filepath = os.path.join(data_dir, filename)
    if not os.path.exists(filepath):
        click.secho(f"File not found: {filepath}", fg="red", err=True)
        raise click.Abort()
    with open(filepath, "rb") as file:
        return orjson.loads(file.read())


def load_collection(client: Client, collection_id: str, data_dir: str) -> None:
    """Load a STAC collection into the database."""
    collection = load_data(data_dir, "collection.json")
    collection["id"] = collection_id
    resp = client.post("/collections", json=collection)
    if resp.status_code == 200 or resp.status_code == 201:
        click.echo(f"Status code: {resp.status_code}")
        click.echo(f"Added collection: {collection['id']}")
    elif resp.status_code == 409:
        click.echo(f"Status code: {resp.status_code}")
        click.echo(f"Collection: {collection['id']} already exists")
    else:
        click.echo(f"Status code: {resp.status_code}")
        click.echo(f"Error writing {collection['id']} collection. Message: {resp.text}")


def load_items(
    client: Client, collection_id: str, use_bulk: bool, data_dir: str
) -> None:
    """Load STAC items into the database based on the method selected."""
    # Attempt to dynamically find a suitable feature collection file
    feature_files = [
        file
        for file in os.listdir(data_dir)
        if file.endswith(".json") and file != "collection.json"
    ]
    if not feature_files:
        click.secho(
            "No feature collection files found in the specified directory.",
            fg="red",
            err=True,
        )
        raise click.Abort()

    # Use the first found feature collection file
    feature_collection = load_data(data_dir, feature_files[0])

    load_collection(client, collection_id, data_dir)
    if use_bulk:
        load_items_bulk_insert(client, collection_id, feature_collection)
    else:
        load_items_one_by_one(client, collection_id, feature_collection)


def load_items_one_by_one(
    client: Client, collection_id: str, feature_collection: dict[str, Any]
) -> None:
    """Load STAC items into the database one by one."""
    for feature in feature_collection["features"]:
        feature["collection"] = collection_id
        resp = client.post(f"/collections/{collection_id}/items", json=feature)
        if resp.status_code == 200:
            click.echo(f"Status code: {resp.status_code}")
            click.echo(f"Added item: {feature['id']}")
        elif resp.status_code == 409:
            click.echo(f"Status code: {resp.status_code}")
            click.echo(f"Item: {feature['id']} already exists")


def load_items_bulk_insert(
    client: Client, collection_id: str, feature_collection: dict[str, Any]
) -> None:
    """Load STAC items into the database via bulk insert."""
    for feature in feature_collection["features"]:
        feature["collection"] = collection_id
    resp = client.post(f"/collections/{collection_id}/items", json=feature_collection)
    if resp.status_code == 200:
        click.echo(f"Status code: {resp.status_code}")
        click.echo("Bulk inserted items successfully.")
    elif resp.status_code == 204:
        click.echo(f"Status code: {resp.status_code}")
        click.echo("Bulk update successful, no content returned.")
    elif resp.status_code == 409:
        click.echo(f"Status code: {resp.status_code}")
        click.echo("Conflict detected, some items might already exist.")


@click.command()
@click.option("--base-url", required=True, help="Base URL of the STAC API")
@click.option(
    "--collection-id",
    default="test-collection",
    help="ID of the collection to which items are added",
)
@click.option("--use-bulk", is_flag=True, help="Use bulk insert method for items")
@click.option(
    "--data-dir",
    type=click.Path(exists=True),
    default="sample_data/",
    help="Directory containing collection.json and feature collection file",
)
def main(base_url: str, collection_id: str, use_bulk: bool, data_dir: str) -> None:
    """Load STAC items into the database."""
    with Client(base_url=base_url) as client:
        load_items(client, collection_id, use_bulk, data_dir)


if __name__ == "__main__":
    main()
