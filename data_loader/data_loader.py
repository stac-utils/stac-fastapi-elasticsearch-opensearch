"""Data Loader CLI tool."""
import json
import os

import click
import requests

# Define the directory where your data files are located
DATA_DIR = os.path.join(os.path.dirname(__file__), "setup_data/")


def load_data(filename):
    """Load json data from a file."""
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)


def load_collection(base_url, collection_id):
    """Load a STAC collection into the database."""
    collection = load_data("collection.json")
    collection["id"] = collection_id
    try:
        resp = requests.post(f"{base_url}/collections", json=collection)
        if resp.status_code == 200:
            click.echo(f"Status code: {resp.status_code}")
            click.echo(f"Added collection: {collection['id']}")
        elif resp.status_code == 409:
            click.echo(f"Status code: {resp.status_code}")
            click.echo(f"Collection: {collection['id']} already exists")
    except requests.ConnectionError:
        click.secho("Failed to connect", fg="red")


def load_items(base_url, collection_id, use_bulk):
    """Load STAC items into the database based on the method selected."""
    feature_collection = load_data("sentinel-s2-l2a-cogs_0_100.json")
    load_collection(base_url, collection_id)
    if use_bulk:
        load_items_bulk_insert(base_url, collection_id, feature_collection)
    else:
        load_items_one_by_one(base_url, collection_id, feature_collection)


def load_items_one_by_one(base_url, collection_id, feature_collection):
    """Load STAC items into the database one by one."""
    for feature in feature_collection["features"]:
        try:
            feature["collection"] = collection_id
            resp = requests.post(
                f"{base_url}/collections/{collection_id}/items", json=feature
            )
            if resp.status_code == 200:
                click.echo(f"Status code: {resp.status_code}")
                click.echo(f"Added item: {feature['id']}")
            elif resp.status_code == 409:
                click.echo(f"Status code: {resp.status_code}")
                click.echo(f"Item: {feature['id']} already exists")
        except requests.ConnectionError:
            click.secho("Failed to connect", fg="red")


def load_items_bulk_insert(base_url, collection_id, feature_collection):
    """Load STAC items into the database via bulk insert."""
    try:
        for i, _ in enumerate(feature_collection["features"]):
            feature_collection["features"][i]["collection"] = collection_id
        resp = requests.post(
            f"{base_url}/collections/{collection_id}/items", json=feature_collection
        )  # Adjust this endpoint as necessary
        if resp.status_code == 200:
            click.echo(f"Status code: {resp.status_code}")
            click.echo("Bulk inserted items successfully.")
        elif resp.status_code == 204:
            click.echo(f"Status code: {resp.status_code}")
            click.echo("Bulk update successful, no content returned.")
        elif resp.status_code == 409:
            click.echo(f"Status code: {resp.status_code}")
            click.echo("Conflict detected, some items might already exist.")
    except requests.ConnectionError:
        click.secho("Failed to connect", fg="red")


@click.command()
@click.option("--base-url", required=True, help="Base URL of the STAC API")
@click.option(
    "--collection-id",
    default="test-collection",
    help="ID of the collection to which items are added",
)
@click.option("--use-bulk", is_flag=True, help="Use bulk insert method for items")
def main(base_url, collection_id, use_bulk):
    """Load STAC items into the database."""
    load_items(base_url, collection_id, use_bulk)


if __name__ == "__main__":
    main()
