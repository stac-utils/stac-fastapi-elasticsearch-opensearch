"""Database ingestion script."""
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


def load_items(base_url, collection_id):
    """Load STAC items into the database."""
    feature_collection = load_data("sentinel-s2-l2a-cogs_0_100.json")
    collection = collection_id
    load_collection(base_url, collection)

    for feature in feature_collection["features"]:
        try:
            feature["collection"] = collection
            resp = requests.post(
                f"{base_url}/collections/{collection}/items", json=feature
            )
            if resp.status_code == 200:
                click.echo(f"Status code: {resp.status_code}")
                click.echo(f"Added item: {feature['id']}")
            elif resp.status_code == 409:
                click.echo(f"Status code: {resp.status_code}")
                click.echo(f"Item: {feature['id']} already exists")
        except requests.ConnectionError:
            click.secho("Failed to connect", fg="red")


@click.command()
@click.option("--base-url", required=True, help="Base URL of the STAC API")
@click.option(
    "--collection-id",
    default="test-collection",
    help="ID of the collection to which items are added",
)
def main(base_url, collection_id):
    """Load STAC items into the database."""
    load_items(base_url, collection_id)


if __name__ == "__main__":
    main()
