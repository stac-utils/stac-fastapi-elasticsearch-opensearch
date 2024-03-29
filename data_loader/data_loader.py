"""Database ingestion script."""
import json
import os
import sys

import click
import requests

if len(sys.argv) != 2:
    print("Usage: python data_loader.py <opensearch|elasticsearch>")
    sys.exit(1)

DATA_DIR = os.path.join(os.path.dirname(__file__), "setup_data/")

backend = sys.argv[1].lower()

if backend == "opensearch":
    STAC_API_BASE_URL = "http://localhost:8082"
elif backend == "elasticsearch":
    STAC_API_BASE_URL = "http://localhost:8080"
else:
    print("Invalid backend tag. Enter either 'opensearch' or 'elasticsearch'.")


def load_data(filename):
    """Load json data."""
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)


def load_collection(collection_id):
    """Load stac collection into the database."""
    collection = load_data("collection.json")
    collection["id"] = collection_id
    try:
        resp = requests.post(f"{STAC_API_BASE_URL}/collections", json=collection)
        if resp.status_code == 200:
            print(f"Status code: {resp.status_code}")
            print(f"Added collection: {collection['id']}")
        elif resp.status_code == 409:
            print(f"Status code: {resp.status_code}")
            print(f"Collection: {collection['id']} already exists")
    except requests.ConnectionError:
        click.secho("failed to connect")


def load_items():
    """Load stac items into the database."""
    feature_collection = load_data("sentinel-s2-l2a-cogs_0_100.json")
    collection = "test-collection"
    load_collection(collection)

    for feature in feature_collection["features"]:
        try:
            feature["collection"] = collection
            resp = requests.post(
                f"{STAC_API_BASE_URL}/collections/{collection}/items", json=feature
            )
            if resp.status_code == 200:
                print(f"Status code: {resp.status_code}")
                print(f"Added item: {feature['id']}")
            elif resp.status_code == 409:
                print(f"Status code: {resp.status_code}")
                print(f"Item: {feature['id']} already exists")
        except requests.ConnectionError:
            click.secho("failed to connect")


load_items()
