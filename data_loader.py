import click
import os
import json
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "setup_data/")
STAC_API_BASE_URL = "http://localhost:8083"

def load_data(filename):
    with open(os.path.join(DATA_DIR, filename)) as file:
        return json.load(file)

def load_collection(collection_id):
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
    feature_collection = load_data("sentinel-s2-l2a-cogs_0_100.json")
    collection = "test-collection"
    load_collection(collection)

    for feature in feature_collection["features"]:
        try:
            feature["stac_extensions"] = []
            feature["stac_version"] = "1.0.0"
            feature["collection"] = "test-collection"
            resp = requests.post(f"{STAC_API_BASE_URL}/collections/{collection}/items", json=feature)
            
            if resp.status_code == 200:
                print(f"Status code: {resp.status_code}")
                print(f"Added item: {feature['id']}")
            elif resp.status_code == 409:
                print(f"Status code: {resp.status_code}")
                print(f"Item: {feature['id']} already exists")
        except requests.ConnectionError:
            click.secho("failed to connect")

load_items()
