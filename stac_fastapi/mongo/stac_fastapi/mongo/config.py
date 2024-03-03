"""API configuration."""
import os
import ssl
from typing import Any, Dict, Set

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

from stac_fastapi.types.config import ApiSettings


def _mongodb_config() -> Dict[str, Any]:
    # MongoDB connection URI and client options
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASS")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    # database = os.getenv("MONGO_DB", "stac")  # Default to 'stac' database
    use_ssl = os.getenv("MONGO_USE_SSL", "false").lower() == "true"
    verify_certs = os.getenv("MONGO_VERIFY_CERTS", "true").lower() == "true"

    ssl_cert_reqs = ssl.CERT_REQUIRED if verify_certs else ssl.CERT_NONE

    # Adjust URI based on whether using SRV record or not
    # if "mongodb+srv" in os.getenv("MONGO_CONNECTION_STRING", ""):
    #     uri = f"mongodb+srv://{user}:{password}@{host}/{database}?retryWrites=true&w=majority"
    # else:
    #     uri = f"mongodb://{user}:{password}@{host}:{port}/{database}?retryWrites=true"

    if "mongodb+srv" in os.getenv("MONGO_CONNECTION_STRING", ""):
        uri = f"mongodb+srv://{user}:{password}@{host}?retryWrites=true&w=majority"
    else:
        uri = f"mongodb://{user}:{password}@{host}:{port}?retryWrites=true"

    if use_ssl:
        uri += "&ssl=true&ssl_cert_reqs={}".format(ssl_cert_reqs)

    # Initialize the configuration dictionary
    config = {
        "uri": uri,
        # "database": database,
        # MongoDB does not use headers, but added here for structure alignment
        "headers": {},  # Placeholder for consistency
    }

    return config


_forbidden_fields: Set[str] = {"type"}


class MongoDBSettings(ApiSettings):
    """MongoDB specific API settings."""

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self) -> MongoClient:
        """Create a synchronous MongoDB client."""
        config = _mongodb_config()
        return MongoClient(config["uri"])


class AsyncMongoDBSettings(ApiSettings):
    """Async MongoDB specific API settings."""

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self) -> AsyncIOMotorClient:
        """Create an asynchronous MongoDB client."""
        config = _mongodb_config()
        print(config)
        return AsyncIOMotorClient(config["uri"])
