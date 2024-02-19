"""API configuration."""
import os
from typing import Set

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

from stac_fastapi.types.config import ApiSettings


def _mongodb_uri() -> str:
    # MongoDB connection URI construction
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASS")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    database = os.getenv(
        "MONGO_DB", "admin"
    )  # Default to admin database for authentication
    use_ssl = os.getenv("MONGO_USE_SSL", "false").lower() == "true"
    ssl_cert_reqs = (
        "CERT_NONE"
        if os.getenv("MONGO_VERIFY_CERTS", "false").lower() == "false"
        else "CERT_REQUIRED"
    )

    # Adjust URI based on whether using SRV record or not
    if "mongodb+srv" in os.getenv("MONGO_CONNECTION_STRING", ""):
        # SRV connection string format does not use port
        uri = f"mongodb+srv://{user}:{password}@{host}/{database}?retryWrites=true&w=majority"
    else:
        # Standard connection string format with port
        uri = f"mongodb://{user}:{password}@{host}:{port}/{database}?retryWrites=true"

    if use_ssl:
        uri += f"&ssl=true&ssl_cert_reqs={ssl_cert_reqs}"

    return uri


_forbidden_fields: Set[str] = {"type"}


class MongoDBSettings(ApiSettings):
    """API settings."""

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self) -> MongoClient:
        """Create MongoDB client."""
        return MongoClient(_mongodb_uri())


class AsyncMongoDBSettings(ApiSettings):
    """API settings."""

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self) -> AsyncIOMotorClient:
        """Create async MongoDB client."""
        return AsyncIOMotorClient(_mongodb_uri())
