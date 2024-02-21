"""utilities for stac-fastapi.mongo."""

from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any, Dict, Iterable

from bson import ObjectId


def serialize_doc(doc):
    """Recursively convert ObjectId to string in MongoDB documents."""
    if isinstance(doc, dict):
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                doc[k] = str(v)  # Convert ObjectId to string
            elif isinstance(v, dict) or isinstance(v, list):
                doc[k] = serialize_doc(v)  # Recurse into sub-docs/lists
    elif isinstance(doc, list):
        doc = [serialize_doc(item) for item in doc]  # Apply to each item in a list
    return doc


def adapt_mongodb_docs_for_es(
    docs: Iterable[Dict[str, Any]]
) -> Iterable[Dict[str, Any]]:
    """
    Adapts MongoDB documents to mimic Elasticsearch's document structure.

    Converts ObjectId instances to strings.

    Args:
        docs (Iterable[Dict[str, Any]]): A list of dictionaries representing MongoDB documents.

    Returns:
        Iterable[Dict[str, Any]]: A list of adapted dictionaries with each original document
                                  nested under a '_source' key, and ObjectId instances converted to strings.
    """
    adapted_docs = [{"_source": serialize_doc(doc)} for doc in docs]
    return adapted_docs


def adapt_mongodb_docs_for_es_sorted(
    docs: Iterable[Dict[str, Any]]
) -> Iterable[Dict[str, Any]]:
    """
    Adapt MongoDB documents to mimic Elasticsearch's document structure.

    Args:
        docs (Iterable[Dict[str, Any]]): The original MongoDB documents.

    Returns:
        Iterable[Dict[str, Any]]: Adapted documents, each nested under a '_source' key.
    """
    adapted_docs = []
    for doc in docs:
        # Optionally, remove MongoDB's '_id' field if not needed in the output
        doc.pop("_id", None)

        adapted_doc = {
            "_source": doc,
            # Assuming 'id' is unique and can be used for sorting and pagination
            "sort": [doc["id"]],
        }
        adapted_docs.append(serialize_doc(adapted_doc))
    return adapted_docs


def decode_token(encoded_token: str) -> str:
    """Decode a base64 string back to its original token value."""
    token_value = urlsafe_b64decode(encoded_token.encode()).decode()
    return token_value


def encode_token(token_value: str) -> str:
    """Encode a token value (e.g., a UUID or cursor) as a base64 string."""
    encoded_token = urlsafe_b64encode(token_value.encode()).decode()
    return encoded_token
