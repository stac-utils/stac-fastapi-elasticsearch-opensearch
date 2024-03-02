"""utilities for stac-fastapi.mongo."""

from base64 import urlsafe_b64decode, urlsafe_b64encode

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


def decode_token(encoded_token: str) -> str:
    """Decode a base64 string back to its original token value."""
    token_value = urlsafe_b64decode(encoded_token.encode()).decode()
    return token_value


def encode_token(token_value: str) -> str:
    """Encode a token value (e.g., a UUID or cursor) as a base64 string."""
    encoded_token = urlsafe_b64encode(token_value.encode()).decode()
    return encoded_token
