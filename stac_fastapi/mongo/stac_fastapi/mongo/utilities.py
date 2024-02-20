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
