"""Entity tags and If-Match preconditions for single items and collections.

A resource's ETag names the stored version of its document: the primary term
and sequence number that Elasticsearch and OpenSearch give every write, and a
hash of the index that holds it, since sequence numbers count per index and an
item created again with another datetime lands in another index. An update or delete that carries
``If-Match`` is written with ``if_seq_no`` and ``if_primary_term`` of the
version that matched, so the check and the write are one atomic step (RFC 9110,
sections 8.8.3 and 13.1.1).

The tag covers the stored document, not the links a response generates from
other documents (a catalog's child links), so it is for ``If-Match`` only; the
server does not evaluate ``If-None-Match`` or ``If-Range``.

A compressed response gets the content coding appended to its tag, since two
representations that differ in content coding need different strong tags
(RFC 9110, section 8.8.1). Both tags name the same version for If-Match.
"""

import hashlib
import re
from typing import Any, Awaitable, Callable

import attr
from fastapi import HTTPException
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from stac_fastapi.core.utilities import get_bool_env

# Empty list elements are ignored, as RFC 9110, section 5.6.1.2 asks.
ENTITY_TAG = re.compile(r'[\s,]*(W/)?("[\x21\x23-\x7e\x80-\xff]*")\s*(?:,|$)')
CODED_TAG = re.compile(r'"(\d+-\d+-[0-9a-f]{8})-[a-z][a-z0-9.]*"')


@attr.s(frozen=True, auto_attribs=True)
class DocumentVersion:
    """The stored version of a document, as the database reports it."""

    index: str
    seq_no: int
    primary_term: int

    @classmethod
    def from_hit(cls, hit: dict) -> "DocumentVersion":
        """Read the version from a get or search hit, or a write response."""
        return cls(
            index=hit["_index"],
            seq_no=hit["_seq_no"],
            primary_term=hit["_primary_term"],
        )

    @property
    def etag(self) -> str:
        """Return the strong entity tag of this version."""
        # The index name, not its UUID: an index deleted and created again under
        # the same name restarts its sequence numbers.
        index = hashlib.blake2b(self.index.encode(), digest_size=4).hexdigest()
        return f'"{self.primary_term}-{self.seq_no}-{index}"'

    def write_kwargs(self) -> dict[str, Any]:
        """Client arguments that make a write conditional on this version."""
        return {"if_seq_no": self.seq_no, "if_primary_term": self.primary_term}


def if_match_holds(if_match: str, etag: str) -> bool:
    """Evaluate an If-Match field value against the current ETag.

    ``*`` matches any current representation. Otherwise the value must list the
    ETag; weak tags never match, since If-Match uses strong comparison. A value
    that does not parse matches nothing.
    """
    if_match = if_match.strip(" \t,")
    if if_match == "*":
        return True
    tags, position = [], 0
    while position < len(if_match):
        match = ENTITY_TAG.match(if_match, position)
        if match is None:
            return False
        if not match.group(1):
            coded = CODED_TAG.fullmatch(match.group(2))
            tags.append(f'"{coded.group(1)}"' if coded else match.group(2))
        position = match.end()
    return etag in tags


def precondition_failed(resource: str) -> HTTPException:
    """Return the 412 raised when a conditional write finds a newer version."""
    return HTTPException(
        status_code=412,
        detail=f"{resource} has changed since the ETag in If-Match; "
        "fetch it again and retry with its current ETag.",
    )


def if_match_header(request: Any, resource: str) -> str | None:
    """Return the request's If-Match value, or None when it has none.

    Raises 428 when ``REQUIRE_IF_MATCH`` is on and the header is missing.
    """
    headers = getattr(request, "headers", None) or {}
    if_match = headers.get("if-match")
    if if_match is None and get_bool_env("REQUIRE_IF_MATCH", default=False):
        raise HTTPException(
            status_code=428,
            detail=f"Updating or deleting {resource} requires an If-Match "
            "header with its current ETag, which a GET returns.",
        )
    return if_match


def check_etag(
    if_match: str | None, current: DocumentVersion, resource: str
) -> DocumentVersion | None:
    """Return the version a write must be conditional on, or None without If-Match.

    Raises 412 when If-Match does not match the current version.
    """
    if if_match is None:
        return None
    if not if_match_holds(if_match, current.etag):
        raise precondition_failed(resource)
    return current


async def check_if_match(
    request: Any,
    resource: str,
    read: Callable[[], Awaitable[tuple[Any, DocumentVersion]]],
) -> DocumentVersion | None:
    """Evaluate If-Match before an update or delete that has not read the resource.

    `read` returns the stored document and its version; it is awaited only when
    the request has If-Match, so its NotFoundError gives a missing resource 404.
    """
    if_match = if_match_header(request, resource)
    if if_match is None:
        return None
    _, current = await read()
    return check_etag(if_match, current, resource)


def record_etag(request: Any, version: DocumentVersion | None) -> None:
    """Remember the ETag for the response; ETagMiddleware adds the header."""
    state = getattr(request, "state", None)
    if version is not None and state is not None:
        state.etag = version.etag


class ETagMiddleware:
    """Add the ETag a handler recorded with `record_etag` to its 2xx response.

    If the response is compressed, the content coding is appended to the tag.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the application."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Add the recorded ETag once the response starts."""
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        state = scope.setdefault("state", {})

        async def send_with_etag(message: Message) -> None:
            if (
                message["type"] == "http.response.start"
                and 200 <= message["status"] < 300
                and state.get("etag")
            ):
                headers = MutableHeaders(scope=message)
                etag = state["etag"]
                coding = headers.get("content-encoding", "identity").strip().lower()
                if coding != "identity":
                    etag = f'{etag[:-1]}-{re.sub("[^a-z0-9]+", ".", coding)}"'
                headers.setdefault("etag", etag)
            await send(message)

        await self.app(scope, receive, send_with_etag)


# Routes whose single-resource responses carry an ETag, and routes that take
# If-Match, for the OpenAPI document.
ETAG_ROUTES: dict[str, tuple[str, ...]] = {
    "/collections": ("post",),
    "/collections/{collection_id}": ("get", "put", "patch"),
    "/collections/{collection_id}/items": ("post",),
    "/collections/{collection_id}/items/{item_id}": ("get", "put", "patch"),
    "/catalogs": ("post",),
    "/catalogs/{catalog_id}": ("get", "put"),
    "/catalogs/{catalog_id}/catalogs": ("post",),
    "/catalogs/{catalog_id}/collections": ("post",),
    "/catalogs/{catalog_id}/collections/{collection_id}": ("get", "put"),
    "/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}": ("get",),
}
IF_MATCH_ROUTES: dict[str, tuple[str, ...]] = {
    "/collections/{collection_id}": ("put", "patch", "delete"),
    "/collections/{collection_id}/items/{item_id}": ("put", "patch", "delete"),
    "/catalogs/{catalog_id}": ("put", "delete"),
    "/catalogs/{catalog_id}/collections/{collection_id}": ("put",),
}
PRECONDITION_REQUIRED_ROUTES: dict[str, tuple[str, ...]] = {
    "/collections/{collection_id}/bulk_items": ("post",)
}


def document_conditional_requests(app: Any) -> None:
    """Describe ETag, If-Match, 412 and 428 in the application's OpenAPI document."""
    build = app.openapi

    def operations(routes: dict[str, tuple[str, ...]], paths: dict) -> list[dict]:
        return [
            paths[path][method]
            for path, methods in routes.items()
            for method in methods
            if method in paths.get(path, {})
        ]

    def openapi() -> dict:
        if app.openapi_schema:
            return app.openapi_schema
        schema = build()
        paths = schema.get("paths", {})
        for operation in operations(ETAG_ROUTES, paths):
            for status, response in operation.get("responses", {}).items():
                if status.startswith("2"):
                    response.setdefault("headers", {})["ETag"] = {
                        "description": "The strong entity tag of the stored version "
                        "of a single item, collection or catalog; send it in "
                        "If-Match to update or delete that version.",
                        "schema": {"type": "string"},
                    }
        for operation in operations(IF_MATCH_ROUTES, paths):
            operation.setdefault("parameters", []).append(
                {
                    "name": "If-Match",
                    "in": "header",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Write only if the resource's current ETag is "
                    "listed (RFC 9110, section 13.1.1); `*` matches any.",
                }
            )
            operation.setdefault("responses", {})["412"] = {
                "description": "Precondition Failed: the resource has changed "
                "since the ETag in If-Match; nothing was written."
            }
            operation["responses"]["428"] = {
                "description": "Precondition Required: REQUIRE_IF_MATCH is on and "
                "the request has no If-Match header."
            }
        for operation in operations(PRECONDITION_REQUIRED_ROUTES, paths):
            operation.setdefault("responses", {})["428"] = {
                "description": "Precondition Required: REQUIRE_IF_MATCH is on and "
                "a bulk upsert cannot carry If-Match; use method 'insert'."
            }
        app.openapi_schema = schema
        return schema

    app.openapi = openapi
