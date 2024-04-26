"""Basic Authentication Module."""

import json
import os
import secrets
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request, status
from fastapi.routing import APIRoute
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing_extensions import Annotated

from stac_fastapi.api.app import StacApi

security = HTTPBasic()

_BASIC_AUTH: Dict[str, Any] = {}


def has_access(
    request: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)]
) -> str:
    """Check if the provided credentials match the expected \
        username and password stored in environment variables for basic authentication.

    Args:
        request (Request): The FastAPI request object.
        credentials (HTTPBasicCredentials): The HTTP basic authentication credentials.

    Returns:
        str: The username if authentication is successful.

    Raises:
        HTTPException: If authentication fails due to incorrect username or password.
    """
    global _BASIC_AUTH

    users = _BASIC_AUTH.get("users")
    user: Dict[str, Any] = next(
        (u for u in users if u.get("username") == credentials.username), {}
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    # Compare the provided username and password with the correct ones using compare_digest
    if not secrets.compare_digest(
        credentials.username.encode("utf-8"), user.get("username").encode("utf-8")
    ) or not secrets.compare_digest(
        credentials.password.encode("utf-8"), user.get("password").encode("utf-8")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    permissions = user.get("permissions", [])
    path = request.url.path
    method = request.method

    if permissions == "*":
        return credentials.username
    for permission in permissions:
        if permission["path"] == path and method in permission.get("method", []):
            return credentials.username

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Insufficient permissions for [{method} {path}]",
    )


def apply_basic_auth(api: StacApi) -> None:
    """Apply basic authentication to the provided FastAPI application \
        based on environment variables for username, password, and endpoints.

    Args:
        api (StacApi): The FastAPI application.

    Raises:
        HTTPException: If there are issues with the configuration or format
                       of the environment variables.
    """
    global _BASIC_AUTH

    basic_auth_json_str = os.environ.get("BASIC_AUTH")
    if not basic_auth_json_str:
        print("Basic authentication disabled.")
        return

    try:
        _BASIC_AUTH = json.loads(basic_auth_json_str)
    except json.JSONDecodeError as exception:
        print(f"Invalid JSON format for BASIC_AUTH. {exception=}")
        raise
    public_endpoints = _BASIC_AUTH.get("public_endpoints", [])
    users = _BASIC_AUTH.get("users")
    if not users:
        raise Exception("Invalid JSON format for BASIC_AUTH. Key 'users' undefined.")

    app = api.app
    for route in app.routes:
        if isinstance(route, APIRoute):
            for method in route.methods:
                endpoint = {"path": route.path, "method": method}
                if endpoint not in public_endpoints:
                    api.add_route_dependencies([endpoint], [Depends(has_access)])

    print("Basic authentication enabled.")
