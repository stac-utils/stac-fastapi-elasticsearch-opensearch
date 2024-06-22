"""Basic Authentication Module."""

import logging
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing_extensions import Annotated

_LOGGER = logging.getLogger("uvicorn.default")
_SECURITY = HTTPBasic()


class BasicAuth:
    """Apply basic authentication to the provided FastAPI application \
    based on environment variables for username, password, and endpoints."""

    def __init__(self, credentials: list) -> None:
        """Generate basic_auth property."""
        self.basic_auth = {}
        for credential in credentials:
            self.basic_auth[credential["username"]] = credential

    async def __call__(
        self,
        credentials: Annotated[HTTPBasicCredentials, Depends(_SECURITY)],
    ) -> str:
        """Check if the provided credentials match the expected \
            username and password stored in basic_auth.

        Args:
            credentials (HTTPBasicCredentials): The HTTP basic authentication credentials.

        Returns:
            str: The username if authentication is successful.

        Raises:
            HTTPException: If authentication fails due to incorrect username or password.
        """
        user = self.basic_auth.get(credentials.username)

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

        return credentials.username
