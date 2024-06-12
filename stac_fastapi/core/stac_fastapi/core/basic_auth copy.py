"""Basic Authentication Module."""

import logging
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing_extensions import Annotated

_LOGGER = logging.getLogger("uvicorn.default")
_SECURITY = HTTPBasic()


class BasicAuth:

    def __init__(self, credentials: list) -> None:
        """Apply basic authentication to the provided FastAPI application \
            based on environment variables for username, password, and endpoints.

        Args:
            api (StacApi): The FastAPI application.

        Raises:
            HTTPException: If there are issues with the configuration or format
                        of the environment variables.
        """
        self.basic_auth = {}
        for credential in credentials:
            self.basic_auth[credential["username"]] = credential

    async def __call__(
        self,
        credentials: Annotated[HTTPBasicCredentials, Depends(_SECURITY)],
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
