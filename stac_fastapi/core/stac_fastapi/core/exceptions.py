"""Custom exceptions for STAC FastAPI.

Provides exception classes and FastAPI-compatible handlers for scenarios
that require non-standard HTTP responses (e.g., 202 Accepted for
background-queue operations).
"""

from fastapi import Request
from fastapi.responses import JSONResponse


class QueuedSuccess(Exception):
    """Raised when an item is successfully pushed to the background queue.

    The `payload` attribute contains the response body that the FastAPI
    exception handler should serialize as a ``202 Accepted`` JSON response.
    """

    def __init__(self, payload: dict | str):
        """Initialize with the response payload.

        Args:
            payload: The response body to serialize as JSON.
        """
        self.payload = payload
        super().__init__(str(payload))


async def queued_success_handler(request: Request, exc: QueuedSuccess) -> JSONResponse:
    """Catch :class:`QueuedSuccess` and format it as a ``202 Accepted`` response.

    Args:
        request: The current FastAPI request.
        exc: The :class:`QueuedSuccess` exception carrying the response payload.

    Returns:
        A ``JSONResponse`` with status code ``202`` and the queued payload.
    """
    return JSONResponse(
        status_code=202,
        content=exc.payload
        if isinstance(exc.payload, dict)
        else {"message": exc.payload},
    )
