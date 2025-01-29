"""Rate limiting middleware."""

import logging
import os
from typing import Optional

from fastapi import FastAPI, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)


def get_limiter(key_func=get_remote_address):
    """Create and return a Limiter instance for rate limiting."""
    return Limiter(key_func=key_func)


def setup_rate_limit(
    app: FastAPI, rate_limit: Optional[str] = None, key_func=get_remote_address
):
    """Set up rate limiting middleware."""
    RATE_LIMIT = rate_limit or os.getenv("STAC_FASTAPI_RATE_LIMIT")

    if not RATE_LIMIT:
        logger.info("Rate limiting is disabled")
        return

    logger.info(f"Setting up rate limit with RATE_LIMIT={RATE_LIMIT}")

    limiter = get_limiter(key_func)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

    @app.middleware("http")
    @limiter.limit(RATE_LIMIT)
    async def rate_limit_middleware(request: Request, call_next):
        response = await call_next(request)
        return response

    logger.info("Rate limit setup complete")
