"""Rate limiting middleware."""

import logging
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
    app: FastAPI,
    rate_limit: Optional[str] = None,
    get_rate_limit: Optional[str] = None,
    post_rate_limit: Optional[str] = None,
    key_func=get_remote_address,
):
    """Set up rate limiting middleware."""
    if get_rate_limit or post_rate_limit:
        if get_rate_limit:
            logger.info(f"GET rate limit set to {get_rate_limit}")
        if post_rate_limit:
            logger.info(f"POST rate limit set to {post_rate_limit}")
        _setup_custom_rate_limits(
            app, get_rate_limit, post_rate_limit, rate_limit, key_func
        )
    elif rate_limit:
        logger.info(f"Setting up rate limit with RATE_LIMIT={rate_limit}")
        _setup_global_rate_limit(app, rate_limit, key_func)
    else:
        logger.info("Rate limiting is disabled")
        return


def _setup_global_rate_limit(app: FastAPI, rate_limit: str, key_func):
    """Rate limit setup."""
    limiter = get_limiter(key_func)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

    @app.middleware("http")
    @limiter.limit(rate_limit)
    async def rate_limit_middleware(request: Request, call_next):
        response = await call_next(request)
        return response


def _setup_custom_rate_limits(
    app: FastAPI,
    get_rate_limit: Optional[str],
    post_rate_limit: Optional[str],
    rate_limit: Optional[str],
    key_func,
):
    """Set rate limits for GET, POST requests."""
    limiter = get_limiter(key_func)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

    handlers = {}

    if get_rate_limit:

        @limiter.limit(get_rate_limit)
        async def get_handler(request: Request, call_next):
            return await call_next(request)

        handlers["GET"] = get_handler

    if post_rate_limit:

        @limiter.limit(post_rate_limit)
        async def post_handler(request: Request, call_next):
            return await call_next(request)

        handlers["POST"] = post_handler

    if rate_limit:

        @limiter.limit(rate_limit)
        async def handler(request: Request, call_next):
            return await call_next(request)

        handlers["GLOBAL"] = handler

    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        method = request.method

        handler = None
        if method == "GET" and "GET" in handlers:
            handler = handlers["GET"]
        elif method == "POST" and "POST" in handlers:
            handler = handlers["POST"]
        elif "GLOBAL" in handlers:
            handler = handlers["GLOBAL"]

        if handler:
            return await handler(request, call_next)
        else:
            return await call_next(request)
