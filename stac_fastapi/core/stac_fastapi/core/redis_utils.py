"""Utilities for connecting to and managing Redis connections."""

import logging
from typing import Dict, List, Optional

from fastapi import Request
from pydantic_settings import BaseSettings
from redis import asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

logger = logging.getLogger(__name__)

redis_pool: Optional[aioredis.Redis] = None


class RedisSentinelSettings(BaseSettings):
    """Configuration for connecting to Redis Sentinel."""

    REDIS_SENTINEL_HOSTS: str = ""
    REDIS_SENTINEL_PORTS: str = "26379"
    REDIS_SENTINEL_MASTER_NAME: str = "master"
    REDIS_DB: int = 15

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_RETRY_TIMEOUT: bool = True
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30


class RedisSettings(BaseSettings):
    """Configuration for connecting Redis Sentinel."""

    REDIS_HOST: str = ""
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_RETRY_TIMEOUT: bool = True
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30


# Select the Redis or Redis Sentinel configuration
redis_settings: BaseSettings = RedisSettings()


async def connect_redis(settings: Optional[RedisSettings] = None) -> aioredis.Redis:
    """Return a Redis connection."""
    global redis_pool
    settings = settings or redis_settings

    if not settings.REDIS_HOST or not settings.REDIS_PORT:
        return None

    if redis_pool is None:
        pool = aioredis.ConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=settings.REDIS_DECODE_RESPONSES,
            retry_on_timeout=settings.REDIS_RETRY_TIMEOUT,
            health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
        )
        redis_pool = aioredis.Redis(
            connection_pool=pool, client_name=settings.REDIS_CLIENT_NAME
        )
    return redis_pool


async def connect_redis_sentinel(
    settings: Optional[RedisSentinelSettings] = None,
) -> Optional[aioredis.Redis]:
    """Return a Redis Sentinel connection."""
    global redis_pool

    settings = settings or redis_settings

    if (
        not settings.REDIS_SENTINEL_HOSTS
        or not settings.REDIS_SENTINEL_PORTS
        or not settings.REDIS_SENTINEL_MASTER_NAME
    ):
        return None

    hosts = [h.strip() for h in settings.REDIS_SENTINEL_HOSTS.split(",") if h.strip()]
    ports = [
        int(p.strip()) for p in settings.REDIS_SENTINEL_PORTS.split(",") if p.strip()
    ]

    if redis_pool is None:
        try:
            sentinel = Sentinel(
                [(h, p) for h, p in zip(hosts, ports)],
                decode_responses=settings.REDIS_DECODE_RESPONSES,
            )
            master = sentinel.master_for(
                service_name=settings.REDIS_SENTINEL_MASTER_NAME,
                db=settings.REDIS_DB,
                decode_responses=settings.REDIS_DECODE_RESPONSES,
                retry_on_timeout=settings.REDIS_RETRY_TIMEOUT,
                client_name=settings.REDIS_CLIENT_NAME,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
            )
            redis_pool = master

        except Exception:
            return None

    return redis_pool


async def save_self_link(
    redis: aioredis.Redis, token: Optional[str], self_href: str
) -> None:
    """Save the self link for the current token with 30 min TTL."""
    if token:
        await redis.setex(f"nav:self:{token}", 1800, self_href)


async def get_prev_link(redis: aioredis.Redis, token: Optional[str]) -> Optional[str]:
    """Get the previous page link for the current token (if exists)."""
    if not token:
        return None
    return await redis.get(f"nav:self:{token}")


async def _handle_pagination_via_redis(
    redis_enable: bool,
    next_token: Optional[str],
    token_param: Optional[str],
    request: Request,
    links: List[Dict],
) -> None:
    """Handle Redis connection and operations for pagination links."""
    if not redis_enable:
        return

    redis = None
    try:
        redis = await connect_redis()
        logger.info("Redis connection established successfully")

        if redis and next_token:
            self_link = str(request.url)
            await save_self_link(redis, next_token, self_link)

            prev_link = await get_prev_link(redis, token_param)
            if prev_link:
                links.insert(
                    0,
                    {
                        "rel": "prev",
                        "type": "application/json",
                        "method": "GET",
                        "href": prev_link,
                    },
                )
    except Exception as e:
        logger.warning(f"Redis connection failed, continuing without Redis: {e}")
