"""Utilities for connecting to and managing Redis connections."""

import logging
import os
from typing import Dict, List, Optional

from pydantic_settings import BaseSettings
from redis import asyncio as aioredis
from stac_pydantic.shared import MimeTypes

from stac_fastapi.core.utilities import get_bool_env

redis_pool = None

logger = logging.getLogger(__name__)


class RedisSettings(BaseSettings):
    """Configuration settings for connecting to a Redis server."""

    sentinel_hosts: List[str] = os.getenv("REDIS_SENTINEL_HOSTS", "").split(",")
    sentinel_ports: List[int] = [
        int(port)
        for port in os.getenv("REDIS_SENTINEL_PORTS", "").split(",")
        if port.strip()
    ]
    sentinel_master_name: str = os.getenv("REDIS_SENTINEL_MASTER_NAME", "")
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    max_connections: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "5"))
    retry_on_timeout: bool = get_bool_env("REDIS_RETRY_TIMEOUT", True)
    decode_responses: bool = get_bool_env("REDIS_DECODE_RESPONSES", True)
    client_name: str = os.getenv("REDIS_CLIENT_NAME", "stac-fastapi-app")
    health_check_interval: int = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))


redis_settings = RedisSettings()


async def connect_redis(
    settings: Optional[RedisSettings] = None,
) -> Optional[aioredis.Redis]:
    """Return a Redis connection, returning None if not configured or connection fails."""
    global redis_pool
    settings = settings or redis_settings

    if (
        not settings.sentinel_hosts
        or not settings.sentinel_hosts[0]
        or not settings.sentinel_master_name
    ):
        logger.warning("Redis not configured - skipping Redis operations")
        return None

    if redis_pool is None:
        try:
            # Create async Sentinel connection
            sentinel = aioredis.Sentinel(
                [
                    (host, port)
                    for host, port in zip(
                        settings.sentinel_hosts, settings.sentinel_ports
                    )
                ],
                decode_responses=settings.decode_responses,
                retry_on_timeout=settings.retry_on_timeout,
                client_name=f"{settings.client_name}-sentinel",
            )

            # Get async master connection
            master = sentinel.master_for(
                settings.sentinel_master_name,
                db=settings.redis_db,
                decode_responses=settings.decode_responses,
                retry_on_timeout=settings.retry_on_timeout,
                client_name=settings.client_name,
            )

            # Test the connection
            await master.ping()
            logger.info("Redis Sentinel connection successful!")

            redis_pool = master

        except Exception as e:
            logger.error(f"Failed to connect to Redis Sentinel: {e}")
            return None

    return redis_pool


async def close_redis() -> None:
    """Close the Redis connection pool if it exists."""
    global redis_pool
    if redis_pool:
        await redis_pool.close()
        redis_pool = None


async def cache_current_url(redis, current_url: str, key_suffix: str) -> None:
    """Cache the current URL for pagination."""
    if not redis:
        return

    try:
        current_key = f"current:{key_suffix}"
        await redis.setex(current_key, 600, current_url)
    except Exception as e:
        logger.error(f"Redis cache error for {key_suffix}: {e}")


async def get_previous_url(redis, key_suffix: str, current_url: str) -> Optional[str]:
    """Get previous URL from cache if it exists."""
    if redis is None:
        return None

    try:
        prev_key = f"prev:{key_suffix}"
        previous_url = await redis.get(prev_key)
        # REMOVE the current_url comparison - just return the cached value
        if previous_url:
            return previous_url
    except Exception as e:
        logger.error(f"Redis get previous error for {key_suffix}: {e}")

    return None


async def cache_previous_url(redis, current_url: str, key_suffix: str) -> None:
    """Cache the current URL as previous for next request."""
    if not redis:
        return

    try:
        prev_key = f"prev:{key_suffix}"
        await redis.setex(prev_key, 600, current_url)
        print(f"DEBUG: Cached {current_url} as previous for {key_suffix}")  # Add debug
    except Exception as e:
        logger.error(f"Redis cache previous error for {key_suffix}: {e}")


async def add_previous_link_if_exists(
    redis,
    links: List[Dict],
    key_suffix: str,
    current_url: str,
    token: Optional[str] = None,
) -> None:
    """Add previous link to links list if it exists in cache and conditions are met."""
    if not redis or not token:
        return

    previous_url = await get_previous_url(redis, key_suffix, current_url)
    if previous_url:
        links.append(
            {
                "rel": "previous",
                "type": MimeTypes.json,
                "href": previous_url,
            }
        )
