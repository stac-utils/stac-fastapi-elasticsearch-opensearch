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


class RedisSentinelSettings(BaseSettings):
    """Configuration settings for connecting to a Redis Sentinel server."""

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


class RedisSettings(BaseSettings):
    """Configuration settings for connecting to a Redis server."""

    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))

    max_connections: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "5"))
    retry_on_timeout: bool = get_bool_env("REDIS_RETRY_TIMEOUT", True)
    decode_responses: bool = get_bool_env("REDIS_DECODE_RESPONSES", True)
    client_name: str = os.getenv("REDIS_CLIENT_NAME", "stac-fastapi-app")
    health_check_interval: int = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))


# select which configuration to be used RedisSettings or RedisSentinelSettings
redis_settings = RedisSettings()


async def connect_redis_sentinel(
    settings: Optional[RedisSentinelSettings] = None,
) -> Optional[aioredis.Redis]:
    """Return a Redis Sentinel connection."""
    global redis_pool
    settings = redis_settings

    if (
        not settings.sentinel_hosts
        or not settings.sentinel_hosts[0]
        or not settings.sentinel_master_name
    ):
        return None

    if redis_pool is None:
        try:
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

            master = sentinel.master_for(
                settings.sentinel_master_name,
                db=settings.redis_db,
                decode_responses=settings.decode_responses,
                retry_on_timeout=settings.retry_on_timeout,
                client_name=settings.client_name,
                max_connections=settings.max_connections,
            )

            redis_pool = master

        except:
            return None

    return redis_pool


async def connect_redis(
    settings: Optional[RedisSettings] = None,
) -> Optional[aioredis.Redis]:
    """Return a Redis connection for regular Redis server."""
    global redis_pool
    settings = redis_settings

    if not settings.redis_host:
        return None

    if redis_pool is None:
        try:
            redis_pool = aioredis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                decode_responses=settings.decode_responses,
                retry_on_timeout=settings.retry_on_timeout,
                client_name=settings.client_name,
                health_check_interval=settings.health_check_interval,
                max_connections=settings.max_connections,
            )
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            return None

    return redis_pool


async def close_redis() -> None:
    """Close the Redis connection pool if it exists."""
    global redis_pool
    if redis_pool:
        await redis_pool.close()
        redis_pool = None


async def cache_current_url(redis, current_url: str, key: str) -> None:
    """Add to Redis cache the current URL for navigation."""
    if not redis:
        return

    try:
        current_key = f"current:{key}"
        await redis.setex(current_key, 600, current_url)
    except Exception as e:
        logger.error(f"Redis cache error for {key}: {e}")


async def get_previous_url(redis, key: str) -> Optional[str]:
    """Get previous URL from Redis cache if it exists."""
    if redis is None:
        return None

    try:
        prev_key = f"prev:{key}"
        previous_url = await redis.get(prev_key)
        if previous_url:
            return previous_url
    except Exception as e:
        logger.error(f"Redis get previous error for {key}: {e}")

    return None


async def cache_previous_url(redis, current_url: str, key: str) -> None:
    """Cache the current URL as previous for previous links in next page."""
    if not redis:
        return

    try:
        prev_key = f"prev:{key}"
        await redis.setex(prev_key, 600, current_url)
    except Exception as e:
        logger.error(f"Redis cache previous error for {key}: {e}")


async def add_previous_link(
    redis,
    links: List[Dict],
    key: str,
    current_url: str,
    token: Optional[str] = None,
) -> None:
    """Add previous link into navigation."""
    if not redis or not token:
        return

    previous_url = await get_previous_url(redis, key)
    if previous_url:
        links.append(
            {
                "rel": "previous",
                "type": MimeTypes.json,
                "href": previous_url,
            }
        )
