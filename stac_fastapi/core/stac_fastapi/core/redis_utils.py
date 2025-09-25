"""Utilities for connecting to and managing Redis connections."""

from typing import Optional

from pydantic_settings import BaseSettings
from redis import asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

redis_pool: Optional[aioredis.Redis] = None


class RedisSentinelSettings(BaseSettings):
    """Configuration for connecting to Redis Sentinel."""

    REDIS_SENTINEL_HOSTS: str = ""
    REDIS_SENTINEL_PORTS: str = "26379"
    REDIS_SENTINEL_MASTER_NAME: str = "master"
    REDIS_DB: int = 0

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_RETRY_TIMEOUT: bool = True
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30


class RedisSettings(BaseSettings):
    """Configuration for connecting Redis."""

    REDIS_HOST: str = ""
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_RETRY_TIMEOUT: bool = True
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30


# Select the Redis or Redis Sentinel configuration
redis_settings: BaseSettings = RedisSentinelSettings()


async def connect_redis_sentinel(
    settings: Optional[RedisSentinelSettings] = None,
) -> Optional[aioredis.Redis]:
    """Return Redis Sentinel connection."""
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
                [(host, port) for host, port in zip(hosts, ports)],
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


async def connect_redis(settings: Optional[RedisSettings] = None) -> aioredis.Redis:
    """Return Redis connection."""
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


async def save_self_link(
    redis: aioredis.Redis, token: Optional[str], self_href: str
) -> None:
    """Add the self link for next page as prev link for the current token."""
    if token:
        await redis.setex(f"nav:self:{token}", 1800, self_href)


async def get_prev_link(redis: aioredis.Redis, token: Optional[str]) -> Optional[str]:
    """Pull the prev page link for the current token."""
    if not token:
        return None
    return await redis.get(f"nav:self:{token}")
