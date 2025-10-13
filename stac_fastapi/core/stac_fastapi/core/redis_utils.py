"""Utilities for connecting to and managing Redis connections."""

import json
import logging
from typing import List, Optional, Tuple

from pydantic_settings import BaseSettings
from redis import asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

logger = logging.getLogger(__name__)


class RedisSentinelSettings(BaseSettings):
    """Configuration settings for connecting to Redis Sentinel."""

    REDIS_SENTINEL_HOSTS: str = ""
    REDIS_SENTINEL_PORTS: str = "26379"
    REDIS_SENTINEL_MASTER_NAME: str = "master"
    REDIS_DB: int = 0

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30
    REDIS_SELF_LINK_TTL: int = 1800

    def get_sentinel_nodes(self) -> List[Tuple[str, int]]:
        """Return list of (host, port) tuples."""
        try:
            hosts = json.loads(self.REDIS_SENTINEL_HOSTS)
            ports = json.loads(self.REDIS_SENTINEL_PORTS)
        except json.JSONDecodeError:
            hosts = [
                h.strip() for h in self.REDIS_SENTINEL_HOSTS.split(",") if h.strip()
            ]
            ports = [
                int(p.strip())
                for p in self.REDIS_SENTINEL_PORTS.split(",")
                if p.strip()
            ]

        if len(ports) == 1 and len(hosts) > 1:
            ports = ports * len(hosts)

        return list(zip(hosts, ports))


class RedisSettings(BaseSettings):
    """Configuration settings for connecting to a standalone Redis instance."""

    REDIS_HOST: str = ""
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    REDIS_MAX_CONNECTIONS: int = 10
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = 30
    REDIS_SELF_LINK_TTL: int = 1800


sentinel_settings = RedisSentinelSettings()
standalone_settings = RedisSettings()

redis: Optional[aioredis.Redis] = None


async def connect_redis() -> Optional[aioredis.Redis]:
    """Initialize global Redis connection (Sentinel or Standalone)."""
    global redis
    if redis:
        return redis

    try:
        # Prefer Sentinel if configured
        if sentinel_settings.REDIS_SENTINEL_HOSTS.strip():
            sentinel_nodes = sentinel_settings.get_sentinel_nodes()
            sentinel = Sentinel(
                sentinel_nodes,
                decode_responses=True,
            )
            redis = sentinel.master_for(
                service_name=sentinel_settings.REDIS_SENTINEL_MASTER_NAME,
                db=sentinel_settings.REDIS_DB,
                decode_responses=True,
                client_name=sentinel_settings.REDIS_CLIENT_NAME,
                max_connections=sentinel_settings.REDIS_MAX_CONNECTIONS,
                health_check_interval=sentinel_settings.REDIS_HEALTH_CHECK_INTERVAL,
            )
            await redis.ping()
            logger.info("✅ Connected to Redis Sentinel")
            return redis

        # Fallback to standalone
        if standalone_settings.REDIS_HOST.strip():
            redis = aioredis.Redis(
                host=standalone_settings.REDIS_HOST,
                port=standalone_settings.REDIS_PORT,
                db=standalone_settings.REDIS_DB,
                decode_responses=True,
                client_name=standalone_settings.REDIS_CLIENT_NAME,
                health_check_interval=standalone_settings.REDIS_HEALTH_CHECK_INTERVAL,
            )
            await redis.ping()
            logger.info("✅ Connected to standalone Redis")
            return redis

        logger.warning("⚠️ No Redis configuration found — skipping connection.")
        return None

    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        redis = None
        return None


async def close_redis():
    """Close global Redis connection."""
    global redis
    if redis:
        await redis.close()
        redis = None
        logger.info("Redis connection closed.")


async def save_self_link(
    redis: aioredis.Redis, token: Optional[str], self_href: str
) -> None:
    """Save current self link for token."""
    if not token:
        return

    ttl = (
        sentinel_settings.REDIS_SELF_LINK_TTL
        if sentinel_settings.REDIS_SENTINEL_HOSTS.strip()
        else standalone_settings.REDIS_SELF_LINK_TTL
    )

    await redis.setex(f"nav:self:{token}", ttl, self_href)


async def get_prev_link(redis: aioredis.Redis, token: Optional[str]) -> Optional[str]:
    """Return previous page link for token."""
    if not token:
        return None
    return await redis.get(f"nav:self:{token}")


async def redis_pagination_links(
    current_url: str,
    token: str,
    next_token: str,
    links: list,
    redis_conn: Optional[aioredis.Redis] = None,
) -> None:
    """Manage pagination links stored in Redis."""
    redis_conn = redis_conn or await connect_redis()
    if not redis_conn:
        logger.warning("Redis not available for pagination.")
        return

    try:
        if next_token:
            await save_self_link(redis_conn, next_token, current_url)

        prev_link = await get_prev_link(redis_conn, token)
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
        logger.warning(f"Redis pagination failed: {e}")
