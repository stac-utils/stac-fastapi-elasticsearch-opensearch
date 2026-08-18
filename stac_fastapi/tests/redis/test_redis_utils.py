import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

import stac_fastapi.core.redis_utils as redis_utils
from stac_fastapi.core.redis_utils import (
    AsyncRedisQueueManager,
    connect_redis,
    get_prev_link,
    save_prev_link,
)


@pytest.mark.asyncio
async def test_redis_connection():
    """Test Redis connection."""
    redis = await connect_redis()

    if redis is None:
        pytest.skip("Redis not configured")

    await redis.set("string_key", "string_value")
    string_value = await redis.get("string_key")
    assert string_value == "string_value"

    exists = await redis.exists("string_key")
    assert exists == 1

    await redis.delete("string_key")
    deleted_value = await redis.get("string_key")
    assert deleted_value is None


@pytest.mark.asyncio
async def test_redis_utils_functions():
    redis = await connect_redis()
    if redis is None:
        pytest.skip("Redis not configured")

    token = "test_token_123"
    current_url = "http://mywebsite.com/search"
    next_url = "http://mywebsite.com/search?token=test_token_123"

    await save_prev_link(redis, next_url, current_url, token)

    retrieved_link = await get_prev_link(redis, next_url, token)
    assert retrieved_link
    assert retrieved_link["href"] == current_url
    assert retrieved_link["method"] == "GET"

    await save_prev_link(redis, None, "should_not_save", None)
    null_result = await get_prev_link(redis, None, None)
    assert null_result is None

    non_existent = await get_prev_link(
        redis, "http://mywebsite.com/search", "non_existent_token"
    )
    assert non_existent is None


@pytest.mark.asyncio
async def test_connect_redis_standalone_passes_username_and_password(monkeypatch):
    monkeypatch.setattr(
        redis_utils.sentinel_settings, "REDIS_SENTINEL_HOSTS", "", raising=False
    )
    monkeypatch.setattr(redis_utils.settings, "REDIS_HOST", "redis-host", raising=False)
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_USERNAME", "testuser", raising=False
    )
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_PASSWORD", "testpass", raising=False
    )

    captured_kwargs = {}

    class FakeConnectionPool:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(redis_utils.aioredis, "ConnectionPool", FakeConnectionPool)
    monkeypatch.setattr(redis_utils.aioredis, "Redis", lambda **kwargs: object())

    await redis_utils._connect_redis_internal()

    assert captured_kwargs["username"] == "testuser"
    assert captured_kwargs["password"] == "testpass"


@pytest.mark.asyncio
async def test_connect_redis_sentinel_passes_username_and_password(monkeypatch):
    # `settings` and `sentinel_settings` are the same object whenever sentinel mode
    # is active in a real process (both derived from REDIS_SENTINEL_HOSTS at import
    # time) - keep that invariant here so `settings.get_sentinel_nodes()` resolves.
    monkeypatch.setattr(redis_utils, "settings", redis_utils.sentinel_settings)
    monkeypatch.setattr(
        redis_utils.sentinel_settings,
        "REDIS_SENTINEL_HOSTS",
        "sentinel-host",
        raising=False,
    )
    monkeypatch.setattr(
        redis_utils.sentinel_settings, "REDIS_USERNAME", "testuser", raising=False
    )
    monkeypatch.setattr(
        redis_utils.sentinel_settings, "REDIS_PASSWORD", "testpass", raising=False
    )

    captured_kwargs = {}

    class FakeSentinel:
        def __init__(self, *args, **kwargs):
            pass

        def master_for(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            return object()

    monkeypatch.setattr(redis_utils, "Sentinel", FakeSentinel)

    await redis_utils._connect_redis_internal()

    assert captured_kwargs["username"] == "testuser"
    assert captured_kwargs["password"] == "testpass"


@pytest.mark.asyncio
async def test_queue_manager_connect_standalone_passes_username_and_password(
    monkeypatch,
):
    monkeypatch.delenv("REDIS_SENTINEL_HOSTS", raising=False)
    monkeypatch.setenv("REDIS_HOST", "redis-host")
    monkeypatch.setenv("REDIS_USERNAME", "testuser")
    monkeypatch.setenv("REDIS_PASSWORD", "testpass")

    captured_kwargs = {}

    def fake_redis(**kwargs):
        captured_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(redis_utils.aioredis, "Redis", fake_redis)

    await AsyncRedisQueueManager._connect()

    assert captured_kwargs["username"] == "testuser"
    assert captured_kwargs["password"] == "testpass"


@pytest.mark.asyncio
async def test_queue_manager_connect_sentinel_passes_username_and_password(
    monkeypatch,
):
    monkeypatch.setenv("REDIS_SENTINEL_HOSTS", "sentinel-host")
    monkeypatch.setenv("REDIS_USERNAME", "testuser")
    monkeypatch.setenv("REDIS_PASSWORD", "testpass")

    captured_kwargs = {}

    class FakeSentinel:
        def __init__(self, *args, **kwargs):
            pass

        def master_for(self, *args, **kwargs):
            captured_kwargs.update(kwargs)
            return object()

    monkeypatch.setattr(redis_utils, "Sentinel", FakeSentinel)

    await AsyncRedisQueueManager._connect()

    assert captured_kwargs["username"] == "testuser"
    assert captured_kwargs["password"] == "testpass"


@pytest.mark.asyncio
async def test_redis_retry_retries_until_success(monkeypatch):
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_QUERY_RETRIES_NUM", 3, raising=False
    )
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_QUERY_INITIAL_DELAY", 0, raising=False
    )
    monkeypatch.setattr(redis_utils.settings, "REDIS_QUERY_BACKOFF", 2.0, raising=False)

    captured_kwargs = {}

    def fake_retry(**kwargs):
        captured_kwargs.update(kwargs)

        def decorator(func):
            async def wrapped(*args, **inner_kwargs):
                attempts = 0
                while True:
                    try:
                        attempts += 1
                        return await func(*args, **inner_kwargs)
                    except kwargs["exceptions"] as exc:
                        if attempts >= kwargs["tries"]:
                            raise exc
                        continue

            return wrapped

        return decorator

    monkeypatch.setattr(redis_utils, "retry", fake_retry)

    call_counter = {"count": 0}

    @redis_utils.redis_retry
    async def flaky() -> str:
        call_counter["count"] += 1
        if call_counter["count"] < 3:
            raise RedisConnectionError("transient failure")
        return "success"

    result = await flaky()

    assert result == "success"
    assert call_counter["count"] == 3
    assert captured_kwargs["tries"] == redis_utils.settings.REDIS_QUERY_RETRIES_NUM
    assert captured_kwargs["delay"] == redis_utils.settings.REDIS_QUERY_INITIAL_DELAY
    assert captured_kwargs["backoff"] == redis_utils.settings.REDIS_QUERY_BACKOFF


@pytest.mark.asyncio
async def test_redis_retry_raises_after_exhaustion(monkeypatch):
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_QUERY_RETRIES_NUM", 3, raising=False
    )
    monkeypatch.setattr(
        redis_utils.settings, "REDIS_QUERY_INITIAL_DELAY", 0, raising=False
    )
    monkeypatch.setattr(redis_utils.settings, "REDIS_QUERY_BACKOFF", 2.0, raising=False)

    def fake_retry(**kwargs):
        def decorator(func):
            async def wrapped(*args, **inner_kwargs):
                attempts = 0
                while True:
                    try:
                        attempts += 1
                        return await func(*args, **inner_kwargs)
                    except kwargs["exceptions"] as exc:
                        if attempts >= kwargs["tries"]:
                            raise exc
                        continue

            return wrapped

        return decorator

    monkeypatch.setattr(redis_utils, "retry", fake_retry)

    @redis_utils.redis_retry
    async def always_fail() -> str:
        raise RedisConnectionError("pernament failure")

    with pytest.raises(RedisConnectionError):
        await always_fail()
