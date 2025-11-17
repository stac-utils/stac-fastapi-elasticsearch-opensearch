import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

import stac_fastapi.core.redis_utils as redis_utils
from stac_fastapi.core.redis_utils import connect_redis, get_prev_link, save_prev_link


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
    assert retrieved_link == current_url

    await save_prev_link(redis, None, "should_not_save", None)
    null_result = await get_prev_link(redis, None, None)
    assert null_result is None

    non_existent = await get_prev_link(
        redis, "http://mywebsite.com/search", "non_existent_token"
    )
    assert non_existent is None


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
