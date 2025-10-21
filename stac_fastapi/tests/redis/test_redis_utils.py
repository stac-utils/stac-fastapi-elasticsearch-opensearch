import pytest

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
