import pytest

from stac_fastapi.core.redis_utils import connect_redis, get_prev_link, save_self_link


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
    self_link = "http://mywebsite.com/search?token=test_token_123"

    await save_self_link(redis, token, self_link)
    retrieved_link = await get_prev_link(redis, token)
    assert retrieved_link == self_link

    await save_self_link(redis, None, "should_not_save")
    null_result = await get_prev_link(redis, None)
    assert null_result is None

    non_existent = await get_prev_link(redis, "non_existent_token")
    assert non_existent is None
