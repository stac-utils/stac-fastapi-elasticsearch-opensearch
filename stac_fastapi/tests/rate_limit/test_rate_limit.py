import logging

import pytest
from httpx import AsyncClient
from slowapi.errors import RateLimitExceeded

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_rate_limit(app_client_rate_limit: AsyncClient, ctx):
    expected_status_codes = [200, 200, 429, 429, 429]

    for i, expected_status_code in enumerate(expected_status_codes):
        try:
            response = await app_client_rate_limit.get("/collections")
            status_code = response.status_code
        except RateLimitExceeded:
            status_code = 429

        logger.info(f"Request {i + 1}: Status code {status_code}")
        assert (
            status_code == expected_status_code
        ), f"Expected status code {expected_status_code}, but got {status_code}"


@pytest.mark.asyncio
async def test_rate_limit_no_limit(app_client: AsyncClient, ctx):
    expected_status_codes = [200, 200, 200, 200, 200]

    for i, expected_status_code in enumerate(expected_status_codes):
        response = await app_client.get("/collections")
        status_code = response.status_code

        logger.info(f"Request {i + 1}: Status code {status_code}")
        assert (
            status_code == expected_status_code
        ), f"Expected status code {expected_status_code}, but got {status_code}"
