import pytest

from lakefs_spec.util import async_wrapper


@pytest.mark.asyncio
async def test_async_wrapper():
    def sync_add(n: int) -> int:
        return n + 42

    async_add = async_wrapper(sync_add)

    assert await async_add(42) == sync_add(42)
