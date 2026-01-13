"""Unit tests for the async AIOSemaphore class."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from pottery_semaphore import AIOSemaphore, BoundedSemaphoreError
from tests.conftest import requires_docker

if TYPE_CHECKING:
    from redis.asyncio import Redis as AIORedis


@requires_docker
class TestAIOSemaphoreBasic:
    """Basic functionality tests for AIOSemaphore."""

    async def test_create_semaphore(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test creating an async semaphore with default values."""
        sem = AIOSemaphore(key=unique_key, masters={aioredis_client})
        assert await sem.get_value() == 1
        assert await sem.get_initial_value() == 1
        assert not await sem.locked()

    async def test_create_semaphore_with_value(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test creating an async semaphore with a specific value."""
        sem = AIOSemaphore(value=5, key=unique_key, masters={aioredis_client})
        assert await sem.get_value() == 5
        assert await sem.get_initial_value() == 5

    async def test_create_semaphore_zero_value(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test creating an async semaphore with zero permits."""
        sem = AIOSemaphore(value=0, key=unique_key, masters={aioredis_client})
        assert await sem.get_value() == 0
        assert await sem.locked()

    def test_create_semaphore_negative_value_raises(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that negative value raises ValueError."""
        with pytest.raises(ValueError, match="non-negative"):
            AIOSemaphore(value=-1, key=unique_key, masters={aioredis_client})


@requires_docker
class TestAIOSemaphoreAcquireRelease:
    """Tests for async acquire and release operations."""

    async def test_acquire_and_release(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test basic async acquire and release."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        assert await sem.acquire()
        assert await sem.get_value() == 0
        assert await sem.locked()

        await sem.release()
        assert await sem.get_value() == 1
        assert not await sem.locked()

    async def test_acquire_multiple(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test acquiring multiple permits."""
        sem = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})

        assert await sem.acquire()
        assert await sem.get_value() == 2
        assert await sem.acquire()
        assert await sem.get_value() == 1
        assert await sem.acquire()
        assert await sem.get_value() == 0
        assert await sem.locked()

    async def test_acquire_non_blocking_success(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test non-blocking acquire when permits available."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})
        assert await sem.acquire(blocking=False)
        assert await sem.get_value() == 0

    async def test_acquire_non_blocking_failure(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test non-blocking acquire when no permits available."""
        sem = AIOSemaphore(value=0, key=unique_key, masters={aioredis_client})
        assert not await sem.acquire(blocking=False)

    async def test_acquire_with_timeout(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test acquire with timeout when semaphore is available."""
        # Test that acquire with timeout succeeds when permits are available
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        start = time.time()
        result = await sem.acquire(blocking=True, timeout=5.0)
        elapsed = time.time() - start

        assert result
        assert elapsed < 2.0  # Should acquire quickly when available
        await sem.release()

    async def test_release_multiple(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test releasing multiple permits at once."""
        sem = AIOSemaphore(
            value=3, key=unique_key, masters={aioredis_client}, bounded=False
        )

        # Acquire all permits
        for _ in range(3):
            await sem.acquire()
        assert await sem.get_value() == 0

        # Release all at once
        await sem.release(n=3)
        assert await sem.get_value() == 3


@requires_docker
class TestAIOSemaphoreBounded:
    """Tests for bounded async semaphore behavior."""

    async def test_bounded_release_raises(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that bounded semaphore raises on over-release."""
        sem = AIOSemaphore(
            value=1, key=unique_key, masters={aioredis_client}, bounded=True
        )

        with pytest.raises(BoundedSemaphoreError):
            await sem.release()  # Release without acquire

    async def test_unbounded_release_allowed(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that unbounded semaphore allows over-release."""
        sem = AIOSemaphore(
            value=1, key=unique_key, masters={aioredis_client}, bounded=False
        )

        await sem.release()  # Should not raise
        assert await sem.get_value() == 2

    async def test_bounded_normal_flow(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test bounded semaphore with normal acquire/release flow."""
        sem = AIOSemaphore(
            value=2, key=unique_key, masters={aioredis_client}, bounded=True
        )

        await sem.acquire()
        await sem.acquire()
        assert await sem.get_value() == 0

        await sem.release()
        await sem.release()
        assert await sem.get_value() == 2

        # This should raise since we're at initial value
        with pytest.raises(BoundedSemaphoreError):
            await sem.release()


@requires_docker
class TestAIOSemaphoreContextManager:
    """Tests for async context manager functionality."""

    async def test_context_manager(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test using async semaphore as context manager."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        async with sem:
            assert await sem.get_value() == 0
            assert await sem.locked()

        assert await sem.get_value() == 1
        assert not await sem.locked()

    async def test_context_manager_exception(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that permit is released even on exception."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        with pytest.raises(RuntimeError):
            async with sem:
                assert await sem.get_value() == 0
                raise RuntimeError("test error")

        # Permit should still be released
        assert await sem.get_value() == 1


@requires_docker
class TestAIOSemaphoreConcurrency:
    """Tests for concurrent async access to semaphore."""

    async def test_concurrent_acquire(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that async semaphore correctly limits concurrent access."""
        sem = AIOSemaphore(value=2, key=unique_key, masters={aioredis_client})

        # Sequential test: acquire all permits then verify limiting
        assert await sem.acquire(blocking=False)  # First acquire succeeds
        assert await sem.get_value() == 1
        assert await sem.acquire(blocking=False)  # Second acquire succeeds
        assert await sem.get_value() == 0
        assert not await sem.acquire(blocking=False)  # Third should fail (no permits)

        # Release both
        await sem.release()
        await sem.release()
        assert await sem.get_value() == 2

    async def test_blocking_acquire_wakes_on_release(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that blocking acquire wakes when permit is released."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})
        acquired = asyncio.Event()
        can_release = asyncio.Event()

        async def holder() -> None:
            await sem.acquire()
            acquired.set()
            await can_release.wait()
            await sem.release()

        async def waiter() -> None:
            await acquired.wait()
            # This should block until holder releases
            start = time.time()
            result = await sem.acquire(blocking=True, timeout=5)
            elapsed = time.time() - start
            assert result
            assert elapsed < 3  # Should not wait full timeout
            await sem.release()

        holder_task = asyncio.create_task(holder())
        waiter_task = asyncio.create_task(waiter())

        # Let waiter start blocking
        await asyncio.sleep(0.5)
        # Release the holder
        can_release.set()

        await asyncio.gather(holder_task, waiter_task)
        assert await sem.get_value() == 1


@requires_docker
class TestAIOSemaphoreSharedState:
    """Tests for shared state between async semaphore instances."""

    async def test_shared_state(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that two semaphores with same key share state."""
        sem1 = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})
        sem2 = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})

        # sem1 acquires
        await sem1.acquire()
        assert await sem1.get_value() == 2

        # sem2 should see the same value
        assert await sem2.get_value() == 2

        # sem2 acquires
        await sem2.acquire()
        assert await sem1.get_value() == 1
        assert await sem2.get_value() == 1

    def test_repr(self, aioredis_client: AIORedis, unique_key: str) -> None:
        """Test string representation of async semaphore."""
        sem = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})
        repr_str = repr(sem)

        assert "AIOSemaphore" in repr_str
        assert unique_key in repr_str
        assert "bounded=True" in repr_str


@requires_docker
class TestAIOSemaphoreLockRaceCondition:
    """Tests to verify concurrent coroutine access to shared AIORedlock."""

    async def test_concurrent_acquire_release_race(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that concurrent acquire/release doesn't corrupt lock state.

        This test specifically targets a race condition where multiple coroutines
        sharing the same AIORedlock instance can corrupt its internal state
        (e.g., UUID tracking) when interleaved at await points.
        """
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})
        errors: list[Exception] = []
        iterations = 20

        async def acquire_release_cycle(task_id: int, semaphore: AIOSemaphore) -> None:
            """Rapidly acquire and release to trigger interleaving."""
            for _ in range(iterations):
                try:
                    # Use non-blocking to create more contention
                    acquired = await semaphore.acquire(blocking=True, timeout=2)
                    if acquired:
                        # Small delay to allow other coroutines to try acquiring
                        await asyncio.sleep(0.01)
                        await semaphore.release()
                except Exception as e:
                    errors.append(e)
                    raise

        # Run multiple concurrent tasks to maximize interleaving
        tasks = [asyncio.create_task(acquire_release_cycle(i, sem)) for i in range(5)]

        # Gather with return_exceptions to capture all errors
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for any ReleaseUnlockedLock or other errors
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)

        # If we hit the race condition, we'll see ReleaseUnlockedLock
        assert not errors, f"Race condition errors: {errors}"

    async def test_holder_waiter_lock_contention(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Reproduce the exact scenario from the failing test.

        Multiple iterations to increase chance of hitting the race condition.
        """
        errors: list[Exception] = []

        async def run_iteration(
            semaphore: AIOSemaphore,
            acquired_event: asyncio.Event,
            release_event: asyncio.Event,
        ) -> None:
            async def holder() -> None:
                await semaphore.acquire()
                acquired_event.set()
                await release_event.wait()
                await semaphore.release()

            async def waiter() -> None:
                await acquired_event.wait()
                # This creates contention on the shared lock
                result = await semaphore.acquire(blocking=True, timeout=2)
                if result:
                    await semaphore.release()

            holder_task = asyncio.create_task(holder())
            waiter_task = asyncio.create_task(waiter())

            # Shorter sleep to increase race window
            await asyncio.sleep(0.1)
            release_event.set()

            try:
                await asyncio.wait_for(
                    asyncio.gather(holder_task, waiter_task),
                    timeout=5,
                )
            except Exception:
                # Cancel tasks on error
                holder_task.cancel()
                waiter_task.cancel()
                await asyncio.gather(holder_task, waiter_task, return_exceptions=True)
                raise

        for iteration in range(10):
            key = f"{unique_key}-{iteration}"
            sem = AIOSemaphore(value=1, key=key, masters={aioredis_client})
            acquired = asyncio.Event()
            can_release = asyncio.Event()

            try:
                await run_iteration(sem, acquired, can_release)
            except Exception as e:
                errors.append(e)

        assert not errors, (
            f"Lock race errors after {len(errors)} failures: "
            f"{errors[0] if errors else None}"
        )
