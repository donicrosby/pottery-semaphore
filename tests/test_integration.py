"""Integration tests for pottery-semaphore using Docker Redis.

These tests verify that sync and async semaphores can interoperate
and share state through Redis.
"""

from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import time
from typing import TYPE_CHECKING

import pytest

from pottery_semaphore import AIOSemaphore, Semaphore
from tests.conftest import requires_docker

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AIORedis


def _multiprocess_worker(
    worker_id: int, redis_url: str, key: str, results_queue: multiprocessing.Queue
) -> None:
    """Worker function for multiprocess test (must be at module level for pickling)."""
    from redis import Redis

    from pottery_semaphore import Semaphore

    r = None
    try:
        r = Redis.from_url(redis_url)
        s = Semaphore(value=2, key=key, masters={r})

        acquired = s.acquire(blocking=True, timeout=10)
        if acquired:
            results_queue.put((worker_id, "acquired", time.time()))
            time.sleep(0.3)  # Shorter sleep for faster tests
            s.release()
            results_queue.put((worker_id, "released", time.time()))
        else:
            results_queue.put((worker_id, "timeout", time.time()))
    except Exception as e:
        results_queue.put((worker_id, "error", str(e)))
    finally:
        if r is not None:
            with contextlib.suppress(Exception):
                r.close()


@requires_docker
class TestSyncAsyncInterop:
    """Tests for sync/async semaphore interoperability."""

    async def test_sync_acquire_async_sees_change(
        self,
        redis_client: Redis,
        aioredis_client: AIORedis,
        unique_key: str,
    ) -> None:
        """Test that async semaphore sees changes made by sync semaphore."""
        sync_sem = Semaphore(value=3, key=unique_key, masters={redis_client})
        async_sem = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})

        # Sync acquires
        sync_sem.acquire()
        assert sync_sem.value == 2

        # Async should see the change
        assert await async_sem.get_value() == 2

    async def test_async_acquire_sync_sees_change(
        self,
        redis_client: Redis,
        aioredis_client: AIORedis,
        unique_key: str,
    ) -> None:
        """Test that sync semaphore sees changes made by async semaphore."""
        sync_sem = Semaphore(value=3, key=unique_key, masters={redis_client})
        async_sem = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})

        # Async acquires
        await async_sem.acquire()
        assert await async_sem.get_value() == 2

        # Sync should see the change
        assert sync_sem.value == 2

    async def test_sync_release_wakes_async(
        self,
        redis_client: Redis,
        aioredis_client: AIORedis,
        unique_key: str,
    ) -> None:
        """Test that sync release can wake async waiter."""
        sync_sem = Semaphore(value=1, key=unique_key, masters={redis_client})
        async_sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        # Sync acquires the only permit
        sync_sem.acquire()
        assert sync_sem.value == 0

        acquired = asyncio.Event()

        async def async_waiter() -> None:
            # This should block until sync releases
            result = await async_sem.acquire(blocking=True, timeout=5)
            if result:
                acquired.set()
                await async_sem.release()

        waiter_task = asyncio.create_task(async_waiter())

        # Give waiter time to start blocking
        await asyncio.sleep(0.5)

        # Sync releases
        sync_sem.release()

        # Wait for async to acquire
        try:
            await asyncio.wait_for(waiter_task, timeout=3)
            assert acquired.is_set()
        except asyncio.TimeoutError:
            pytest.fail("Async waiter did not wake up after sync release")

    async def test_mixed_acquire_release(
        self,
        redis_client: Redis,
        aioredis_client: AIORedis,
        unique_key: str,
    ) -> None:
        """Test mixed sync/async acquire and release operations."""
        sync_sem = Semaphore(value=3, key=unique_key, masters={redis_client})
        async_sem = AIOSemaphore(value=3, key=unique_key, masters={aioredis_client})

        # Mix of sync and async acquires
        sync_sem.acquire()
        await async_sem.acquire()
        sync_sem.acquire()

        assert sync_sem.value == 0
        assert await async_sem.get_value() == 0

        # Mix of sync and async releases
        await async_sem.release()
        sync_sem.release()
        await async_sem.release()

        assert sync_sem.value == 3
        assert await async_sem.get_value() == 3


@requires_docker
class TestMultiProcess:
    """Tests for multi-process semaphore usage."""

    def test_multiprocess_semaphore(self, docker_redis: str, unique_key: str) -> None:
        """Test semaphore works across multiple processes."""
        from redis import Redis

        redis_client = Redis.from_url(docker_redis)

        # Initialize semaphore with 2 permits
        sem = Semaphore(value=2, key=unique_key, masters={redis_client})
        assert sem.value == 2
        redis_client.close()

        results: multiprocessing.Queue = multiprocessing.Queue()

        # Start 4 workers competing for 2 permits
        processes = []
        for i in range(4):
            p = multiprocessing.Process(
                target=_multiprocess_worker,
                args=(i, docker_redis, unique_key, results),
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=30)

        # Terminate any hanging processes
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)

        # Collect results
        acquired_times = []
        errors = []
        while not results.empty():
            worker_id, status, data = results.get()
            if status == "acquired":
                acquired_times.append(data)
            elif status == "error":
                errors.append((worker_id, data))

        # Check for errors
        assert not errors, f"Worker errors: {errors}"

        # All 4 workers should have acquired at some point
        assert len(acquired_times) == 4

        # Check semaphore is back to initial value
        redis_client = Redis.from_url(docker_redis)
        try:
            sem = Semaphore(value=2, key=unique_key, masters={redis_client})
            assert sem.value == 2
        finally:
            redis_client.close()


@requires_docker
class TestAsyncPrimitives:
    """Tests for the async primitive classes."""

    async def test_aio_counter(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test AIORedisCounter operations."""
        from pottery_semaphore import AIORedisCounter

        counter = AIORedisCounter(redis=aioredis_client, key=f"{unique_key}:counter")

        # Test set and get
        await counter.set("field1", 10)
        assert await counter.get("field1") == 10

        # Test default value
        assert await counter.get("nonexistent", 42) == 42

        # Test incr/decr
        await counter.incr("field1", 5)
        assert await counter.get("field1") == 15

        await counter.decr("field1", 3)
        assert await counter.get("field1") == 12

        # Test exists
        assert await counter.exists("field1")
        assert not await counter.exists("nonexistent")

    async def test_aio_queue(self, aioredis_client: AIORedis, unique_key: str) -> None:
        """Test AIORedisSimpleQueue operations."""
        from pottery_semaphore import AIORedisSimpleQueue, QueueEmpty

        queue = AIORedisSimpleQueue(redis=aioredis_client, key=f"{unique_key}:queue")

        # Test empty
        assert await queue.empty()
        assert await queue.qsize() == 0

        # Test put and get
        await queue.put("item1")
        await queue.put("item2")

        assert await queue.qsize() == 2
        assert not await queue.empty()

        item = await queue.get(block=False)
        assert item == "item1"

        item = await queue.get(block=False)
        assert item == "item2"

        assert await queue.empty()

        # Test QueueEmpty exception
        with pytest.raises(QueueEmpty):
            await queue.get(block=False)

    async def test_aio_queue_blocking(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test AIORedisSimpleQueue blocking get."""
        from pottery_semaphore import AIORedisSimpleQueue

        queue = AIORedisSimpleQueue(redis=aioredis_client, key=f"{unique_key}:queue")

        async def producer() -> None:
            await asyncio.sleep(0.5)
            await queue.put("delayed_item")

        producer_task = asyncio.create_task(producer())

        # This should block until producer puts item
        start = time.time()
        item = await queue.get(block=True, timeout=5)
        elapsed = time.time() - start

        assert item == "delayed_item"
        assert 0.3 < elapsed < 2.0

        await producer_task


@requires_docker
class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_release_negative_raises(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test that releasing negative permits raises ValueError."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})

        with pytest.raises(ValueError, match="n must be >= 1"):
            sem.release(n=0)

        with pytest.raises(ValueError, match="n must be >= 1"):
            sem.release(n=-1)

    async def test_async_release_negative_raises(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test that async releasing negative permits raises ValueError."""
        sem = AIOSemaphore(value=1, key=unique_key, masters={aioredis_client})

        with pytest.raises(ValueError, match="n must be >= 1"):
            await sem.release(n=0)

        with pytest.raises(ValueError, match="n must be >= 1"):
            await sem.release(n=-1)

    def test_acquire_non_blocking_when_exhausted(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test non-blocking acquire returns immediately when no permits."""
        sem = Semaphore(value=0, key=unique_key, masters={redis_client})

        start = time.time()
        result = sem.acquire(blocking=False)
        elapsed = time.time() - start

        assert not result
        assert elapsed < 1.0  # Should return immediately

    async def test_async_acquire_non_blocking_when_exhausted(
        self, aioredis_client: AIORedis, unique_key: str
    ) -> None:
        """Test async non-blocking acquire returns immediately when no permits."""
        sem = AIOSemaphore(value=0, key=unique_key, masters={aioredis_client})

        start = time.time()
        result = await sem.acquire(blocking=False)
        elapsed = time.time() - start

        assert not result
        assert elapsed < 1.0  # Should return immediately
