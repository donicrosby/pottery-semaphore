"""Unit tests for the sync Semaphore class."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from pottery_semaphore import BoundedSemaphoreError, Semaphore
from tests.conftest import requires_docker

if TYPE_CHECKING:
    from redis import Redis


@requires_docker
class TestSemaphoreBasic:
    """Basic functionality tests for Semaphore."""

    def test_create_semaphore(self, redis_client: Redis, unique_key: str) -> None:
        """Test creating a semaphore with default values."""
        sem = Semaphore(key=unique_key, masters={redis_client})
        assert sem.value == 1
        assert sem.initial_value == 1
        assert not sem.locked()

    def test_create_semaphore_with_value(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test creating a semaphore with a specific value."""
        sem = Semaphore(value=5, key=unique_key, masters={redis_client})
        assert sem.value == 5
        assert sem.initial_value == 5

    def test_create_semaphore_zero_value(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test creating a semaphore with zero permits."""
        sem = Semaphore(value=0, key=unique_key, masters={redis_client})
        assert sem.value == 0
        assert sem.locked()

    def test_create_semaphore_negative_value_raises(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test that negative value raises ValueError."""
        with pytest.raises(ValueError, match="non-negative"):
            Semaphore(value=-1, key=unique_key, masters={redis_client})


@requires_docker
class TestSemaphoreAcquireRelease:
    """Tests for acquire and release operations."""

    def test_acquire_and_release(self, redis_client: Redis, unique_key: str) -> None:
        """Test basic acquire and release."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})

        assert sem.acquire()
        assert sem.value == 0
        assert sem.locked()

        sem.release()
        assert sem.value == 1
        assert not sem.locked()

    def test_acquire_multiple(self, redis_client: Redis, unique_key: str) -> None:
        """Test acquiring multiple permits."""
        sem = Semaphore(value=3, key=unique_key, masters={redis_client})

        assert sem.acquire()
        assert sem.value == 2
        assert sem.acquire()
        assert sem.value == 1
        assert sem.acquire()
        assert sem.value == 0
        assert sem.locked()

    def test_acquire_non_blocking_success(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test non-blocking acquire when permits available."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})
        assert sem.acquire(blocking=False)
        assert sem.value == 0

    def test_acquire_non_blocking_failure(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test non-blocking acquire when no permits available."""
        sem = Semaphore(value=0, key=unique_key, masters={redis_client})
        assert not sem.acquire(blocking=False)

    def test_acquire_with_timeout(self, redis_client: Redis, unique_key: str) -> None:
        """Test acquire with timeout when semaphore is available."""
        # Test that acquire with timeout succeeds when permits are available
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})

        start = time.time()
        result = sem.acquire(blocking=True, timeout=5.0)
        elapsed = time.time() - start

        assert result
        assert elapsed < 2.0  # Should acquire quickly when available
        sem.release()

    def test_release_multiple(self, redis_client: Redis, unique_key: str) -> None:
        """Test releasing multiple permits at once."""
        sem = Semaphore(value=3, key=unique_key, masters={redis_client}, bounded=False)

        # Acquire all permits
        for _ in range(3):
            sem.acquire()
        assert sem.value == 0

        # Release all at once
        sem.release(n=3)
        assert sem.value == 3


@requires_docker
class TestSemaphoreBounded:
    """Tests for bounded semaphore behavior."""

    def test_bounded_release_raises(self, redis_client: Redis, unique_key: str) -> None:
        """Test that bounded semaphore raises on over-release."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client}, bounded=True)

        with pytest.raises(BoundedSemaphoreError):
            sem.release()  # Release without acquire

    def test_unbounded_release_allowed(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test that unbounded semaphore allows over-release."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client}, bounded=False)

        sem.release()  # Should not raise
        assert sem.value == 2

    def test_bounded_normal_flow(self, redis_client: Redis, unique_key: str) -> None:
        """Test bounded semaphore with normal acquire/release flow."""
        sem = Semaphore(value=2, key=unique_key, masters={redis_client}, bounded=True)

        sem.acquire()
        sem.acquire()
        assert sem.value == 0

        sem.release()
        sem.release()
        assert sem.value == 2

        # This should raise since we're at initial value
        with pytest.raises(BoundedSemaphoreError):
            sem.release()


@requires_docker
class TestSemaphoreContextManager:
    """Tests for context manager functionality."""

    def test_context_manager(self, redis_client: Redis, unique_key: str) -> None:
        """Test using semaphore as context manager."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})

        with sem:
            assert sem.value == 0
            assert sem.locked()

        assert sem.value == 1
        assert not sem.locked()

    def test_context_manager_exception(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test that permit is released even on exception."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})

        with pytest.raises(RuntimeError):
            with sem:
                assert sem.value == 0
                raise RuntimeError("test error")

        # Permit should still be released
        assert sem.value == 1


@requires_docker
class TestSemaphoreConcurrency:
    """Tests for concurrent access to semaphore."""

    def test_concurrent_acquire(self, redis_client: Redis, unique_key: str) -> None:
        """Test that semaphore correctly limits concurrent access."""
        sem = Semaphore(value=2, key=unique_key, masters={redis_client})

        # Sequential test: acquire all permits then verify limiting
        assert sem.acquire(blocking=False)  # First acquire succeeds
        assert sem.value == 1
        assert sem.acquire(blocking=False)  # Second acquire succeeds
        assert sem.value == 0
        assert not sem.acquire(blocking=False)  # Third should fail (no permits)

        # Release both
        sem.release()
        sem.release()
        assert sem.value == 2

    def test_blocking_acquire_wakes_on_release(
        self, redis_client: Redis, unique_key: str
    ) -> None:
        """Test that blocking acquire wakes when permit is released."""
        sem = Semaphore(value=1, key=unique_key, masters={redis_client})
        acquired = threading.Event()
        released = threading.Event()

        def holder() -> None:
            sem.acquire()
            acquired.set()
            released.wait(timeout=5)
            sem.release()

        def waiter() -> None:
            acquired.wait(timeout=5)
            # This should block until holder releases
            start = time.time()
            result = sem.acquire(blocking=True, timeout=5)
            elapsed = time.time() - start
            assert result
            assert elapsed < 3  # Should not wait full timeout
            sem.release()

        holder_thread = threading.Thread(target=holder)
        waiter_thread = threading.Thread(target=waiter)

        holder_thread.start()
        waiter_thread.start()

        # Let waiter start blocking
        time.sleep(0.5)
        # Release the holder
        released.set()

        holder_thread.join(timeout=5)
        waiter_thread.join(timeout=5)

        assert sem.value == 1


@requires_docker
class TestSemaphoreSharedState:
    """Tests for shared state between semaphore instances."""

    def test_shared_state(self, redis_client: Redis, unique_key: str) -> None:
        """Test that two semaphores with same key share state."""
        sem1 = Semaphore(value=3, key=unique_key, masters={redis_client})
        sem2 = Semaphore(value=3, key=unique_key, masters={redis_client})

        # sem1 acquires
        sem1.acquire()
        assert sem1.value == 2

        # sem2 should see the same value
        assert sem2.value == 2

        # sem2 acquires
        sem2.acquire()
        assert sem1.value == 1
        assert sem2.value == 1

    def test_repr(self, redis_client: Redis, unique_key: str) -> None:
        """Test string representation of semaphore."""
        sem = Semaphore(value=3, key=unique_key, masters={redis_client})
        repr_str = repr(sem)

        assert "Semaphore" in repr_str
        assert unique_key in repr_str
        assert "3/3" in repr_str
        assert "bounded=True" in repr_str
