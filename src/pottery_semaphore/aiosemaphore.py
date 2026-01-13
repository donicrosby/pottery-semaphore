"""Async distributed semaphore using Pottery's public API.

This module implements an async distributed semaphore by composing Pottery's
AIORedlock with async Redis primitives for counter and queue operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, FrozenSet, Iterable

from pottery import AIORedlock, ContextTimer

from .aioprimitives import AIORedisCounter, AIORedisSimpleQueue, QueueEmpty
from .exceptions import BoundedSemaphoreError

if TYPE_CHECKING:
    from redis.asyncio import Redis as AIORedis


class AIOSemaphore:
    """Async distributed Redis-powered semaphore.

    This semaphore uses Pottery's AIORedlock for distributed mutual exclusion,
    AIORedisCounter for permit tracking, and AIORedisSimpleQueue for the
    notification queue. It supports both bounded and unbounded modes.

    Usage:
        >>> import asyncio
        >>> from redis.asyncio import Redis
        >>> async def main():
        ...     redis = Redis()
        ...     sem = AIOSemaphore(value=3, key='my-resource', masters={redis})
        ...     if await sem.acquire():
        ...         try:
        ...             # Critical section with limited concurrency
        ...             pass
        ...         finally:
        ...             await sem.release()
        >>> asyncio.run(main())

        >>> # Or use as async context manager
        >>> async with sem:
        ...     # Critical section
        ...     pass

    Args:
        value: Initial number of permits (default: 1)
        key: A string that identifies this semaphore
        masters: Async Redis clients for distributed locking
        bounded: If True, raises BoundedSemaphoreError if released too many times
        raise_on_redis_errors: Whether to raise when Redis errors prevent quorum
    """

    _KEY_PREFIX = "semaphore"
    _RETRY_DELAY = 0.2  # seconds between acquire retries

    def __init__(
        self,
        *,
        value: int = 1,
        key: str = "",
        masters: Iterable[AIORedis] = frozenset(),
        bounded: bool = True,
        raise_on_redis_errors: bool = False,
    ) -> None:
        if value < 0:
            raise ValueError("Semaphore value must be non-negative")

        self._value = value
        self._bounded = bounded
        self._key = key
        self._masters: FrozenSet[AIORedis] = frozenset(masters)
        self._initialized = False
        self._raise_on_redis_errors = raise_on_redis_errors

        # Lazy initialization - masters may be empty at construction
        self._lock: AIORedlock | None = None
        self._counter: AIORedisCounter | None = None
        self._notify: AIORedisSimpleQueue | None = None

    async def _ensure_initialized(self) -> None:
        """Lazily initialize Redis connections and primitives."""
        if self._initialized:
            return

        if not self._masters:
            from redis.asyncio import Redis as AIORedisClient

            self._masters = frozenset({AIORedisClient()})

        # Use first master for counter and notify queue
        redis = next(iter(self._masters))

        # Use Pottery's AIORedlock for distributed mutual exclusion
        self._lock = AIORedlock(
            key=f"{self._KEY_PREFIX}:{self._key}:lock",
            masters=self._masters,
            raise_on_redis_errors=self._raise_on_redis_errors,
        )

        # Use async counter (compatible with sync RedisCounter storage)
        self._counter = AIORedisCounter(
            redis=redis,
            key=f"{self._KEY_PREFIX}:{self._key}:counter",
        )

        # Use async queue (compatible with sync RedisSimpleQueue storage)
        self._notify = AIORedisSimpleQueue(
            redis=redis,
            key=f"{self._KEY_PREFIX}:{self._key}:notify",
        )

        # Initialize semaphore state
        await self._init_semaphore()
        self._initialized = True

    async def _init_semaphore(self) -> None:
        """Initialize semaphore state in Redis if not already present."""
        assert self._lock is not None
        assert self._counter is not None
        assert self._notify is not None

        async with self._lock:
            if not await self._counter.exists("permits"):
                await self._counter.set("permits", self._value)
                await self._counter.set("initial", self._value)
                # Pre-populate notification tokens for initial permits
                for _ in range(self._value):
                    await self._notify.put("1")

    async def get_value(self) -> int:
        """Return the current number of available permits."""
        await self._ensure_initialized()
        assert self._counter is not None
        return await self._counter.get("permits")

    async def get_initial_value(self) -> int:
        """Return the initial value of the semaphore."""
        await self._ensure_initialized()
        assert self._counter is not None
        return await self._counter.get("initial", self._value)

    async def locked(self) -> bool:
        """Return True if no permits are available."""
        return await self.get_value() <= 0

    async def acquire(self, *, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire a permit from the semaphore.

        Args:
            blocking: If True, block until a permit is available
            timeout: Maximum time to wait in seconds (-1 for no timeout)

        Returns:
            True if permit acquired, False otherwise
        """
        await self._ensure_initialized()

        if blocking:
            with ContextTimer() as timer:
                while timeout == -1 or timer.elapsed() / 1000 < timeout:
                    if await self._try_acquire():
                        return True

                    # Calculate remaining time for wait
                    if timeout == -1:
                        wait_timeout = self._RETRY_DELAY
                    else:
                        remaining = timeout - timer.elapsed() / 1000
                        if remaining <= 0:
                            return False
                        wait_timeout = min(self._RETRY_DELAY, remaining)

                    # Wait for notification or timeout
                    await self._wait_for_signal(wait_timeout)

                return False
        else:
            return await self._try_acquire()

    async def _try_acquire(self) -> bool:
        """Try to acquire a permit without blocking."""
        assert self._lock is not None
        assert self._counter is not None
        assert self._notify is not None

        async with self._lock:
            current = await self._counter.get("permits")

            if current > 0:
                await self._counter.decr("permits")
                # Consume a notification token if available
                try:
                    await self._notify.get(block=False)
                except QueueEmpty:
                    pass  # Queue might be empty, that's ok
                return True

        return False

    async def _wait_for_signal(self, timeout: float) -> bool:
        """Block until a permit is released or timeout.

        Uses AIORedisSimpleQueue's blocking get for efficient waiting.
        """
        assert self._notify is not None

        try:
            await self._notify.get(block=True, timeout=timeout)
            # Put the token back - we just wanted to wake up
            # The actual permit will be acquired in _try_acquire
            await self._notify.put("1")
            return True
        except QueueEmpty:
            # Timeout occurred
            return False

    async def release(self, n: int = 1) -> None:
        """Release permit(s) back to the semaphore.

        Args:
            n: Number of permits to release (default: 1)

        Raises:
            BoundedSemaphoreError: If bounded=True and releasing would exceed
                                   the initial value
        """
        await self._ensure_initialized()
        assert self._lock is not None
        assert self._counter is not None
        assert self._notify is not None

        if n < 1:
            raise ValueError("n must be >= 1")

        async with self._lock:
            current = await self._counter.get("permits")
            initial_val = await self._counter.get("initial", self._value)

            if self._bounded and current + n > initial_val:
                raise BoundedSemaphoreError(
                    key=self._key,
                    current=current,
                    initial=initial_val,
                )

            # Increment counter and signal waiters
            await self._counter.incr("permits", n)
            for _ in range(n):
                await self._notify.put("1")

    async def __aenter__(self) -> AIOSemaphore:
        """Enter async context manager, acquiring a permit."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager, releasing the permit."""
        await self.release()

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"key={self._key!r} "
            f"bounded={self._bounded}>"
        )
