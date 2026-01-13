"""Distributed semaphore using Pottery's public API.

This module implements a distributed semaphore by composing Pottery's
Redlock, RedisCounter, and RedisSimpleQueue primitives.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from pottery import (
    ContextTimer,
    QueueEmptyError,
    RedisCounter,
    RedisSimpleQueue,
    Redlock,
)

from .exceptions import BoundedSemaphoreError

if TYPE_CHECKING:
    from redis import Redis


class Semaphore:
    """Distributed Redis-powered semaphore.

    This semaphore uses Pottery's Redlock for distributed mutual exclusion,
    RedisCounter for permit tracking, and RedisSimpleQueue for the notification
    queue. It supports both bounded and unbounded modes.

    Usage:
        >>> from redis import Redis
        >>> redis = Redis()
        >>> sem = Semaphore(value=3, key='my-resource', masters={redis})
        >>> if sem.acquire():
        ...     try:
        ...         # Critical section with limited concurrency
        ...         pass
        ...     finally:
        ...         sem.release()

        >>> # Or use as context manager
        >>> with sem:
        ...     # Critical section
        ...     pass

    Args:
        value: Initial number of permits (default: 1)
        key: A string that identifies this semaphore
        masters: Redis clients for distributed locking
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
        masters: Iterable[Redis] = frozenset(),
        bounded: bool = True,
        raise_on_redis_errors: bool = False,
    ) -> None:
        if value < 0:
            raise ValueError("Semaphore value must be non-negative")

        self._value = value
        self._bounded = bounded
        self._key = key
        self._masters: frozenset[Redis] = frozenset(masters)

        if not self._masters:
            from redis import Redis as RedisClient

            self._masters = frozenset({RedisClient()})

        # Use Pottery's Redlock for distributed mutual exclusion
        self._lock = Redlock(
            key=f"{self._KEY_PREFIX}:{key}:lock",
            masters=self._masters,
            raise_on_redis_errors=raise_on_redis_errors,
        )

        # Use first master for counter and notify queue
        # (protected by the distributed lock)
        self._redis = next(iter(self._masters))

        # Use Pottery's RedisCounter for permit tracking
        self._counter = RedisCounter(
            redis=self._redis,
            key=f"{self._KEY_PREFIX}:{key}:counter",
        )

        # Use Pottery's RedisSimpleQueue for notification (supports blocking get)
        self._notify = RedisSimpleQueue(
            redis=self._redis,
            key=f"{self._KEY_PREFIX}:{key}:notify",
        )

        # Initialize semaphore state
        self._init_semaphore()

    def _init_semaphore(self) -> None:
        """Initialize semaphore state in Redis if not already present."""
        with self._lock:
            if "permits" not in self._counter:
                self._counter["permits"] = self._value
                self._counter["initial"] = self._value
                # Pre-populate notification tokens for initial permits
                for _ in range(self._value):
                    self._notify.put("1")

    @property
    def value(self) -> int:
        """Return the current number of available permits."""
        return self._counter["permits"]

    @property
    def initial_value(self) -> int:
        """Return the initial value of the semaphore."""
        return self._counter["initial"] or self._value

    def locked(self) -> bool:
        """Return True if no permits are available."""
        return self.value <= 0

    def acquire(self, *, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire a permit from the semaphore.

        Args:
            blocking: If True, block until a permit is available
            timeout: Maximum time to wait in seconds (-1 for no timeout)

        Returns:
            True if permit acquired, False otherwise
        """
        if blocking:
            with ContextTimer() as timer:
                while timeout == -1 or timer.elapsed() / 1000 < timeout:
                    if self._try_acquire():
                        return True

                    # Calculate remaining time for blocking wait
                    if timeout == -1:
                        wait_timeout = self._RETRY_DELAY
                    else:
                        remaining = timeout - timer.elapsed() / 1000
                        if remaining <= 0:
                            return False
                        wait_timeout = min(self._RETRY_DELAY, remaining)

                    # Wait for notification or timeout
                    self._wait_for_signal(wait_timeout)

                return False
        else:
            return self._try_acquire()

    def _try_acquire(self) -> bool:
        """Try to acquire a permit without blocking."""
        with self._lock:
            current = self._counter["permits"]

            if current > 0:
                self._counter.subtract({"permits": 1})
                # Consume a notification token if available
                try:
                    self._notify.get(block=False)
                except QueueEmptyError:
                    pass  # Queue might be empty, that's ok
                return True

        return False

    def _wait_for_signal(self, timeout: float) -> bool:
        """Block until a permit is released or timeout.

        Uses RedisSimpleQueue's blocking get for efficient waiting.
        """
        try:
            # Use Pottery's RedisSimpleQueue blocking get
            self._notify.get(block=True, timeout=timeout)
            # Put the token back - we just wanted to wake up
            # The actual permit will be acquired in _try_acquire
            self._notify.put("1")
            return True
        except QueueEmptyError:
            # Timeout occurred
            return False

    def release(self, n: int = 1) -> None:
        """Release permit(s) back to the semaphore.

        Args:
            n: Number of permits to release (default: 1)

        Raises:
            BoundedSemaphoreError: If bounded=True and releasing would exceed
                                   the initial value
        """
        if n < 1:
            raise ValueError("n must be >= 1")

        with self._lock:
            current = self._counter["permits"]
            initial_val = self._counter["initial"] or self._value

            if self._bounded and current + n > initial_val:
                raise BoundedSemaphoreError(
                    key=self._key,
                    current=current,
                    initial=initial_val,
                )

            # Increment counter and signal waiters
            self._counter.update({"permits": n})
            for _ in range(n):
                self._notify.put("1")

    def __enter__(self) -> Semaphore:
        """Enter context manager, acquiring a permit."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, releasing the permit."""
        self.release()

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"key={self._key!r} "
            f"value={self.value}/{self.initial_value} "
            f"bounded={self._bounded}>"
        )
