"""Distributed semaphore using Pottery's Redis primitives.

This package provides distributed semaphore implementations that compose
Pottery's Redlock and AIORedlock for distributed mutual exclusion with
Redis atomic operations for permit tracking.

Example usage (sync):

    >>> from redis import Redis
    >>> from pottery_semaphore import Semaphore
    >>>
    >>> redis = Redis()
    >>> sem = Semaphore(value=3, key='my-resource', masters={redis})
    >>>
    >>> with sem:
    ...     # Critical section with limited concurrency (max 3)
    ...     pass

Example usage (async):

    >>> import asyncio
    >>> from redis.asyncio import Redis
    >>> from pottery_semaphore import AIOSemaphore
    >>>
    >>> async def main():
    ...     redis = Redis()
    ...     sem = AIOSemaphore(value=3, key='my-resource', masters={redis})
    ...     async with sem:
    ...         # Critical section with limited concurrency
    ...         pass
    >>> asyncio.run(main())
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Final

from .aioprimitives import AIORedisCounter, AIORedisSimpleQueue, QueueEmpty
from .aiosemaphore import AIOSemaphore
from .exceptions import BoundedSemaphoreError, SemaphoreError
from .semaphore import Semaphore

__all__: Final[tuple[str, ...]] = (
    "AIORedisCounter",
    "AIORedisSimpleQueue",
    "AIOSemaphore",
    "BoundedSemaphoreError",
    "QueueEmpty",
    "Semaphore",
    "SemaphoreError",
)

try:
    __version__ = version("pottery-semaphore")
except PackageNotFoundError:
    __version__ = "unknown"
