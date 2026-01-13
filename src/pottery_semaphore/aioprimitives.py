"""Async Redis primitives for use with AIOSemaphore.

Pottery does not provide async versions of RedisCounter or RedisSimpleQueue,
so we implement minimal async equivalents here that are compatible with
the sync Pottery versions' storage format.

IMPORTANT: These primitives use the same encoding/storage format as Pottery:
- RedisCounter uses JSON-encoded keys and values in a Redis hash
- RedisSimpleQueue uses JSON-encoded values in a Redis stream
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis as AIORedis


def _encode(value: Any) -> str:
    """JSON-encode a value (matches Pottery's _encode)."""
    return json.dumps(value, sort_keys=True)


def _decode(encoded_value: str | bytes) -> Any:
    """JSON-decode a value (matches Pottery's _decode)."""
    if isinstance(encoded_value, bytes):
        encoded_value = encoded_value.decode()
    return json.loads(encoded_value)


class AIORedisCounter:
    """Async Redis-backed counter compatible with Pottery's RedisCounter.

    Uses Redis hash storage with JSON encoding, same as Pottery's RedisCounter,
    so sync and async semaphores can share state.

    Usage:
        >>> from redis.asyncio import Redis
        >>> redis = Redis()
        >>> counter = AIORedisCounter(redis=redis, key='my-counter')
        >>> await counter.set('permits', 5)
        >>> await counter.get('permits')
        5
        >>> await counter.decr('permits')
        4
    """

    def __init__(self, *, redis: AIORedis, key: str) -> None:
        self._redis: AIORedis = redis  # Any to avoid type issues with async Redis
        self._key = key

    @property
    def key(self) -> str:
        """Return the Redis key for this counter."""
        return self._key

    async def get(self, field: str, default: int = 0) -> int:
        """Get a counter field value."""
        encoded_field = _encode(field)
        encoded_value = await self._redis.hget(self._key, encoded_field)
        if encoded_value is None:
            return default
        return _decode(encoded_value)

    async def set(self, field: str, value: int) -> None:
        """Set a counter field value."""
        encoded_field = _encode(field)
        encoded_value = _encode(value)
        await self._redis.hset(self._key, encoded_field, encoded_value)

    async def incr(self, field: str, amount: int = 1) -> int:
        """Increment a counter field and return new value."""
        # Can't use HINCRBY with JSON-encoded values, need to get/set
        current = await self.get(field, 0)
        new_value = current + amount
        await self.set(field, new_value)
        return new_value

    async def decr(self, field: str, amount: int = 1) -> int:
        """Decrement a counter field and return new value."""
        return await self.incr(field, -amount)

    async def exists(self, field: str) -> bool:
        """Check if a field exists in the counter."""
        encoded_field = _encode(field)
        return await self._redis.hexists(self._key, encoded_field)


class QueueEmpty(Exception):
    """Raised when a non-blocking get is called on an empty queue."""

    pass


class AIORedisSimpleQueue:
    """Async Redis-backed queue compatible with Pottery's RedisSimpleQueue.

    Uses Redis streams with JSON encoding, same as Pottery's RedisSimpleQueue,
    so sync and async semaphores can share state.

    Usage:
        >>> from redis.asyncio import Redis
        >>> redis = Redis()
        >>> queue = AIORedisSimpleQueue(redis=redis, key='my-queue')
        >>> await queue.put('item')
        >>> await queue.get()
        'item'
    """

    def __init__(self, *, redis: AIORedis, key: str) -> None:
        self._redis: AIORedis = redis  # Any to avoid type issues with async Redis
        self._key = key

    @property
    def key(self) -> str:
        """Return the Redis key for this queue."""
        return self._key

    async def qsize(self) -> int:
        """Return the number of items in the queue."""
        return await self._redis.xlen(self._key)

    async def empty(self) -> bool:
        """Return True if the queue is empty."""
        return await self.qsize() == 0

    async def put(self, item: Any) -> None:
        """Put an item into the queue using Redis streams (like Pottery)."""
        encoded_item = _encode(item)
        await self._redis.xadd(self._key, {"item": encoded_item}, id="*")

    async def get(self, *, block: bool = True, timeout: float | None = None) -> Any:
        """Remove and return an item from the queue.

        Args:
            block: If True, block until an item is available
            timeout: Maximum time to wait in seconds (None for infinite)

        Returns:
            The item from the queue

        Raises:
            QueueEmpty: If non-blocking and queue is empty, or timeout expires
        """
        if block:
            # XREAD with BLOCK for blocking read
            block_ms = int(timeout * 1000) if timeout is not None else 0
            result = await self._redis.xread(
                {self._key: "0-0"}, count=1, block=block_ms
            )
            if not result:
                raise QueueEmpty()
            # result format: [[key, [(id, {field: value})]]]
            stream_data = result[0][1]
            if not stream_data:
                raise QueueEmpty()
            entry_id, entry_data = stream_data[0]
            # Delete the entry we just read
            await self._redis.xdel(self._key, entry_id)
            encoded_item = entry_data.get(b"item") or entry_data.get("item")
            return _decode(encoded_item)
        else:
            # Non-blocking: read and delete
            result = await self._redis.xrange(self._key, count=1)
            if not result:
                raise QueueEmpty()
            entry_id, entry_data = result[0]
            await self._redis.xdel(self._key, entry_id)
            encoded_item = entry_data.get(b"item") or entry_data.get("item")
            return _decode(encoded_item)
