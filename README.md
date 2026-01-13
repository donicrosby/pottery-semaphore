# pottery-semaphore

A Redis/Valkey distributed semaphore built on top of [Pottery](https://github.com/brainix/pottery).

`pottery-semaphore` provides distributed counting semaphores for coordinating access to shared resources across multiple processes and machines. It composes Pottery's battle-tested [Redlock](https://redis.io/docs/manual/patterns/distributed-locks/) implementation with Redis atomic operations for permit tracking.

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/donicrosby/pottery-semaphore.git
```

Or clone and install locally:

```bash
git clone https://github.com/donicrosby/pottery-semaphore.git
cd pottery-semaphore
pip install .
```

## Quickstart

### Synchronous Usage

```python
from redis import Redis
from pottery_semaphore import Semaphore

redis = Redis()

# Create a semaphore that allows 3 concurrent accesses
sem = Semaphore(value=3, key='my-resource', masters={redis})

# Use as a context manager
with sem:
    # Critical section - at most 3 processes can be here simultaneously
    do_work()
```

### Asynchronous Usage

```python
import asyncio
from redis.asyncio import Redis
from pottery_semaphore import AIOSemaphore

async def main():
    redis = Redis()
    sem = AIOSemaphore(value=3, key='my-resource', masters={redis})

    async with sem:
        # Critical section with limited concurrency
        await do_async_work()

asyncio.run(main())
```

## Usage

### Basic Acquire/Release

If you need more control than context managers provide:

```python
from pottery_semaphore import Semaphore

sem = Semaphore(value=2, key='database-connections', masters={redis})

if sem.acquire():
    try:
        # Use the resource
        pass
    finally:
        sem.release()
```

### Non-blocking Acquire

Try to acquire without waiting:

```python
# Returns immediately with True/False
if sem.acquire(blocking=False):
    try:
        # Got the permit
        pass
    finally:
        sem.release()
else:
    # No permits available, do something else
    pass
```

### Timeout

Wait up to a specified time for a permit:

```python
# Wait up to 5 seconds for a permit
if sem.acquire(timeout=5):
    try:
        # Got the permit within 5 seconds
        pass
    finally:
        sem.release()
else:
    # Timed out waiting for permit
    pass
```

### Bounded vs Unbounded Semaphores

By default, semaphores are **bounded** - they raise `BoundedSemaphoreError` if you release more times than you acquire (similar to `threading.BoundedSemaphore`):

```python
from pottery_semaphore import Semaphore, BoundedSemaphoreError

sem = Semaphore(value=1, key='bounded-example', masters={redis})

sem.release()  # Raises BoundedSemaphoreError!
```

For unbounded semaphores that allow extra releases:

```python
sem = Semaphore(value=1, key='unbounded-example', masters={redis}, bounded=False)

sem.release()  # OK - permits can exceed initial value
```

### Configuration Options

Both `Semaphore` and `AIOSemaphore` accept the same parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value` | `int` | `1` | Initial number of permits |
| `key` | `str` | `""` | Unique identifier for this semaphore |
| `masters` | `Iterable[Redis]` | `frozenset()` | Redis clients for distributed locking |
| `bounded` | `bool` | `True` | Raise error if released more than acquired |
| `raise_on_redis_errors` | `bool` | `False` | Raise when Redis errors prevent quorum |

### Checking Semaphore State

```python
# Synchronous
sem.value           # Current available permits
sem.initial_value   # Initial permit count
sem.locked()        # True if no permits available

# Asynchronous
await sem.get_value()
await sem.get_initial_value()
await sem.locked()
```

## API Reference

### `Semaphore`

Synchronous distributed semaphore.

- `acquire(blocking=True, timeout=-1)` → `bool` - Acquire a permit
- `release(n=1)` → `None` - Release permit(s)
- `locked()` → `bool` - Check if no permits available
- `value` → `int` - Current available permits (property)
- `initial_value` → `int` - Initial permit count (property)

### `AIOSemaphore`

Asynchronous distributed semaphore with the same interface as `Semaphore`, but all methods are coroutines:

- `await acquire(blocking=True, timeout=-1)` → `bool`
- `await release(n=1)` → `None`
- `await locked()` → `bool`
- `await get_value()` → `int`
- `await get_initial_value()` → `int`

### Exceptions

- `SemaphoreError` - Base exception for all semaphore errors
- `BoundedSemaphoreError` - Raised when releasing a bounded semaphore would exceed its initial value

## Requirements

- Python 3.9+
- Redis or Valkey server
- [pottery](https://github.com/brainix/pottery) ≥ 3.0.1

## How It Works

`pottery-semaphore` combines several Redis primitives:

1. **Redlock** (via Pottery) - Provides distributed mutual exclusion for atomic operations
2. **Counter** - Tracks available permits
3. **Queue** - Enables efficient blocking waits via Redis's `BLPOP`

This composition ensures correct semaphore semantics across distributed systems while leveraging Pottery's robust Redlock implementation.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
