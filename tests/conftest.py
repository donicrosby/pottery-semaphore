"""Pytest configuration and fixtures for pottery-semaphore tests."""

from __future__ import annotations

import os
import time
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AIORedis


def is_docker_available() -> bool:
    """Check if Docker is available."""
    import shutil
    import subprocess

    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


# Skip integration tests if Docker is not available
requires_docker = pytest.mark.skipif(
    not is_docker_available(),
    reason="Docker is not available",
)


@pytest.fixture(scope="session")
def docker_compose_file() -> str:
    """Return path to docker-compose file for Redis."""
    return os.path.join(os.path.dirname(__file__), "docker-compose.yml")


@pytest.fixture(scope="session")
def redis_port() -> int:
    """Return the Redis port for tests."""
    return 6399  # Use non-standard port to avoid conflicts


@pytest.fixture(scope="session")
def docker_redis(docker_compose_file: str, redis_port: int) -> Generator[str, None, None]:
    """Start Redis in Docker for integration tests.

    Returns the Redis URL.
    """
    import subprocess

    # Create docker-compose.yml if it doesn't exist
    compose_content = f"""
services:
  redis:
    image: redis:7-alpine
    ports:
      - "{redis_port}:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 1s
      timeout: 3s
      retries: 30
"""
    with open(docker_compose_file, "w") as f:
        f.write(compose_content)

    # Start Redis
    subprocess.run(
        ["docker", "compose", "-f", docker_compose_file, "up", "-d", "--wait"],
        check=True,
        capture_output=True,
    )

    # Wait for Redis to be ready
    redis_url = f"redis://localhost:{redis_port}/0"
    _wait_for_redis(redis_url)

    yield redis_url

    # Cleanup
    subprocess.run(
        ["docker", "compose", "-f", docker_compose_file, "down", "-v"],
        capture_output=True,
    )
    os.remove(docker_compose_file)


def _wait_for_redis(url: str, timeout: float = 30) -> None:
    """Wait for Redis to be ready."""
    from redis import Redis
    from redis.exceptions import ConnectionError

    start = time.time()
    while time.time() - start < timeout:
        try:
            r = Redis.from_url(url)
            r.ping()
            r.close()
            return
        except ConnectionError:
            time.sleep(0.5)
    raise TimeoutError(f"Redis at {url} did not become ready in {timeout}s")


@pytest.fixture
def redis_client(docker_redis: str) -> Generator[Redis, None, None]:
    """Create a Redis client connected to Docker Redis."""
    from redis import Redis

    client = Redis.from_url(docker_redis)
    # Cleanup before test to ensure isolation
    client.flushdb()
    yield client
    # Cleanup all keys after each test
    try:
        client.flushdb()
    except Exception:
        pass  # Ignore errors during cleanup
    client.close()


@pytest.fixture
async def aioredis_client(docker_redis: str) -> AsyncGenerator[AIORedis, None]:
    """Create an async Redis client connected to Docker Redis."""
    from redis.asyncio import Redis as AIORedis

    client = AIORedis.from_url(docker_redis)
    # Cleanup before test to ensure isolation
    await client.flushdb()
    yield client
    # Cleanup all keys after each test
    try:
        await client.flushdb()
    except Exception:
        pass  # Ignore errors during cleanup
    await client.aclose()


@pytest.fixture
def unique_key() -> Generator[str, None, None]:
    """Generate a unique key for each test."""
    import uuid

    yield f"test-{uuid.uuid4().hex[:8]}"
