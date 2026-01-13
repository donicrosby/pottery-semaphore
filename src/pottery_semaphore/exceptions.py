"""Exceptions for pottery-semaphore."""

from __future__ import annotations


class SemaphoreError(Exception):
    """Base exception for semaphore errors."""

    pass


class BoundedSemaphoreError(SemaphoreError, ValueError):
    """Raised when releasing a bounded semaphore would exceed its initial value.

    This mirrors the behavior of Python's threading.BoundedSemaphore which raises
    ValueError when release() is called too many times.
    """

    def __init__(self, key: str, current: int, initial: int) -> None:
        self.key = key
        self.current = current
        self.initial = initial
        super().__init__(
            f"Semaphore '{key}' released too many times: "
            f"current={current}, initial={initial}"
        )
