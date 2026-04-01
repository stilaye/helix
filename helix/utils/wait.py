"""Polling and wait utilities for async cluster operations."""

from __future__ import annotations

import time
from typing import Callable


def poll_until(
    condition: Callable[[], bool],
    timeout: float = 60.0,
    interval: float = 2.0,
    message: str = "Condition not met within timeout",
) -> None:
    """
    Poll a condition function until it returns True or timeout expires.

    Args:
        condition: Callable returning bool. Called every `interval` seconds.
        timeout: Maximum seconds to wait.
        interval: Polling interval in seconds.
        message: Error message if timeout exceeded.

    Raises:
        TimeoutError: If condition is not met within timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        remaining = deadline - time.monotonic()
        time.sleep(min(interval, max(remaining, 0)))
    raise TimeoutError(f"{message} (timeout={timeout}s)")


def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 300.0,
    interval: float = 5.0,
    message: str = "Condition not met",
) -> None:
    """Alias for poll_until with longer default timeout (for backup/restore operations)."""
    poll_until(condition, timeout=timeout, interval=interval, message=message)
