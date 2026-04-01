"""
Retry decorator using tenacity with exponential backoff and jitter.
Wraps tenacity.retry with sensible defaults for storage infrastructure tests.
"""

from __future__ import annotations

import functools
import logging
from typing import Callable, Type, TypeVar

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable)


def retry_on_exception(
    exceptions: tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 30.0,
    jitter: float = 2.0,
) -> Callable[[F], F]:
    """
    Decorator: retry on specified exceptions with exponential backoff + jitter.

    Args:
        exceptions: Exception types to retry on.
        max_attempts: Maximum number of total attempts (1 = no retry).
        min_wait: Minimum seconds to wait between retries.
        max_wait: Maximum seconds to wait between retries.
        jitter: Random jitter range in seconds (prevents thundering herd in parallel workers).

    Usage:
        @retry_on_exception(exceptions=(ConnectionError, TimeoutError), max_attempts=3)
        def flaky_operation():
            ...
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        @retry(
            retry=retry_if_exception_type(exceptions),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=min_wait, max=max_wait, jitter=jitter),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper  # type: ignore[return-value]
    return decorator
