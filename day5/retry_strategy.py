import asyncio
import inspect
import logging
import math
import random
from typing import Any, Awaitable, Callable, Coroutine, Dict, Iterable, Optional, Tuple, Type, Union

class TransientError(Exception):
    """Temporary error (timeouts, 5xx, backpressure like 429)."""


class PermanentError(Exception):
    """Non-recoverable error (404, 403, etc.)."""


class NetworkError(Exception):
    """Networking problems (connection refused, DNS, SSL)."""


class ParseError(Exception):
    """Parsing issues; usually not retried."""


OnRetryCallback = Callable[[int, BaseException, float], None]
AsyncCallable = Callable[..., Awaitable[Any]]


class RetryStrategy:

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        backoff_base: float = 0.5,
        max_backoff: float = 10.0,
        retry_on: Optional[Iterable[Type[BaseException]]] = None,
        per_type_backoff: Optional[Dict[Type[BaseException], Tuple[float, float]]] = None,
        jitter: float = 0.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.max_retries = int(max(0, max_retries))
        self.backoff_factor = float(backoff_factor)
        self.backoff_base = float(backoff_base)
        self.max_backoff = float(max_backoff)
        self.retry_on = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        self.per_type_backoff = per_type_backoff or {}
        self.jitter = max(0.0, float(jitter))
        self._logger = logger or logging.getLogger("RetryStrategy")

    @staticmethod
    def _accepts_attempt(coro: AsyncCallable) -> bool:
        try:
            return "attempt" in inspect.signature(coro).parameters
        except (TypeError, ValueError):
            return False

    async def execute_with_retry(
        self,
        coro: Union[AsyncCallable, Coroutine[Any, Any, Any]],
        *args: Any,
        on_retry: Optional[OnRetryCallback] = None,
        **kwargs: Any,
    ) -> Any:
        if inspect.iscoroutine(coro):
            raise TypeError(
                "execute_with_retry expects a coroutine function, not a coroutine object; "
                "pass the callable and its arguments instead"
            )

        attempt = 0
        inject_attempt = self._accepts_attempt(coro)
        while True:
            try:
                call_kwargs = dict(kwargs)
                if inject_attempt:
                    call_kwargs["attempt"] = attempt
                return await coro(*args, **call_kwargs)
            except self.retry_on as exc:  # type: ignore[misc]
                if attempt >= self.max_retries:
                    self._logger.warning(
                        "Retry: giving up after %d attempts due to %s", attempt, type(exc).__name__
                    )
                    raise
                sleep_for = self._compute_sleep(exc, attempt)
                if self.jitter > 0:
                    sleep_for += random.uniform(0, self.jitter)
                self._logger.info(
                    "Retry: attempt=%d error=%s sleep=%.2fs", attempt, type(exc).__name__, sleep_for
                )
                if on_retry:
                    try:
                        on_retry(attempt, exc, sleep_for)
                    except Exception:
                        pass
                await asyncio.sleep(sleep_for)
                attempt += 1
            except PermanentError:
                raise

    def _compute_sleep(self, exc: BaseException, attempt: int) -> float:
        base = self.backoff_base
        factor = self.backoff_factor
        for etype, (b, f) in self.per_type_backoff.items():
            if isinstance(exc, etype):
                base, factor = b, f
                break
        exp = max(0, attempt)
        sleep_for = base * math.pow(factor, exp)
        return min(self.max_backoff, max(0.0, sleep_for))
