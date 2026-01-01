import asyncio
import time
from collections import defaultdict
from typing import Dict, Optional


class RateLimiter:

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self._rps = float(requests_per_second)
        self._period = 1.0 / self._rps
        self._per_domain = per_domain
        self._last_time: Dict[str, float] = defaultdict(lambda: 0.0)
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._global_lock = asyncio.Lock()

    def _key(self, domain: Optional[str]) -> str:
        if self._per_domain:
            return (domain or "")
        return "__global__"

    async def acquire(self, domain: Optional[str] = None) -> None:
        key = self._key(domain)
        lock = self._global_lock if not self._per_domain else self._locks[key]
        async with lock:
            now = time.perf_counter()
            last = self._last_time[key]
            wait = self._period - (now - last)
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.perf_counter()
            self._last_time[key] = now
