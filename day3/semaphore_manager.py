import asyncio
import logging
from collections import defaultdict
from typing import Dict
from urllib.parse import urlparse

LOGGER = logging.getLogger("SemaphoreManager")


class SemaphoreManager:

    def __init__(self, max_global: int, max_per_domain: int) -> None:
        self._global = asyncio.Semaphore(max_global)
        self._per_domain: Dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(max_per_domain))
        self._active_tasks = 0

    @staticmethod
    def _domain_from_url(url: str) -> str:
        try:
            p = urlparse(url)
            return (p.netloc or "").lower()
        except Exception as e:
            LOGGER.warning(f"Failed to parse domain from URL %r: %s", url, e)
            return ""

    async def acquire(self, url: str) -> None:
        dom = self._domain_from_url(url)
        await self._global.acquire()
        await self._per_domain[dom].acquire()
        self._active_tasks += 1

    def release(self, url: str) -> None:
        dom = self._domain_from_url(url)
        self._active_tasks = max(0, self._active_tasks - 1)
        try:
            self._per_domain[dom].release()
        except ValueError:
            pass
        try:
            self._global.release()
        except ValueError:
            pass

    @property
    def active_tasks(self) -> int:
        return self._active_tasks
