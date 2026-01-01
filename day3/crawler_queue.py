import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Set, Tuple


@dataclass(order=True)
class _PrioritizedItem:
    priority: int
    seq: int
    url: str = field(compare=False)


class CrawlerQueue:

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[_PrioritizedItem] = asyncio.PriorityQueue()
        self._depths: Dict[str, int] = {}
        self._pending: Set[str] = set()
        self._in_progress: Set[str] = set()
        self._processed: Set[str] = set()
        self._failed: Dict[str, str] = {}
        self._seq_counter: int = 0
        self._t0 = time.perf_counter()

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        if not url:
            return False
        if url in self._pending or url in self._in_progress or url in self._processed or url in self._failed:
            return False
        self._seq_counter += 1
        self._depths[url] = depth
        self._pending.add(url)
        self._queue.put_nowait(_PrioritizedItem(priority=priority, seq=self._seq_counter, url=url))
        return True

    async def get_next(self) -> Optional[str]:
        try:
            item = await self._queue.get()
        except asyncio.CancelledError:
            return None
        if item is None:
            return None
        url = item.url
        if url in self._pending:
            self._pending.remove(url)
        self._in_progress.add(url)
        return url

    def mark_processed(self, url: str) -> None:
        self._in_progress.discard(url)
        self._processed.add(url)
        self._depths.pop(url, None)

    def mark_failed(self, url: str, error: str) -> None:
        self._in_progress.discard(url)
        self._failed[url] = error
        self._depths.pop(url, None)

    def get_depth(self, url: str) -> int:
        return int(self._depths.get(url, 0))

    def get_stats(self) -> Dict[str, int]:
        elapsed = time.perf_counter() - self._t0
        processed = len(self._processed)
        rate = processed / elapsed if elapsed > 0 else 0.0
        return {
            "queued": self._queue.qsize(),
            "pending": len(self._pending),
            "in_progress": len(self._in_progress),
            "processed": processed,
            "failed": len(self._failed),
            "rate_per_sec": int(rate),
        }

    def is_empty(self) -> bool:
        return self._queue.qsize() == 0 and not self._in_progress

    def pending_count(self) -> int:
        return self._queue.qsize()

    def in_progress_count(self) -> int:
        return len(self._in_progress)

    def totals(self) -> Tuple[int, int, int]:
        return len(self._processed), len(self._failed), self._queue.qsize()
