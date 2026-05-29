import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

try:
    import aiofiles  # type: ignore
except Exception:  # pragma: no cover
    aiofiles = None  # type: ignore

try:
    import aiosqlite  # type: ignore
except Exception:  # pragma: no cover
    aiosqlite = None  # type: ignore


class DataStorage(ABC):
    """
    Abstract base for async data storage backends.
    """

    @abstractmethod
    async def save(self, data: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class JSONStorage(DataStorage):
    """
    Append-only JSON Lines (NDJSON) storage using aiofiles for async writes.
    Each call to save() writes a single JSON object per line.
    """

    def __init__(self, path: str, ensure_ascii: bool = False) -> None:
        self.path = path
        self.ensure_ascii = ensure_ascii
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("JSONStorage")
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)

    async def save(self, data: Dict[str, Any]) -> None:
        if aiofiles is None:
            # Fallback to blocking write in a thread to keep async interface
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._blocking_write, data)
            return
        async with self._lock:
            try:
                async with aiofiles.open(self.path, mode="a", encoding="utf-8") as f:  # type: ignore
                    line = json.dumps(data, ensure_ascii=self.ensure_ascii)
                    await f.write(line + "\n")
            except Exception as e:
                self._logger.error("JSON save failed: %s", e)
                raise

    def _blocking_write(self, data: Dict[str, Any]) -> None:
        line = json.dumps(data, ensure_ascii=self.ensure_ascii)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    async def close(self) -> None:
        # Nothing to close for file appends
        return


class CSVStorage(DataStorage):
    """
    CSV storage with automatic header detection. Uses aiofiles to append.
    If file does not exist, writes header on first save.
    """

    def __init__(self, path: str, encoding: str = "utf-8") -> None:
        self.path = path
        self.encoding = encoding
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("CSVStorage")
        self._header_written = os.path.exists(self.path) and os.path.getsize(self.path) > 0
        self._fields: Optional[List[str]] = None
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)

    async def save(self, data: Dict[str, Any]) -> None:
        import csv
        import io

        # Flatten some nested structures for CSV
        row: Dict[str, Any] = dict(data)
        if isinstance(row.get("links"), list):
            row["links"] = json.dumps(row["links"], ensure_ascii=False)
        if isinstance(row.get("metadata"), dict):
            row["metadata"] = json.dumps(row["metadata"], ensure_ascii=False)

        async with self._lock:
            try:
                if self._fields is None:
                    # Determine fields deterministically
                    self._fields = list(row.keys())
                    # ensure standard columns first if present
                    preferred = [
                        "url",
                        "title",
                        "text",
                        "links",
                        "metadata",
                        "crawled_at",
                        "status_code",
                        "content_type",
                    ]
                    # sort fields: preferred order, then rest alphabetically
                    ordered: List[str] = []
                    for k in preferred:
                        if k in self._fields:
                            ordered.append(k)
                    for k in sorted(self._fields):
                        if k not in ordered:
                            ordered.append(k)
                    self._fields = ordered

                # Compose CSV line using csv.writer into StringIO
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=self._fields, extrasaction="ignore")
                need_header = not self._header_written
                if need_header:
                    writer.writeheader()
                    self._header_written = True
                writer.writerow(row)
                text = output.getvalue()
                output.close()

                if aiofiles is None:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self._blocking_append, text)
                else:
                    async with aiofiles.open(self.path, mode="a", encoding=self.encoding, newline="") as f:  # type: ignore
                        await f.write(text)
            except Exception as e:
                self._logger.error("CSV save failed: %s", e)
                raise

    def _blocking_append(self, text: str) -> None:
        with open(self.path, "a", encoding=self.encoding, newline="") as f:
            f.write(text)

    async def close(self) -> None:
        return


class SQLiteStorage(DataStorage):
    """
    SQLite storage using aiosqlite. Maintains a small insert buffer for batch inserts.
    """

    def __init__(self, path: str, batch_size: int = 50) -> None:
        self.path = path
        self.batch_size = max(1, int(batch_size))
        self._db: Optional[Any] = None
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("SQLiteStorage")
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)

    async def _ensure_db(self) -> None:
        if self._db is None:
            if aiosqlite is None:
                raise RuntimeError("aiosqlite is not installed")
            self._db = await aiosqlite.connect(self.path)
            await self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS pages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    title TEXT,
                    text TEXT,
                    links TEXT,
                    metadata TEXT,
                    crawled_at TEXT,
                    status_code INTEGER,
                    content_type TEXT
                )
                """
            )
            await self._db.execute("CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url)")
            await self._db.commit()

    async def save(self, data: Dict[str, Any]) -> None:
        await self._ensure_db()
        async with self._lock:
            self._buffer.append(data)
            if len(self._buffer) >= self.batch_size:
                await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer or self._db is None:
            return
        try:
            rows = []
            for d in self._buffer:
                links = json.dumps(d.get("links") or [], ensure_ascii=False)
                metadata = json.dumps(d.get("metadata") or {}, ensure_ascii=False)
                rows.append(
                    (
                        d.get("url"),
                        d.get("title"),
                        d.get("text"),
                        links,
                        metadata,
                        d.get("crawled_at"),
                        d.get("status_code"),
                        d.get("content_type"),
                    )
                )
            await self._db.executemany(
                "INSERT INTO pages (url, title, text, links, metadata, crawled_at, status_code, content_type) VALUES (?,?,?,?,?,?,?,?)",
                rows,
            )
            await self._db.commit()
            self._buffer.clear()
        except Exception as e:
            self._logger.error("SQLite batch insert failed: %s", e)
            raise

    async def close(self) -> None:
        if self._db is not None:
            async with self._lock:
                await self._flush_locked()
                await self._db.close()
                self._db = None
