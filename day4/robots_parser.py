import asyncio
import logging
import time
from typing import Dict, Optional
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import aiohttp


class RobotsParser:

    def __init__(self) -> None:
        self._logger = logging.getLogger("RobotsParser")
        self._cache: Dict[str, robotparser.RobotFileParser] = {}
        self._fetched_at: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._ttl = 60 * 30

    @staticmethod
    def _robots_url(base_url: str) -> Optional[str]:
        try:
            p = urlparse(base_url)
            if not p.scheme or not p.netloc:
                return None
            return f"{p.scheme}://{p.netloc}/robots.txt"
        except Exception:
            return None

    async def fetch_robots(self, base_url: str) -> Dict[str, str]:
        robots_url = self._robots_url(base_url)
        if not robots_url:
            return {"status": "invalid_base_url"}

        async with self._lock:
            now = time.time()
            rp = self._cache.get(robots_url)
            if rp and (now - self._fetched_at.get(robots_url, 0)) < self._ttl:
                return {"status": "cached"}

            text: Optional[str] = None
            status: Optional[int] = None
            try:
                timeout = aiohttp.ClientTimeout(connect=5, sock_read=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(robots_url, headers={"User-Agent": "AsyncCrawler/1.0"}) as resp:
                        status = resp.status
                        if resp.status == 200:
                            text = await resp.text()
                        else:
                            text = ""
            except Exception as e:
                self._logger.debug("robots.txt fetch failed for %s: %s", robots_url, e)

            parser = robotparser.RobotFileParser()
            parser.set_url(robots_url)
            if text is not None:
                try:
                    parser.parse(text.splitlines())
                except Exception:
                    parser.parse("")
            else:
                parser.parse("")

            self._cache[robots_url] = parser
            self._fetched_at[robots_url] = now
            return {"status": "fetched", "http_status": status or 0}

    def _get_parser(self, url: str) -> Optional[robotparser.RobotFileParser]:
        robots_url = self._robots_url(url)
        if not robots_url:
            return None
        return self._cache.get(robots_url)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        rp = self._get_parser(url)
        if not rp:
            return True
        try:
            return bool(rp.can_fetch(user_agent, url))
        except Exception:
            return True

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> Optional[float]:
        rp = self._get_parser(url)
        if not rp:
            return None
        try:
            delay = rp.crawl_delay(user_agent)
            if delay is None:
                return None
            return float(delay)
        except Exception:
            return None
