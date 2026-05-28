import logging
import xml.etree.ElementTree as ET
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse

import aiohttp

LOGGER = logging.getLogger("SitemapParser")

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class SitemapParser:
    """Fetches and parses sitemap.xml and sitemap index files."""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None, max_sitemaps: int = 20) -> None:
        self._session = session
        self._max_sitemaps = max_sitemaps
        self._own_session = False

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
        return self._session

    async def close(self) -> None:
        if self._own_session and self._session is not None and not self._session.closed:
            await self._session.close()

    async def fetch_urls(self, sitemap_url: str) -> List[str]:
        seen_sitemaps: Set[str] = set()
        return await self._parse_sitemap(sitemap_url, seen_sitemaps)

    async def _parse_sitemap(self, url: str, seen_sitemaps: Set[str]) -> List[str]:
        if url in seen_sitemaps or len(seen_sitemaps) >= self._max_sitemaps:
            return []
        seen_sitemaps.add(url)

        session = await self._ensure_session()
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    LOGGER.warning("Sitemap fetch failed %s: HTTP %s", url, resp.status)
                    return []
                text = await resp.text()
        except Exception as e:
            LOGGER.warning("Sitemap fetch error for %s: %s", url, e)
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            LOGGER.warning("Sitemap parse error for %s: %s", url, e)
            return []

        tag = root.tag.rsplit("}", 1)[-1] if "}" in root.tag else root.tag
        if tag == "sitemapindex":
            child_urls: List[str] = []
            for node in root.findall(".//sm:sitemap/sm:loc", SITEMAP_NS) or root.findall(".//sitemap/loc"):
                loc = (node.text or "").strip()
                if loc:
                    child_urls.append(loc)
            urls: List[str] = []
            for child in child_urls:
                urls.extend(await self._parse_sitemap(child, seen_sitemaps))
            return urls

        if tag == "urlset":
            result: List[str] = []
            for node in root.findall(".//sm:url/sm:loc", SITEMAP_NS) or root.findall(".//url/loc"):
                loc = (node.text or "").strip()
                if loc and loc.startswith(("http://", "https://")):
                    result.append(loc)
            return result

        LOGGER.warning("Unknown sitemap root tag %r at %s", tag, url)
        return []

    @staticmethod
    def guess_sitemap_url(start_url: str) -> str:
        parsed = urlparse(start_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        return urljoin(base, "/sitemap.xml")
