import logging
from typing import Any, Callable, Dict, List, Optional

from day1.async_crawler import AsyncCrawler
from day7.crawler_stats import CrawlerStats
from day7.sitemap_parser import SitemapParser


class AdvancedCrawler(AsyncCrawler):
    """Full-featured crawler integrating queue-based crawl, sitemaps, and statistics."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.stats = CrawlerStats()
        self._sitemap_parser = SitemapParser()
        self._logger = logging.getLogger("AdvancedCrawler")

    async def resolve_start_urls(
        self,
        start_urls: List[str],
        sitemap_url: Optional[str] = None,
        use_sitemap: bool = False,
    ) -> List[str]:
        urls = list(start_urls)
        if use_sitemap or sitemap_url:
            target = sitemap_url
            if not target and start_urls:
                target = SitemapParser.guess_sitemap_url(start_urls[0])
            if target:
                self._logger.info("Fetching sitemap: %s", target)
                sitemap_urls = await self._sitemap_parser.fetch_sitemap(target)
                self._logger.info("Sitemap returned %d URL(s)", len(sitemap_urls))
                seen = set(urls)
                for u in sitemap_urls:
                    if u not in seen:
                        seen.add(u)
                        urls.append(u)
        return urls

    async def crawl(
        self,
        start_urls: List[str],
        max_pages: int = 100,
        max_depth: int = 2,
        same_domain_only: bool = True,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        sitemap_url: Optional[str] = None,
        use_sitemap: bool = False,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        resolved = await self.resolve_start_urls(start_urls, sitemap_url=sitemap_url, use_sitemap=use_sitemap)

        def _progress_cb(payload: Dict[str, Any]) -> None:
            self.stats.record_progress(payload)
            if on_progress is not None:
                on_progress(payload)

        result = await super().crawl(
            start_urls=resolved,
            max_pages=max_pages,
            max_depth=max_depth,
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            on_progress=_progress_cb,
        )

        for url, page in result.get("processed", {}).items():
            depth = 0
            for snap in reversed(self.stats.progress_snapshots):
                if snap.get("last_url") == url:
                    depth = int(snap.get("depth") or 0)
                    break
            self.stats.record_page(url, page, depth=depth)

        for url in result.get("failed", {}):
            self.stats.record_failure(url)

        self.stats.finish()
        result["crawler_stats"] = self.stats.to_dict()
        return result

    def export_reports(
        self,
        json_path: Optional[str] = None,
        html_path: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if json_path:
            self.stats.export_json(json_path, extra=extra)
        if html_path:
            self.stats.export_html(html_path, extra=extra)

    async def close(self) -> None:
        await self._sitemap_parser.close()
        await super().close()
