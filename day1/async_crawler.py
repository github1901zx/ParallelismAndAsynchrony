import asyncio
import logging
import re
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Any, Set, Tuple

import aiohttp

try:
    from day2.html_parser import HTMLParser
except Exception:
    HTMLParser = None

try:
    from day5.retry_strategy import RetryStrategy, TransientError, PermanentError, NetworkError, ParseError  # type: ignore
except Exception:
    class TransientError(Exception):
        pass
    class PermanentError(Exception):
        pass
    class NetworkError(Exception):
        pass
    class ParseError(Exception):
        pass
    RetryStrategy = None

from html.parser import HTMLParser as _StdlibHTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

try:
    from day3.crawler_queue import CrawlerQueue
    from day3.semaphore_manager import SemaphoreManager
except Exception:
    CrawlerQueue = None    SemaphoreManager = None


def _normalize_ws(text: str) -> str:
    return " ".join((text or "").split())


def _to_absolute_url(href: Optional[str], base_url: str) -> Optional[str]:
    if not href:
        return None
    abs_url = urljoin(base_url, href)
    try:
        parsed = urlparse(abs_url)
        if parsed.scheme not in {"http", "https"}:
            return None
        parsed = parsed._replace(fragment="")
        normalized = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", parsed.params, parsed.query, parsed.fragment))
        return normalized
    except Exception:
        return None


class _StdlibFallbackParser:
    """A minimal HTML parser using the stdlib to extract basic info when bs4 isn't installed."""

    class _P(_StdlibHTMLParser):
        def __init__(self, base_url: str) -> None:
            super().__init__(convert_charrefs=True)
            self.base_url = base_url
            self.in_title = False
            self.title_parts: List[str] = []
            self.links: List[str] = []
            self.images: List[Dict[str, Optional[str]]] = []
            self.text_parts: List[str] = []
            self._skip_stack: List[str] = []  # track script/style

        def handle_starttag(self, tag: str, attrs):
            attrs_dict = {k.lower(): v for k, v in attrs}
            if tag.lower() in ("script", "style", "noscript"):
                self._skip_stack.append(tag.lower())
            if tag.lower() == "title":
                self.in_title = True
            if tag.lower() == "a":
                href = attrs_dict.get("href")
                abs_url = _to_absolute_url(href, self.base_url)
                if abs_url:
                    self.links.append(abs_url)
            if tag.lower() == "img":
                src = attrs_dict.get("src")
                abs_src = _to_absolute_url(src, self.base_url)
                alt = attrs_dict.get("alt")
                self.images.append({"src": abs_src, "alt": _normalize_ws(alt) if isinstance(alt, str) else alt})

        def handle_endtag(self, tag: str):
            if tag.lower() == "title":
                self.in_title = False
            if self._skip_stack and tag.lower() == self._skip_stack[-1]:
                self._skip_stack.pop()

        def handle_data(self, data: str):
            if self._skip_stack:
                return
            if self.in_title:
                self.title_parts.append(data)
            else:
                self.text_parts.append(data)

    async def parse_html(self, html: str, url: str) -> Dict[str, Any]:
        parser = self._P(url)
        try:
            parser.feed(html or "")
            parser.close()
        except Exception:
            # Even if malformed, return what we have
            pass
        title = _normalize_ws("".join(parser.title_parts)) or None
        text = _normalize_ws(" ".join(parser.text_parts))
        # Deduplicate links while preserving order
        seen = set()
        links: List[str] = []
        for u in parser.links:
            if u and u not in seen:
                seen.add(u)
                links.append(u)
        return {
            "url": url,
            "title": title,
            "text": text,
            "links": links,
            "metadata": {"title": title, "description": None, "keywords": None},
            "images": parser.images,
            "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [],
            "lists": {"ul": [], "ol": []},
        }

# Day_2
class AsyncCrawler:

    def __init__(self, max_concurrent: int = 10, connect_timeout: float = 10.0, read_timeout: float = 20.0,
                 user_agent: str = "AsyncCrawler/1.0",
                 requests_per_second: Optional[float] = None,
                 per_domain_rate: bool = True,
                 respect_robots: bool = False,
                 min_delay: float = 0.0,
                 jitter: float = 0.0,
                 backoff_base: float = 0.5,
                 backoff_factor: float = 2.0,
                 max_backoff: float = 5.0,
                 retries: int = 0,
                 retry_strategy: Optional[Any] = None,
                 cb_threshold: int = 0,
                 cb_window: float = 30.0,
                 cb_cooldown: float = 60.0,
                 storage: Optional[Any] = None) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: Optional[aiohttp.ClientSession] = None
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._user_agent = user_agent
        self._logger = logging.getLogger("AsyncCrawler")
        self._parser: Any = HTMLParser() if HTMLParser else _StdlibFallbackParser()
        self._storage = storage
        # Day 3 state
        self.visited_urls: set = set()
        self.failed_urls: Dict[str, str] = {}
        self.processed_urls: Dict[str, Any] = {}
        # Day 4 components
        try:
            from day4.rate_limiter import RateLimiter  # type: ignore
            self._rate_limiter = RateLimiter(requests_per_second=requests_per_second or 0.0, per_domain=per_domain_rate) if requests_per_second else None
        except Exception:
            self._rate_limiter = None
        try:
            from day4.robots_parser import RobotsParser  # type: ignore
            self._robots = RobotsParser()
        except Exception:
            self._robots = None
        self._respect_robots = bool(respect_robots)
        self._min_delay = float(min_delay) if min_delay and min_delay > 0 else 0.0
        self._jitter = float(jitter) if jitter and jitter > 0 else 0.0
        self._last_request_time_per_domain: Dict[str, float] = {}
        self._total_blocked = 0
        self._total_requests = 0
        self._cumulative_delay = 0.0
        self._backoff_base = float(backoff_base)
        self._backoff_factor = float(backoff_factor)
        self._max_backoff = float(max_backoff)
        self._retries = int(retries)
        # Day 5 retry strategy
        if retry_strategy is not None:
            self._retry_strategy = retry_strategy
        else:
            self._retry_strategy = RetryStrategy(max_retries=self._retries, backoff_factor=self._backoff_factor,
                                                backoff_base=self._backoff_base, max_backoff=self._max_backoff,
                                                jitter=self._jitter) if RetryStrategy else None
        # error statistics
        self._error_stats: Dict[str, float] = {
            "TransientError": 0.0,
            "NetworkError": 0.0,
            "PermanentError": 0.0,
            "ParseError": 0.0,
            "successful_retries": 0.0,
            "avg_retry_wait": 0.0,
            "retry_events": 0.0,
        }
        self._retry_wait_total = 0.0
        # circuit breaker config and state
        self._cb_threshold = int(max(0, cb_threshold))
        self._cb_window = float(cb_window)
        self._cb_cooldown = float(cb_cooldown)
        self._cb_failures: Dict[str, list] = {}
        self._cb_open_until: Dict[str, float] = {}
        self._start_time = time.perf_counter()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(connect=self._connect_timeout, sock_read=self._read_timeout)
            connector = aiohttp.TCPConnector(limit=100)  # pooling; limit total connections
            headers = {"User-Agent": self._user_agent}
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers)
        return self._session

    def _domain(self, url: str) -> str:
        try:
            return (urlparse(url).netloc or "").lower()
        except Exception:
            return ""

    def _cb_is_open(self, domain: str) -> bool:
        until = self._cb_open_until.get(domain)
        return bool(until and until > time.perf_counter())

    def _cb_record_failure(self, domain: str) -> None:
        if self._cb_threshold <= 0:
            return
        now = time.perf_counter()
        lst = self._cb_failures.setdefault(domain, [])
        lst.append(now)
        # drop old
        cutoff = now - self._cb_window
        self._cb_failures[domain] = [t for t in lst if t >= cutoff]
        if len(self._cb_failures[domain]) >= self._cb_threshold:
            self._cb_open_until[domain] = now + self._cb_cooldown
            self._logger.warning(f"Circuit open for domain {domain} for {self._cb_cooldown:.1f}s")

    def _cb_record_success(self, domain: str) -> None:
        self._cb_failures.pop(domain, None)
        self._cb_open_until.pop(domain, None)

    async def _attempt_fetch(self, url: str, attempt: int = 0) -> str:
        body, status, content_type = await self._attempt_fetch_page(url, attempt=attempt)
        return body

    async def _attempt_fetch_page(self, url: str, attempt: int = 0) -> Tuple[str, int, str]:
        assert self._session is not None
        connect_to = self._connect_timeout * (1.0 + 0.5 * attempt)
        read_to = self._read_timeout * (1.0 + 0.5 * attempt)
        timeout = aiohttp.ClientTimeout(connect=connect_to, sock_read=read_to)
        try:
            async with self._session.get(url, timeout=timeout) as resp:
                text = await resp.text()
                self._total_requests += 1
                status = resp.status
                ctype = resp.headers.get("Content-Type", "")
                if status == 429 or status == 503 or status == 500:
                    self._logger.warning(f"HTTP {status} for {url}")
                    raise TransientError(f"HTTP {status}")
                if status in (404, 403, 401):
                    self._logger.warning(f"HTTP {status} for {url}")
                    raise PermanentError(f"HTTP {status}")
                if status >= 400:
                    # other client errors: treat as permanent by default
                    self._logger.warning(f"HTTP {status} for {url}")
                    raise PermanentError(f"HTTP {status}")
                self._logger.info(f"Done: {url} [{status}]")
                return text, status, ctype
        except asyncio.TimeoutError as e:
            self._logger.warning(f"Timeout while fetching {url}")
            raise TransientError(str(e))
        except aiohttp.ClientError as e:
            self._logger.warning(f"Network error while fetching {url}: {e}")
            raise NetworkError(str(e))

    async def fetch_url(self, url: str) -> str:
        body, _, _ = await self.fetch_page(url)
        return body

    async def fetch_page(self, url: str) -> Tuple[str, int, str]:
        await self._ensure_session()
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower()
        if self._cb_is_open(domain):
            self._logger.warning(f"Circuit open: skipping {url}")
            return "", 0, ""
        if self._respect_robots and self._robots is not None:
            try:
                await self._robots.fetch_robots(url)
                if not self._robots.can_fetch(url, user_agent=self._user_agent):
                    self._total_blocked += 1
                    self._logger.info(f"Blocked by robots.txt: {url}")
                    return "", 0, ""
            except Exception as e:
                self._logger.debug(f"Robots check failed for {url}: {e}")
        delay = 0.0
        if self._respect_robots and self._robots is not None:
            try:
                rd = self._robots.get_crawl_delay(url, user_agent=self._user_agent)
                if rd and rd > delay:
                    delay = rd
            except Exception:
                pass
        if self._min_delay > delay:
            delay = self._min_delay
        if self._jitter > 0:
            delay += random.uniform(0, self._jitter)
        if delay > 0:
            last = self._last_request_time_per_domain.get(domain, 0.0)
            now = time.perf_counter()
            to_wait = delay - (now - last)
            if to_wait > 0:
                await asyncio.sleep(to_wait)
                self._cumulative_delay += to_wait
        if self._rate_limiter is not None:
            try:
                await self._rate_limiter.acquire(domain if domain else None)
            except Exception:
                pass
        self._last_request_time_per_domain[domain] = time.perf_counter()
        async with self._semaphore:
            self._logger.info(f"Start fetching: {url}")
            if self._retry_strategy is None or self._retries <= 0:
                try:
                    body, status, ctype = await self._attempt_fetch_page(url, attempt=0)
                    return body, status, ctype
                except PermanentError as e:
                    self.failed_urls[url] = str(e)
                    self._error_stats["PermanentError"] += 1.0
                    self._cb_record_failure(domain)
                    return "", 0, ""
                except TransientError:
                    self._error_stats["TransientError"] += 1.0
                    self._cb_record_failure(domain)
                    return "", 0, ""
                except NetworkError:
                    self._error_stats["NetworkError"] += 1.0
                    self._cb_record_failure(domain)
                    return "", 0, ""
                except Exception as e:
                    self._logger.exception(f"Unexpected error while fetching {url}: {e}")
                    return "", 0, ""
            retry_events_before = self._error_stats["retry_events"]
            def on_retry(attempt_idx: int, exc: BaseException, sleep_for: float) -> None:
                self._error_stats["retry_events"] += 1.0
                self._retry_wait_total += sleep_for
                # record error category
                if isinstance(exc, PermanentError):
                    self._error_stats["PermanentError"] += 1.0
                elif isinstance(exc, NetworkError):
                    self._error_stats["NetworkError"] += 1.0
                else:
                    self._error_stats["TransientError"] += 1.0
                self._logger.info(f"Retrying {url}: attempt={attempt_idx+1}, error={type(exc).__name__}, next in {sleep_for:.2f}s")
                self._cumulative_delay += sleep_for
            try:
                body, status, ctype = await self._retry_strategy.execute_with_retry(self._attempt_fetch_page, url, on_retry=on_retry)
                if self._error_stats["retry_events"] > retry_events_before:
                    self._error_stats["successful_retries"] += 1.0
                self._cb_record_success(domain)
                ev = self._error_stats["retry_events"]
                self._error_stats["avg_retry_wait"] = (self._retry_wait_total / ev) if ev > 0 else 0.0
                return body, status, ctype
            except PermanentError as e:
                self.failed_urls[url] = str(e)
                self._error_stats["PermanentError"] += 1.0
                self._cb_record_failure(domain)
                return "", 0, ""
            except (TransientError, NetworkError) as e:
                if isinstance(e, NetworkError):
                    self._error_stats["NetworkError"] += 1.0
                else:
                    self._error_stats["TransientError"] += 1.0
                self._cb_record_failure(domain)
                return "", 0, ""
            except Exception as e:
                self._logger.exception(f"Unexpected error while fetching {url}: {e}")
                return "", 0, ""

    async def fetch_urls(self, urls: List[str]) -> Dict[str, str]:
        tasks = [asyncio.create_task(self.fetch_url(u)) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return {u: r for u, r in zip(urls, results)}

    async def fetch_and_parse(self, url: str) -> Dict[str, Any]:
        html, status_code, content_type = await self.fetch_page(url)
        crawled_at = datetime.utcnow().isoformat() + "Z"
        base_result: Dict[str, Any] = {
            "url": url,
            "title": None,
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [],
            "lists": {"ul": [], "ol": []},
        }
        result: Dict[str, Any] = dict(base_result)
        if html:
            try:
                parsed = await self._parser.parse_html(html, url)
                result = parsed
            except Exception as e:
                try:
                    self._error_stats["ParseError"] += 1.0
                except Exception:
                    pass
                self._logger.warning(f"Parsing failed for {url}: {e}")
                result = base_result
        standardized: Dict[str, Any] = {
            "url": url,
            "title": result.get("title"),
            "text": result.get("text") or "",
            "links": result.get("links") or [],
            "metadata": result.get("metadata") or {},
            "crawled_at": crawled_at,
            "status_code": int(status_code or 0),
            "content_type": content_type or "",
        }
        standardized["images"] = result.get("images") or []
        standardized["headings"] = result.get("headings") or {"h1": [], "h2": [], "h3": []}
        standardized["tables"] = result.get("tables") or []
        standardized["lists"] = result.get("lists") or {"ul": [], "ol": []}
        # Save if storage is configured
        if self._storage is not None:
            for i in range(3):
                try:
                    await self._storage.save(standardized)
                    break
                except Exception as e:
                    self._logger.error(f"Storage save failed for {url} (attempt {i+1}/3): {e}")
                    await asyncio.sleep(min(1.0 * (i + 1), 3.0))
        return standardized

    def get_speed_stats(self) -> Dict[str, float]:
        elapsed_delays = self._cumulative_delay
        avg_delay = (elapsed_delays / self._total_requests) if self._total_requests > 0 else 0.0
        elapsed = max(1e-6, time.perf_counter() - self._start_time)
        rps = self._total_requests / elapsed
        return {
            "avg_delay": round(avg_delay, 3),
            "req_per_sec": round(rps, 2),
            "blocked": float(self._total_blocked),
            "total_requests": float(self._total_requests),
        }

    def get_error_stats(self) -> Dict[str, Any]:
        ev = self._error_stats.get("retry_events", 0.0)
        if ev > 0:
            self._error_stats["avg_retry_wait"] = (self._retry_wait_total / ev)
        self._error_stats["permanent_urls"] = float(len(self.failed_urls))
        stats: Dict[str, Any] = dict(self._error_stats)
        stats["permanent_error_urls"] = list(self.failed_urls.keys())
        return stats

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._logger.info("HTTP session closed")
        try:
            if getattr(self, "_storage", None) is not None:
                await self._storage.close()
        except Exception as e:
            self._logger.warning(f"Storage close failed: {e}")
