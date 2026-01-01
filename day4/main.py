import asyncio
import logging
import time
from typing import List

from day1.async_crawler import AsyncCrawler


async def demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    urls: List[str] = [
        "https://example.com/",
        "https://httpbin.org/get",
        "https://www.python.org/",
        "https://www.python.org/search/",
    ]

    crawler = AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        jitter=0.2,
        user_agent="MyBot/1.0",
        retries=1,
    )

    t0 = time.perf_counter()
    try:
        bodies = await crawler.fetch_urls(urls)
    finally:
        stats = crawler.get_speed_stats()
        await crawler.close()
    t1 = time.perf_counter()

    print("Results:")
    for u, body in bodies.items():
        status = "OK" if body else "BLOCKED/EMPTY"
        print(f" - {u}: {status}, length={len(body)}")

    print("\nPoliteness / rate-limit stats:")
    print(f" - avg delay: {stats.get('avg_delay')} s")
    print(f" - req/sec: {stats.get('req_per_sec')}")
    print(f" - robots.txt blocked: {int(stats.get('blocked', 0.0))}")
    print(f" - total requests: {int(stats.get('total_requests', 0.0))}")
    print(f" - wall time: {t1 - t0:.2f}s")


if __name__ == "__main__":
    asyncio.run(demo())
