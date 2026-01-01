import asyncio
import logging
import time
from typing import List

from async_crawler import AsyncCrawler


async def demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    urls: List[str] = [
        "https://example.com",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/3",
        "https://httpbin.org/status/404",
        "https://www.python.org",
        "https://httpbin.org/get",
        "https://nonexistent.domain.test/",
    ]

    crawler = AsyncCrawler(max_concurrent=5)

    t0 = time.perf_counter()
    sequential_results = {}
    for u in urls:
        sequential_results[u] = await crawler.fetch_url(u)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    parallel_results = await crawler.fetch_urls(urls)
    t3 = time.perf_counter()

    await crawler.close()

    print("Sequential results:")
    for u, body in sequential_results.items():
        status = "OK" if body else "FAIL"
        print(f" - {u}: {status}, length={len(body)}")

    print("\nParallel results:")
    for u, body in parallel_results.items():
        status = "OK" if body else "FAIL"
        print(f" - {u}: {status}, length={len(body)}")

    seq_time = t1 - t0
    par_time = t3 - t2
    print(f"\n⏱️ Sequential time: {seq_time:.2f}s")
    print(f"⏱️ Parallel time:   {par_time:.2f}s")
    if par_time > 0:
        print(f"⚡ Speedup: {seq_time / par_time:.2f}x")


if __name__ == '__main__':
    asyncio.run(demo())
