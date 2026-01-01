import asyncio
import json
import logging
import time
from pathlib import Path
from typing import List, Dict, Any

from day1.async_crawler import AsyncCrawler

async def demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    urls: List[str] = [
        "https://httpbin.org/status/503",
        "https://httpbin.org/status/500",
        "https://httpbin.org/status/429",
        "https://httpbin.org/status/404",
        "https://httpbin.org/delay/3",
        "https://example.com/",
    ]

    crawler = AsyncCrawler(
        max_concurrent=3,
        user_agent="MyBot/1.0",
        retries=3,
        backoff_base=0.5,
        backoff_factor=2.0,
        max_backoff=5.0,
        requests_per_second=2.0,
        respect_robots=False,
        min_delay=0.0,
        jitter=0.0,
        cb_threshold=4,
        cb_window=30.0,
        cb_cooldown=20.0,
    )

    t0 = time.perf_counter()
    try:
        results = await crawler.fetch_urls(urls)
        stats = crawler.get_speed_stats()
        error_stats = {}
        try:
            error_stats = crawler.get_error_stats()
        except Exception:
            pass
    finally:
        await crawler.close()
    t1 = time.perf_counter()

    print("Results:")
    for u, body in results.items():
        status = "OK" if body else "FAIL/EMPTY"
        print(f" - {u}: {status}, len={len(body)}")

    print("\nError/Retry stats:")
    for k, v in (error_stats or {}).items():
        print(f" - {k}: {v}")

    print("\nPoliteness/Rate stats:")
    print(f" - avg delay: {stats.get('avg_delay')} s")
    print(f" - req/sec: {stats.get('req_per_sec')}")
    print(f" - blocked: {int(stats.get('blocked', 0.0))}")
    print(f" - total requests: {int(stats.get('total_requests', 0.0))}")
    print(f" - wall time: {t1 - t0:.2f}s")

    out_dir = Path(__file__).parent
    out_file = out_dir / "error_report.json"
    report: Dict[str, Any] = {
        "results": {u: len(b) for u, b in results.items()},
        "error_stats": error_stats,
        "speed_stats": stats,
        "elapsed": t1 - t0,
    }
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nSaved error report: {out_file}")


if __name__ == "__main__":
    asyncio.run(demo())
