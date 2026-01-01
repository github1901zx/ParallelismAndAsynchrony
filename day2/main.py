import asyncio
import json
import logging
import time
from pathlib import Path
from typing import List, Dict, Any

# Import AsyncCrawler from Day 1 implementation
from day1.async_crawler import AsyncCrawler


async def fetch_and_parse_many(urls: List[str]) -> List[Dict[str, Any]]:
    crawler = AsyncCrawler(max_concurrent=5)
    try:
        tasks = [crawler.fetch_and_parse(u) for u in urls]
        results = await asyncio.gather(*tasks)
        return results
    finally:
        await crawler.close()


def summarize(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "url": result.get("url"),
        "title": result.get("title"),
        "text_length": len(result.get("text") or ""),
        "links_count": len(result.get("links") or []),
        "links": result.get("links") or [],
        "images_count": len(result.get("images") or []),
    }


async def demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    urls: List[str] = [
        "https://example.com",
        "https://httpbin.org/html",
        "https://www.python.org",
        "https://httpbin.org/links/5/0",
        "https://httpbin.org/status/404",
    ]

    t0 = time.perf_counter()
    results = await fetch_and_parse_many(urls)
    t1 = time.perf_counter()

    out_dir = Path(__file__).parent
    out_file = out_dir / "results.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("Parsed pages summary:")
    for r in results:
        s = summarize(r)
        print(
            f"- {s['url']} | title={s['title']!r} | text_length={s['text_length']} | "
            f"links_count={s['links_count']} | images_count={s['images_count']}"
        )

    print(f"\nSaved JSON: {out_file}")
    print(f"Total time: {t1 - t0:.2f}s")


if __name__ == "__main__":
    asyncio.run(demo())
