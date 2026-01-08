import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from day1.async_crawler import AsyncCrawler
from day6.storage import JSONStorage, CSVStorage, SQLiteStorage


URLS: List[str] = [
    "https://example.com/",
    "https://httpbin.org/html",
    "https://www.python.org/",
]


async def run_with_storage(storage, label: str) -> Dict[str, Any]:
    crawler = AsyncCrawler(max_concurrent=5, user_agent="MyBot/1.0", retries=1, storage=storage)
    count = 0
    try:
        for url in URLS:
            data = await crawler.fetch_and_parse(url)
            count += 1 if data else 0
        stats = crawler.get_speed_stats()
        return {"saved": count, "stats": stats}
    finally:
        await crawler.close()


async def demo() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    out_dir = Path(__file__).parent

    json_path = out_dir / "results.ndjson"
    json_storage = JSONStorage(str(json_path))
    json_res = await run_with_storage(json_storage, "JSON")
    print(f"JSON saved {json_res['saved']} items -> {json_path}")

    csv_path = out_dir / "results.csv"
    csv_storage = CSVStorage(str(csv_path))
    csv_res = await run_with_storage(csv_storage, "CSV")
    print(f"CSV saved {csv_res['saved']} items -> {csv_path}")

    sqlite_path = out_dir / "crawler.db"
    sqlite_storage = SQLiteStorage(str(sqlite_path), batch_size=2)
    sqlite_res = await run_with_storage(sqlite_storage, "SQLite")
    print(f"SQLite saved {sqlite_res['saved']} items -> {sqlite_path}")

    print("\nRead-back demo:")
    try:
        read_lines = 0
        with open(json_path, "r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                print(f" - {obj.get('url')} | title={obj.get('title')!r} | links={len(obj.get('links') or [])}")
                read_lines += 1
                if read_lines >= 2:
                    break
    except Exception as e:
        print(f"Failed to read JSON: {e}")

    try:
        import sqlite3
        con = sqlite3.connect(str(sqlite_path))
        cur = con.cursor()
        cur.execute("SELECT url, title, status_code FROM pages LIMIT 2")
        for row in cur.fetchall():
            print(f" - {row[0]} | title={row[1]!r} | status={row[2]}")
        con.close()
    except Exception as e:
        print(f"Failed to read SQLite: {e}")


if __name__ == "__main__":
    asyncio.run(demo())
