import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from day1.async_crawler import AsyncCrawler
from day6.storage import JSONStorage, CSVStorage, SQLiteStorage, DataStorage  # type: ignore

try:
    import yaml
except Exception:
    yaml = None


class CompositeStorage(DataStorage):
    def __init__(self, backends: List[DataStorage]) -> None:
        self._backends = backends

    async def save(self, data: Dict[str, Any]) -> None:
        for b in self._backends:
            try:
                await b.save(data)
            except Exception as e:
                logging.getLogger("CLI").error("Storage backend failed: %s", e)

    async def close(self) -> None:
        for b in self._backends:
            try:
                await b.close()
            except Exception:
                pass


def parse_outputs(values: List[str]) -> List[DataStorage]:
    backends: List[DataStorage] = []
    for v in values:
        if ":" not in v:
            raise ValueError(f"Invalid --output '{v}'. Expected format:path")
        fmt, path = v.split(":", 1)
        fmt = fmt.strip().lower()
        path = path.strip()
        if fmt == "json":
            backends.append(JSONStorage(path))
        elif fmt == "csv":
            backends.append(CSVStorage(path))
        elif fmt == "sqlite":
            backends.append(SQLiteStorage(path))
        else:
            raise ValueError(f"Unsupported output format: {fmt}")
    return backends


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    text = p.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except Exception:
        pass
    if yaml is not None:
        try:
            obj = yaml.safe_load(text)
            return obj or {}
        except Exception:
            pass
    raise ValueError("Config file must be valid JSON" + (" or YAML" if yaml is not None else ""))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler",
        description="Asynchronous web crawler with parsing, politeness, retries, and multi-format storage",
    )
    parser.add_argument("urls", nargs="*", help="Start URLs to crawl")
    parser.add_argument("--urls-file", help="Path to a text file with one URL per line")
    parser.add_argument("--config", help="Path to JSON or YAML config file with defaults")

    # Crawler options
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--backoff-base", type=float, default=0.5)
    parser.add_argument("--backoff-factor", type=float, default=2.0)
    parser.add_argument("--max-backoff", type=float, default=5.0)
    parser.add_argument("--requests-per-second", type=float, default=0.0, help="0 disables explicit rate limiter")
    parser.add_argument("--respect-robots", action="store_true")
    parser.add_argument("--min-delay", type=float, default=0.0)
    parser.add_argument("--jitter", type=float, default=0.0)
    parser.add_argument("--user-agent", default="AsyncCrawler/1.0")

    # Output options
    parser.add_argument(
        "--output",
        action="append",
        default=[],
        help="Output target in the form format:path. Repeat to save to multiple formats. Supported: json, csv, sqlite",
    )
    parser.add_argument("--report", default="report.json", help="Path to save summary report JSON")

    # Logging
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return parser


async def run_crawl(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    logger = logging.getLogger("CLI")

    urls: List[str] = []
    urls.extend(cfg.get("urls", []) or [])
    if args.urls:
        urls.extend(args.urls)
    if args.urls_file:
        p = Path(args.urls_file)
        if not p.exists():
            logger.error("URLs file not found: %s", p)
            return 2
        urls.extend([ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")])
    seen = set()
    urls = [u for u in urls if not (u in seen or seen.add(u))]
    if not urls:
        logger.error("No URLs provided. Use positional URL args or --urls-file or config 'urls'.")
        return 2

    outputs: List[str] = []
    outputs.extend(cfg.get("output", []) or [])
    outputs.extend(args.output or [])
    if not outputs:
        outputs = ["json:results.ndjson"]
    try:
        storages = parse_outputs(outputs)
    except Exception as e:
        logger.error("%s", e)
        return 2
    storage: DataStorage = CompositeStorage(storages) if len(storages) > 1 else storages[0]

    # Rate limiter setting
    rps = cfg.get("requests_per_second", args.requests_per_second)
    rps_val = float(rps) if rps else 0.0

    crawler = AsyncCrawler(
        max_concurrent=int(cfg.get("max_concurrent", args.max_concurrent)),
        user_agent=str(cfg.get("user_agent", args.user_agent)),
        retries=int(cfg.get("retries", args.retries)),
        backoff_base=float(cfg.get("backoff_base", args.backoff_base)),
        backoff_factor=float(cfg.get("backoff_factor", args.backoff_factor)),
        max_backoff=float(cfg.get("max_backoff", args.max_backoff)),
        requests_per_second=rps_val if rps_val > 0 else None,
        respect_robots=bool(cfg.get("respect_robots", args.respect_robots)),
        min_delay=float(cfg.get("min_delay", args.min_delay)),
        jitter=float(cfg.get("jitter", args.jitter)),
        storage=storage,
    )

    # Crawl concurrently: fetch_and_parse all URLs
    try:
        logger.info("Starting crawl of %d URL(s)", len(urls))
        tasks = [crawler.fetch_and_parse(u) for u in urls]
        results = await asyncio.gather(*tasks)
        # Produce summary report
        speed = crawler.get_speed_stats()
        try:
            errors = crawler.get_error_stats()
        except Exception:
            errors = {}
        report: Dict[str, Any] = {
            "count": len(results),
            "speed_stats": speed,
            "error_stats": errors,
            "outputs": outputs,
        }
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved report to %s", args.report)
    finally:
        await crawler.close()
    return 0


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    cfg: Dict[str, Any] = {}
    if args.config:
        try:
            cfg = load_config(args.config)
        except Exception as e:
            print(f"Failed to load config: {e}")
            return 2

    return asyncio.run(run_crawl(args, cfg))



if __name__ == "__main__":
    raise SystemExit(main())
