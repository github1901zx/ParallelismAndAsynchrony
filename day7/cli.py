import argparse
import asyncio
import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

from day6.storage import JSONStorage, CSVStorage, SQLiteStorage, DataStorage  # type: ignore
from day7.advanced_crawler import AdvancedCrawler

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
            raise ValueError(f"Invalid --output '{v}'. Expected format: format:path")
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


def setup_logging(level: str, log_file: Optional[str] = None, max_bytes: int = 5_000_000, backup_count: int = 3) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    if log_file:
        fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler",
        description="Asynchronous web crawler with parsing, politeness, retries, and multi-format storage",
    )
    parser.add_argument("urls", nargs="*", help="Start URLs to crawl")
    parser.add_argument("--urls-file", help="Path to a text file with one URL per line")
    parser.add_argument("--config", help="Path to JSON or YAML config file with defaults")

    # Crawl scope
    parser.add_argument("--max-pages", type=int, default=100, help="Maximum pages to crawl")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum link depth from start URLs")
    parser.add_argument("--same-domain-only", action="store_true", default=True)
    parser.add_argument("--allow-external", action="store_true", help="Allow crawling external domains")
    parser.add_argument("--include", action="append", default=[], dest="include_patterns", help="Regex: URL must match")
    parser.add_argument("--exclude", action="append", default=[], dest="exclude_patterns", help="Regex: URL must not match")

    # Sitemap
    parser.add_argument("--sitemap", help="Explicit sitemap.xml URL")
    parser.add_argument("--use-sitemap", action="store_true", help="Auto-fetch /sitemap.xml from start domain")

    # Crawler options
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--max-per-domain", type=int, default=None)
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
    parser.add_argument("--report-html", default="report.html", help="Path to save HTML statistics report")

    # Logging
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--log-file", help="Log file path (with rotation)")
    parser.add_argument("--log-max-bytes", type=int, default=5_000_000)
    parser.add_argument("--log-backup-count", type=int, default=3)

    return parser


def _progress_printer(logger: logging.Logger) -> Any:
    last_reported = {"processed": -1}

    def on_progress(payload: Dict[str, Any]) -> None:
        processed = int(payload.get("processed") or 0)
        max_pages = int(payload.get("max_pages") or 0)
        if processed != last_reported["processed"] and processed % 5 == 0:
            last_reported["processed"] = processed
            q = payload.get("queue") or {}
            logger.info(
                "Progress: %d/%d pages | queued=%s in_progress=%s failed=%s",
                processed, max_pages,
                q.get("queued", "?"), q.get("in_progress", "?"), q.get("failed", "?"),
            )

    return on_progress


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

    rps = cfg.get("requests_per_second", args.requests_per_second)
    rps_val = float(rps) if rps else 0.0
    same_domain = not args.allow_external
    if "same_domain_only" in cfg:
        same_domain = bool(cfg["same_domain_only"])

    crawler = AdvancedCrawler(
        max_concurrent=int(cfg.get("max_concurrent", args.max_concurrent)),
        max_per_domain=cfg.get("max_per_domain", args.max_per_domain),
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

    max_pages = int(cfg.get("max_pages", args.max_pages))
    max_depth = int(cfg.get("max_depth", args.max_depth))
    include_patterns = list(cfg.get("include_patterns", []) or []) + list(args.include_patterns or [])
    exclude_patterns = list(cfg.get("exclude_patterns", []) or []) + list(args.exclude_patterns or [])

    try:
        logger.info("Starting crawl from %d seed URL(s), max_pages=%d, max_depth=%d", len(urls), max_pages, max_depth)
        result = await crawler.crawl(
            start_urls=urls,
            max_pages=max_pages,
            max_depth=max_depth,
            same_domain_only=same_domain,
            include_patterns=include_patterns or None,
            exclude_patterns=exclude_patterns or None,
            sitemap_url=cfg.get("sitemap", args.sitemap),
            use_sitemap=bool(cfg.get("use_sitemap", args.use_sitemap)),
            on_progress=_progress_printer(logger),
        )

        speed = crawler.get_speed_stats()
        try:
            errors = crawler.get_error_stats()
        except Exception:
            errors = {}

        report: Dict[str, Any] = {
            "count": len(result.get("processed", {})),
            "failed_count": len(result.get("failed", {})),
            "visited_count": len(result.get("visited", set())),
            "speed_stats": speed,
            "error_stats": errors,
            "queue_stats": result.get("stats", {}),
            "crawler_stats": result.get("crawler_stats", {}),
            "outputs": outputs,
        }
        report_path = str(cfg.get("report", args.report))
        Path(report_path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved JSON report to %s", report_path)

        html_path = str(cfg.get("report_html", args.report_html))
        crawler.export_reports(json_path=None, html_path=html_path, extra={
            "speed_stats": speed,
            "error_stats": errors,
            "queue_stats": result.get("stats", {}),
        })
        logger.info("Saved HTML report to %s", html_path)

        logger.info(
            "Crawl finished: %d processed, %d failed, %.2f pages/sec",
            len(result.get("processed", {})),
            len(result.get("failed", {})),
            crawler.stats.pages_per_second,
        )
    finally:
        await crawler.close()
    return 0


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    cfg: Dict[str, Any] = {}
    if args.config:
        try:
            cfg = load_config(args.config)
        except Exception as e:
            print(f"Failed to load config: {e}")
            return 2

    log_file = cfg.get("log_file", args.log_file)
    setup_logging(
        str(cfg.get("log_level", args.log_level)),
        log_file=log_file,
        max_bytes=int(cfg.get("log_max_bytes", args.log_max_bytes)),
        backup_count=int(cfg.get("log_backup_count", args.log_backup_count)),
    )

    return asyncio.run(run_crawl(args, cfg))


if __name__ == "__main__":
    raise SystemExit(main())
