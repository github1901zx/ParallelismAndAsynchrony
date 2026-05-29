import html
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class CrawlerStats:
    """Collects and exports crawl statistics."""

    def __init__(self) -> None:
        self._started_at = datetime.now(timezone.utc)
        self._t0 = time.perf_counter()
        self._end_t: Optional[float] = None
        self.pages_crawled = 0
        self.pages_failed = 0
        self.links_discovered = 0
        self.domains: Dict[str, int] = defaultdict(int)
        self.status_codes: Dict[int, int] = defaultdict(int)
        self.depths: Dict[int, int] = defaultdict(int)
        self.progress_snapshots: List[Dict[str, Any]] = []

    def record_progress(self, payload: Dict[str, Any]) -> None:
        self.progress_snapshots.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **payload,
        })

    def record_page(self, url: str, result: Dict[str, Any], depth: int = 0) -> None:
        self.pages_crawled += 1
        domain = url.split("/")[2] if "://" in url else "unknown"
        self.domains[domain] += 1
        status = int(result.get("status_code") or 0)
        self.status_codes[status] += 1
        self.depths[depth] += 1
        links = result.get("links") or []
        self.links_discovered += len(links)

    def record_failure(self, url: str) -> None:
        self.pages_failed += 1
        domain = url.split("/")[2] if "://" in url else "unknown"
        self.domains[domain] += 1

    def finish(self) -> None:
        self._end_t = time.perf_counter()

    @property
    def elapsed(self) -> float:
        end = self._end_t or time.perf_counter()
        return end - self._t0

    @property
    def pages_per_second(self) -> float:
        elapsed = max(self.elapsed, 1e-6)
        return self.pages_crawled / elapsed

    def to_dict(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "started_at": self._started_at.isoformat(),
            "elapsed_seconds": round(self.elapsed, 3),
            "pages_crawled": self.pages_crawled,
            "pages_failed": self.pages_failed,
            "links_discovered": self.links_discovered,
            "pages_per_second": round(self.pages_per_second, 3),
            "domains": dict(sorted(self.domains.items(), key=lambda x: -x[1])),
            "status_codes": dict(sorted(self.status_codes.items())),
            "depths": dict(sorted(self.depths.items())),
            "progress_events": len(self.progress_snapshots),
        }
        if extra:
            data.update(extra)
        return data

    def export_json(self, path: str, extra: Optional[Dict[str, Any]] = None) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(extra), f, ensure_ascii=False, indent=2)

    def to_html(self, extra: Optional[Dict[str, Any]] = None) -> str:
        data = self.to_dict(extra)
        rows = "".join(
            f"<tr><td>{html.escape(str(k))}</td><td>{html.escape(str(v))}</td></tr>"
            for k, v in data.items()
            if not isinstance(v, (dict, list))
        )
        domain_rows = "".join(
            f"<tr><td>{html.escape(d)}</td><td>{c}</td></tr>"
            for d, c in data.get("domains", {}).items()
        )
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crawler Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #222; }}
    h1 {{ border-bottom: 2px solid #333; padding-bottom: 0.5rem; }}
    table {{ border-collapse: collapse; margin: 1rem 0; min-width: 320px; }}
    th, td {{ border: 1px solid #ccc; padding: 0.4rem 0.8rem; text-align: left; }}
    th {{ background: #f5f5f5; }}
  </style>
</head>
<body>
  <h1>Crawler Statistics Report</h1>
  <h2>Summary</h2>
  <table>{rows}</table>
  <h2>Domains</h2>
  <table>
    <tr><th>Domain</th><th>Pages</th></tr>
    {domain_rows}
  </table>
</body>
</html>"""

    def export_html(self, path: str, extra: Optional[Dict[str, Any]] = None) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_html(extra))
