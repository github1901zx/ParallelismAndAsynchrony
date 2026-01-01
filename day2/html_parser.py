import logging
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup


def _normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def _to_absolute_url(href: str, base_url: str) -> Optional[str]:
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


class HTMLParser:
    """
    Helper class to parse HTML and extract structured data.
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger("HTMLParser")

    async def parse_html(self, html: str, url: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {
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
        soup: Optional[BeautifulSoup] = None
        try:
            soup = BeautifulSoup(html or "", "lxml")
        except Exception as e:
            self._logger.warning(f"Failed to parse with lxml for {url}: {e}. Falling back to html.parser")
            try:
                soup = BeautifulSoup(html or "", "html.parser")
            except Exception as e2:
                self._logger.warning(f"Fallback parser failed for {url}: {e2}")
                return result

        try:
            result["metadata"] = self.extract_metadata(soup)
            result["title"] = result["metadata"].get("title")
        except Exception as e:
            self._logger.warning(f"Metadata extraction failed for {url}: {e}")

        try:
            result["links"] = self.extract_links(soup, base_url=url)
        except Exception as e:
            self._logger.warning(f"Link extraction failed for {url}: {e}")

        try:
            result["text"] = self.extract_text(soup)
        except Exception as e:
            self._logger.warning(f"Text extraction failed for {url}: {e}")

        try:
            result["images"] = self.extract_images(soup, base_url=url)
        except Exception as e:
            self._logger.warning(f"Image extraction failed for {url}: {e}")

        try:
            result["headings"] = self.extract_headings(soup)
        except Exception as e:
            self._logger.warning(f"Heading extraction failed for {url}: {e}")

        try:
            result["tables"] = self.extract_tables(soup)
        except Exception as e:
            self._logger.warning(f"Table extraction failed for {url}: {e}")

        try:
            result["lists"] = self.extract_lists(soup)
        except Exception as e:
            self._logger.warning(f"List extraction failed for {url}: {e}")

        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        seen = set()
        links: List[str] = []
        for a in soup.find_all("a"):
            href = a.get("href")
            abs_url = _to_absolute_url(href, base_url)
            if not abs_url:
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)
            links.append(abs_url)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: Optional[str] = None) -> str:
        # Remove non-content elements
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        if selector:
            nodes = soup.select(selector)
            text = " ".join(_normalize_whitespace(n.get_text(" ", strip=True)) for n in nodes)
        else:
            text = soup.get_text(" ", strip=True)
        return _normalize_whitespace(text)

    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        title = None
        if soup.title and soup.title.string:
            title = _normalize_whitespace(soup.title.string)
        desc = None
        kw = None
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            desc = _normalize_whitespace(md.get("content"))
        mk = soup.find("meta", attrs={"name": "keywords"})
        if mk and mk.get("content"):
            kw = _normalize_whitespace(mk.get("content"))
        return {"title": title, "description": desc, "keywords": kw}

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Optional[str]]]:
        images: List[Dict[str, Optional[str]]] = []
        for img in soup.find_all("img"):
            src = img.get("src")
            abs_src = _to_absolute_url(src, base_url)
            alt = img.get("alt")
            images.append({"src": abs_src, "alt": _normalize_whitespace(alt) if isinstance(alt, str) else alt})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        def texts(tag: str) -> List[str]:
            return [_normalize_whitespace(h.get_text(" ", strip=True)) for h in soup.find_all(tag)]

        return {
            "h1": texts("h1"),
            "h2": texts("h2"),
            "h3": texts("h3"),
        }

    def extract_tables(self, soup: BeautifulSoup) -> List[List[List[str]]]:
        tables: List[List[List[str]]] = []
        for table in soup.find_all("table"):
            rows: List[List[str]] = []
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])  # keep header cells too
                row = [_normalize_whitespace(c.get_text(" ", strip=True)) for c in cells]
                if row:
                    rows.append(row)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> Dict[str, List[List[str]]]:
        def list_items(name: str) -> List[List[str]]:
            all_lists: List[List[str]] = []
            for lst in soup.find_all(name):
                items = [
                    _normalize_whitespace(li.get_text(" ", strip=True)) for li in lst.find_all("li", recursive=False)
                ]
                if items:
                    all_lists.append(items)
            return all_lists

        return {"ul": list_items("ul"), "ol": list_items("ol")}
