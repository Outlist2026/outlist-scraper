"""I amsterdam (iamsterdam.com) agenda. Read-only, no login. Official city-marketing site, listing data is JSON embedded in the page
(Next.js flight payload under "content":{"items":[...]}), each item has a detail page with a schema.org Event (JSON-LD).

robots.txt: /en/whats-on/calendar* with a query is disallowed EXCEPT ?page= (allowed on purpose), so only plain category paths and ?page=N are used.
Listing items are SERIES (date = [first, last] of a recurring run) and the detail JSON-LD carries the same range. Only single-occurrence events
are usable, so the client skips long series without a detail request and the mapper drops anything longer than 36 hours.
"""

from __future__ import annotations

import json
import re

from ..base import parse_iso

BASE = "https://www.iamsterdam.com"
CATEGORIES = ["nightlife", "concerts-and-music", "festivals"]
PAGES_PER_CATEGORY = 2
MAX_DETAILS = 40
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S)
FLIGHT_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)</script>', re.S)


def _html(http, url: str) -> str:
    """GET a page as text through the polite client (same pacing, request budget and size cap as the JSON calls)."""
    status, _headers, raw = http.raw_get(url, {"Accept": "text/html"})
    if status != 200:
        raise RuntimeError(f"{url} HTTP {status}")
    return raw.decode("utf-8", errors="replace")


def parse_listing(html: str) -> list[dict]:
    """Items of the listing from the embedded flight JSON. [] when the page layout changed (the caller treats 0 as a signal)."""
    try:
        blob = "".join(json.loads('"' + p + '"') for p in FLIGHT_RE.findall(html))
    except ValueError:
        return []
    i = blob.find('"content":{"items":[')
    if i < 0:
        return []
    try:
        items, _ = json.JSONDecoder().raw_decode(blob[i + len('"content":{"items":'):])
    except ValueError:
        return []
    return [x for x in items if isinstance(x, dict) and x.get("type") == "Event" and isinstance(x.get("slug"), str) and x["slug"].startswith("whats-on/calendar/")]


def parse_detail(html: str) -> dict | None:
    """The schema.org Event object of a detail page, or None."""
    for raw in LD_RE.findall(html):
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        for d in data if isinstance(data, list) else [data]:
            if isinstance(d, dict) and d.get("@type") == "Event":
                return d
    return None


def is_series(date_range: list | None) -> bool:
    """True when the listing item spans more than ~36 hours: a recurring run with no occurrence list, not usable."""
    if not date_range or len(date_range) != 2:
        return False
    a, b = parse_iso(date_range[0]), parse_iso(date_range[1])
    return bool(a and b and (b - a).total_seconds() > 36 * 3600)


def fetch(http, meta: dict, **_) -> list[dict]:
    """[{listing, ld}] for single-day candidates. Detail pages are requested only for plausible single occurrences."""
    out: list[dict] = []
    seen: set[str] = set()
    for cat in (meta.get("params") or {}).get("categories", CATEGORIES):
        for page in range(1, PAGES_PER_CATEGORY + 1):
            items = parse_listing(_html(http, f"{BASE}/en/whats-on/calendar/{cat}" + (f"?page={page}" if page > 1 else "")))
            if not items:
                break
            for it in items:
                if it["slug"] in seen or is_series(it.get("date")):
                    continue
                seen.add(it["slug"])
                if len(out) >= MAX_DETAILS:
                    return out
                ld = parse_detail(_html(http, f"{BASE}/en/{it['slug']}"))
                if ld:
                    out.append({"listing": it, "ld": ld})
    return out
