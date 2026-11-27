"""Eventbrite Amsterdam (eventbrite.nl). Read-only, no login, no API key (replaces the Apify actor, whose keys ran out).

The public discovery pages are server-rendered: window.__SERVER_DATA__ carries the event cards ("buckets", with the real local start time and tags)
and a schema.org ItemList (venue, address, geo, image). Both are joined by the numeric event id. Items that have only a date (no time) get ONE detail
request each (schema.org Event with a full ISO start), capped. robots.txt: /d/ and /e/ are allowed; the paths with a query string are NOT used at all
(Eventbrite disallows several query patterns and its pagination runs on a disallowed API), so the yield is the first page of each listing.
"""

from __future__ import annotations

import json
import re

BASE = "https://www.eventbrite.nl"
PAGES = ["/d/netherlands--amsterdam/events/", "/d/netherlands--amsterdam/nightlife--events/"]
MAX_DETAILS = 30
MARK = "window.__SERVER_DATA__ = "
ID_RE = re.compile(r"(\d{6,})(?:[/?#]|$)")
LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', re.S)


def event_id(url: str) -> str | None:
    m = ID_RE.search(url or "")
    return m.group(1) if m else None


def _html(http, url: str) -> str:
    status, _headers, raw = http.raw_get(url, {"Accept": "text/html"})
    if status != 200:
        raise RuntimeError(f"{url} HTTP {status}")
    return raw.decode("utf-8", errors="replace")


def parse_page(html: str) -> tuple[list[dict], list[dict]]:
    """([bucket event cards], [schema.org Event items]) of one listing page. ([], []) when the layout changed (the caller treats 0 as a signal)."""
    i = html.find(MARK)
    if i < 0:
        return [], []
    try:
        data, _ = json.JSONDecoder().raw_decode(html[i + len(MARK):])
    except ValueError:
        return [], []
    cards = [e for b in (data.get("buckets") or []) if isinstance(b, dict) for e in (b.get("events") or []) if isinstance(e, dict) and e.get("url")]
    items: list[dict] = []
    for block in data.get("jsonld") or []:
        for el in (block.get("itemListElement") or []) if isinstance(block, dict) else []:
            it = el.get("item") if isinstance(el, dict) else None
            if isinstance(it, dict) and it.get("@type") == "Event" and it.get("url"):
                items.append(it)
    return cards, items


def parse_detail(html: str) -> dict | None:
    for raw in LD_RE.findall(html):
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        for d in data if isinstance(data, list) else [data]:
            if isinstance(d, dict) and d.get("@type") == "Event":
                return d
    return None


def fetch(http, meta: dict, **_) -> list[dict]:
    """[{id, ld, card, nightlife, detail}] one per event id. Detail pages only for items that have no time on the cards."""
    by_id: dict[str, dict] = {}
    for path in (meta.get("params") or {}).get("pages", PAGES):
        cards, items = parse_page(_html(http, BASE + path))
        for c in cards:
            eid = event_id(c["url"])
            if eid:
                by_id.setdefault(eid, {"id": eid, "ld": None, "card": c, "nightlife": False, "detail": None})
        for it in items:
            eid = event_id(it["url"])
            if not eid:
                continue
            row = by_id.setdefault(eid, {"id": eid, "ld": None, "card": None, "nightlife": False, "detail": None})
            row["ld"] = row["ld"] or it
            row["nightlife"] = row["nightlife"] or "nightlife" in path
    out = list(by_id.values())
    asked = 0
    for row in out:
        card = row["card"] or {}
        if card.get("start_date") and card.get("start_time"):
            continue
        if asked >= MAX_DETAILS or not (row["ld"] or {}).get("url"):
            continue
        asked += 1
        tail = row["ld"]["url"].split("/e/", 1)[-1] if "/e/" in row["ld"]["url"] else ""
        row["detail"] = parse_detail(_html(http, f"{BASE}/e/{tail}" if tail else row["ld"]["url"]))
    return out
