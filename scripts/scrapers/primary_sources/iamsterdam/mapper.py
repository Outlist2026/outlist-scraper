"""Pure mapper: iamsterdam detail JSON-LD -> webhook events (source_kind='primary'). No network.

Quirk: the site stamps wall-clock times with a +00:00 offset ("23:30+00:00" for a 23:30 Amsterdam party), so the offset is dropped and the clock
is read as Europe/Amsterdam. Series (longer than 36 hours) are skipped: the feed has no per-occurrence list.
"""

from __future__ import annotations

import re

from ..base import clean, make_event, parse_iso

MAX_SPAN_S = 36 * 3600
CATEGORY = {"clubbing": "club_nightclub", "nightlife": "club_nightclub", "concerts-and-music": "concert_live_music", "festivals": "festival"}


def wallclock(value):
    return parse_iso(re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", value)) if isinstance(value, str) else None


def map_events(candidates: list[dict], meta: dict) -> list[dict]:
    prefix, out = meta["source_prefix"], []
    for c in candidates or []:
        ld, it = (c or {}).get("ld") or {}, (c or {}).get("listing") or {}
        title, url = clean(ld.get("name") or it.get("title"), 200), str(ld.get("url") or "")
        start, end = wallclock(ld.get("startDate")), wallclock(ld.get("endDate"))
        if not title or start is None or not url.startswith("https://www.iamsterdam.com/"):
            continue
        if end is not None and end > start and (end - start).total_seconds() > MAX_SPAN_S:
            continue  # a series, not an occurrence
        if end is not None and end <= start:
            end = None  # the feed repeats the start as the end when it does not know it
        loc = ld.get("location") or {}
        adr = loc.get("address") or {}
        address = ", ".join(x for x in (clean(adr.get("streetAddress"), 100), clean(adr.get("postalCode"), 20), clean(adr.get("addressLocality"), 60)) if x)
        geo = loc.get("geo") or {}
        image = ld.get("image")[0] if isinstance(ld.get("image"), list) and ld.get("image") else ld.get("image")
        slug_parts = str(it.get("slug") or url).split("/")
        cat = next((CATEGORY[p] for p in slug_parts if p in CATEGORY), "")
        out.append(make_event(f"{prefix}/{slug_parts[-1]}", title=title, start=start, end=end, venue=clean(loc.get("name"), 200), address=address,
                              url=url, image=image if isinstance(image, str) else None, description=ld.get("description") or "", category=cat,
                              lat=geo.get("latitude"), lng=geo.get("longitude"), upstream_id=str(it.get("id") or "")))
    return out
