"""Pure mapper: Eventbrite cards + schema.org items -> webhook events (source_kind='primary'). No network.

Time: the detail page ISO start when there is one, else the card local date + time read as Amsterdam (cards of another timezone are skipped).
Never guesses a time: an item with a date only is dropped. Kept: music (category 103), anything on the nightlife listing, party format (11).
Dropped: cancelled, online, more than 36 hours long. The URL is normalised to eventbrite.nl so the same event never appears under two domains.
"""

from __future__ import annotations

import re

from ..base import clean, make_event, parse_iso

MAX_SPAN_S = 36 * 3600
MUSIC, PARTY, CONCERT, FAIR = "EventbriteCategory/103", "EventbriteFormat/11", "EventbriteFormat/6", "EventbriteFormat/5"


def tags(card: dict) -> set[str]:
    return {t.get("tag") for t in (card.get("tags") or []) if isinstance(t, dict) and t.get("tag")}


def canonical(url: str) -> str:
    m = re.search(r"/e/([^?#]+)", url or "")
    return f"https://www.eventbrite.nl/e/{m.group(1)}" if m else ""


def start_of(c: dict):
    detail, card = c.get("detail") or {}, c.get("card") or {}
    if detail.get("startDate") and "T" in str(detail["startDate"]):
        return parse_iso(detail["startDate"]), parse_iso(detail.get("endDate"))
    if card.get("start_date") and card.get("start_time"):
        if card.get("timezone") not in (None, "Europe/Amsterdam"):
            return None, None
        end = parse_iso(f"{card['end_date']}T{card['end_time']}") if card.get("end_date") and card.get("end_time") and not card.get("hide_end_date") else None
        return parse_iso(f"{card['start_date']}T{card['start_time']}"), end
    return None, None


def map_events(candidates: list[dict], meta: dict) -> list[dict]:
    prefix, out = meta["source_prefix"], []
    for c in candidates or []:
        ld, card = (c or {}).get("ld") or {}, (c or {}).get("card") or {}
        if card.get("is_cancelled") or card.get("is_online_event") or "Online" in str(ld.get("eventAttendanceMode", "")):
            continue
        t = tags(card)
        if not (c.get("nightlife") or MUSIC in t or PARTY in t):
            continue
        start, end = start_of(c)
        title = clean(card.get("name") or ld.get("name") or (c.get("detail") or {}).get("name"), 200)
        url = canonical(card.get("url") or ld.get("url") or "")
        if not title or start is None or not url:
            continue
        if end is not None:
            if end <= start:
                end = None  # the page repeats the start when it does not know the end
            elif (end - start).total_seconds() > MAX_SPAN_S:
                continue  # a multi-day run, not one occurrence
        loc = ld.get("location") or (c.get("detail") or {}).get("location") or {}
        adr = loc.get("address") or {}
        address = ", ".join(x for x in (clean(adr.get("streetAddress"), 100), clean(adr.get("postalCode"), 20), clean(adr.get("addressLocality"), 60)) if x)
        geo = loc.get("geo") or {}
        try:
            lat, lng = float(geo.get("latitude")), float(geo.get("longitude"))
        except (TypeError, ValueError):
            lat = lng = None
        cat = "club_nightclub" if (c.get("nightlife") or PARTY in t) else "concert_live_music" if CONCERT in t else "festival" if FAIR in t and MUSIC in t else ""
        image = ld.get("image")
        out.append(make_event(f"{prefix}/{c['id']}", title=title, start=start, end=end, venue=clean(loc.get("name"), 200), address=address, url=url,
                              image=image if isinstance(image, str) else None, description=ld.get("description") or card.get("summary") or "",
                              category=cat, lat=lat, lng=lng, upstream_id=str(c["id"])))
    return out
