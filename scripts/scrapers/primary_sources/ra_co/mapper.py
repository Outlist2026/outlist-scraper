"""Pure mapper: RA GraphQL listing rows -> webhook events (source_kind='primary'). No network."""

from __future__ import annotations

from ..base import clean, make_event, parse_iso

SITE = "https://ra.co"


def map_events(rows: list[dict], meta: dict) -> list[dict]:
    prefix = meta["source_prefix"]
    out: list[dict] = []
    for row in rows or []:
        ev = (row or {}).get("event") or {}
        title, start = clean(ev.get("title"), 200), parse_iso(ev.get("startTime") or ev.get("date"))
        path = clean(ev.get("contentUrl"), 200)
        if not title or start is None or not path.startswith("/events/"):
            continue
        end = parse_iso(ev.get("endTime"))
        if end is not None and end <= start:
            end = None
        venue = ev.get("venue") or {}
        loc = venue.get("location") or {}
        img = next((i.get("filename") for i in ev.get("images") or [] if i.get("type") == "FLYERFRONT"), None) or next((i.get("filename") for i in ev.get("images") or []), None)
        artists = ", ".join(clean(a.get("name"), 60) for a in (ev.get("artists") or [])[:6] if a.get("name"))
        desc = clean(ev.get("content"), 1500) or (f"Line-up: {artists}" if artists else "")
        cost = clean(ev.get("cost"), 60)
        promoters = ev.get("promoters") or []
        out.append(make_event(
            f"{prefix}/{clean(ev.get('id'), 20)}", title=title, start=start, end=end, venue=clean(venue.get("name"), 200),
            address=clean(venue.get("address"), 300), url=SITE + path, image=img, description=desc,
            price=f"€{cost}" if cost[:1].isdigit() else cost,
            organizer=clean((promoters[0] or {}).get("name"), 120) if promoters else "", category="club_nightclub",
            lat=loc.get("latitude"), lng=loc.get("longitude"), upstream_id=str(ev.get("id") or "")))
    return out
