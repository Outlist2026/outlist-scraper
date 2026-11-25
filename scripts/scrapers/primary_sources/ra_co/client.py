"""Resident Advisor public GraphQL (https://ra.co/graphql), listing query only. Read-only, no login, no cookies.

Found by inspecting the network calls of https://ra.co/events/nl/amsterdam (protocol step 2). robots.txt does not cover /graphql
(it disallows /api/ only). The Terms of Use forbid automated access without written permission, so this source ships OFF
(meta.json enabled=false) until the owner has that permission. See meta.json.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

ENDPOINT = "https://ra.co/graphql"
PAGE_SIZE = 50
MAX_PAGES = 6
AREA_AMSTERDAM = 29  # RA area id of Amsterdam (the url /events/nl/amsterdam)
DAYS_AHEAD = 90

QUERY = (
    "query Listing($f: FilterInputDtoInput, $n: Int, $p: Int) { eventListings(filters: $f, pageSize: $n, page: $p) { totalResults "
    "data { id listingDate event { id title date startTime endTime contentUrl images { filename type } cost isTicketed content "
    "venue { id name address area { name } location { latitude longitude } } artists { name } promoters { name } } } } }"
)


def fetch(http, meta: dict, now: datetime | None = None) -> list[dict]:
    """All listing rows for the area from today to +DAYS_AHEAD, paged. Returns the raw `data` rows."""
    now = now or datetime.now(timezone.utc)
    area = int((meta.get("params") or {}).get("area_id", AREA_AMSTERDAM))
    flt = {"areas": {"eq": area}, "listingDate": {"gte": now.strftime("%Y-%m-%dT00:00:00.000Z"), "lte": (now + timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%dT00:00:00.000Z")}}
    rows: list[dict] = []
    for page in range(1, int((meta.get("params") or {}).get("max_pages", MAX_PAGES)) + 1):
        status, body = http.post_json(ENDPOINT, {"operationName": "Listing", "query": QUERY, "variables": {"f": flt, "n": PAGE_SIZE, "p": page}},
                                      headers={"Referer": "https://ra.co/events/nl/amsterdam", "Accept": "application/json"})
        if status != 200 or not isinstance(body, dict) or "data" not in body or not body["data"]:
            raise RuntimeError(f"ra.co graphql HTTP {status}: {str(body)[:120]}")
        listing = body["data"]["eventListings"]
        chunk = listing.get("data") or []
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE or len(rows) >= int(listing.get("totalResults") or 0):
            break
    return rows
