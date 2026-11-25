"""Shared plumbing for primary-source scrapers (release 3.8, Train C). Stdlib only, no network at import time.

A primary source is an official or first-party data feed (a public API or JSON the site itself uses). Each one lives in
primary_sources/<slug>/ with client.py (network), mapper.py (pure: raw -> webhook events), meta.json (policy + switches),
fixtures/ and test_offline.py. The runner (run.py) is the only place that posts to Outlist.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

UA = "Mozilla/5.0 (compatible; OutlistBot/1.0; +https://app.outlist.org)"
MAX_BODY_BYTES = 8_000_000
SOURCE_KIND = "primary"
EVENT_CAP_DEFAULT = 300


# ─── time: Europe/Amsterdam without a tz database (Windows has none by default) ───────────────────────────────────────
def _last_sunday(year: int, month: int) -> datetime:
    d = datetime(year, month, 31, 1, 0, tzinfo=timezone.utc)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def amsterdam_offset(utc: datetime) -> timedelta:
    """CET (+1) / CEST (+2): EU rule, DST from the last Sunday of March 01:00 UTC to the last Sunday of October 01:00 UTC."""
    u = utc.astimezone(timezone.utc)
    return timedelta(hours=2) if _last_sunday(u.year, 3) <= u < _last_sunday(u.year, 10) else timedelta(hours=1)


def parse_iso(value: str | None) -> datetime | None:
    """ISO 8601 with Z/offset -> aware datetime. A naive value is taken as Amsterdam wall-clock. Anything else -> None."""
    if not value or not isinstance(value, str):
        return None
    s = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        guess = dt.replace(tzinfo=timezone.utc) - timedelta(hours=1)
        dt = dt.replace(tzinfo=timezone(amsterdam_offset(guess)))
    return dt


def amsterdam_parts(dt: datetime) -> tuple[str, str]:
    """(YYYY-MM-DD, HH:MM) wall-clock in Amsterdam for an aware datetime."""
    local = dt.astimezone(timezone.utc) + amsterdam_offset(dt)
    return local.strftime("%Y-%m-%d"), local.strftime("%H:%M")


# ─── text ────────────────────────────────────────────────────────────────────────────────────────────────────────────
def clean(value: Any, limit: int = 2000) -> str:
    text = str(value or "").encode("utf-8", "ignore").decode("utf-8")  # drops lone surrogates that would break json.dumps(...).encode()
    return re.sub(r"\s+", " ", text).strip()[:limit]


def https_url(value: Any) -> str | None:
    u = clean(value, 1000)
    return u if u.lower().startswith("https://") else None


# ─── http ────────────────────────────────────────────────────────────────────────────────────────────────────────────
class RobotsDisallowed(RuntimeError):
    """The target URL is disallowed for OutlistBot by the origin's robots.txt."""


class Http:
    """Polite JSON client: minimum interval between calls, bounded retries with Retry-After, size cap, injectable transport."""

    def __init__(self, min_interval_s: float = 1.5, retries: int = 2, timeout: int = 30, transport: Callable | None = None,
                 sleep: Callable[[float], None] = time.sleep, max_requests: int = 40, enforce_robots: bool = False):
        self.min_interval_s, self.retries, self.timeout = min_interval_s, retries, timeout
        self.transport, self._sleep, self.max_requests = transport or self._urllib, sleep, max_requests
        self.calls = 0
        self._last = 0.0
        self.enforce_robots = enforce_robots
        self._robots: dict[str, str] = {}

    @staticmethod
    def _urllib(method: str, url: str, headers: dict, body: bytes | None, timeout: int) -> tuple[int, dict, bytes]:
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read(MAX_BODY_BYTES + 1)
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read(MAX_BODY_BYTES + 1)

    def _check_robots(self, url: str) -> None:
        """Opt-in robots.txt gate: fetched once per origin through the same transport; any non-200 or error -> allow (fail-open)."""
        p = urllib.parse.urlsplit(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in self._robots:
            self._robots[origin] = ""
            try:
                status, _rh, raw = self._send_raw("GET", f"{origin}/robots.txt", {"User-Agent": UA}, None)
                if status == 200:
                    self._robots[origin] = raw.decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001 - robots unreachable: fail open
                pass
        if self._robots[origin] and not robots_allows(self._robots[origin], url, "OutlistBot"):
            raise RobotsDisallowed(f"robots.txt of {origin} disallows {url}")

    def _send(self, method: str, url: str, hdrs: dict, data: bytes | None) -> tuple[int, dict, bytes]:
        if self.enforce_robots:
            self._check_robots(url)
        return self._send_raw(method, url, hdrs, data)

    def _send_raw(self, method: str, url: str, hdrs: dict, data: bytes | None) -> tuple[int, dict, bytes]:
        if self.calls >= self.max_requests:
            raise RuntimeError(f"request budget exhausted ({self.max_requests})")
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            wait = self.min_interval_s - (time.monotonic() - self._last)
            if wait > 0 and self.calls:
                self._sleep(wait)
            self._last = time.monotonic()
            self.calls += 1
            try:
                status, rh, raw = self.transport(method, url, hdrs, data, self.timeout)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last = exc
                self._sleep(2 ** attempt)
                continue
            if len(raw) > MAX_BODY_BYTES:
                raise RuntimeError("response too large")
            if status in (429, 500, 502, 503, 504) and attempt < self.retries:
                ra = next((v for k, v in rh.items() if k.lower() == "retry-after"), "")
                self._sleep(min(float(ra), 30.0) if str(ra).isdigit() else 2 ** (attempt + 1))
                last = RuntimeError(f"HTTP {status}")
                continue
            return status, rh, raw
        raise RuntimeError(f"request failed after retries: {last}")

    def request(self, method: str, url: str, *, headers: dict | None = None, json_body: Any = None) -> tuple[int, Any]:
        data = json.dumps(json_body).encode("utf-8") if json_body is not None else None
        hdrs = {"User-Agent": UA, "Accept": "application/json", **({"Content-Type": "application/json"} if data else {}), **(headers or {})}
        status, _rh, raw = self._send(method, url, hdrs, data)
        try:
            return status, (json.loads(raw.decode("utf-8", errors="replace")) if raw else None)
        except ValueError:
            return status, None

    def raw_get(self, url: str, headers: dict | None = None) -> tuple[int, dict, bytes]:
        """GET without JSON parsing (HTML pages): same pacing, budget, retries and size cap."""
        return self._send("GET", url, {"User-Agent": UA, **(headers or {})}, None)

    def get_json(self, url: str, **kw) -> tuple[int, Any]:
        return self.request("GET", url, **kw)

    def post_json(self, url: str, body: Any, **kw) -> tuple[int, Any]:
        return self.request("POST", url, json_body=body, **kw)


def robots_allows(robots_txt: str, url: str, agent: str = "OutlistBot") -> bool:
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_txt.splitlines())
    return rp.can_fetch(agent, url)


# ─── events ──────────────────────────────────────────────────────────────────────────────────────────────────────────
REQUIRED = ("source", "title", "raw_date_string", "external_url", "source_kind")


def make_event(source: str, *, title: str, start: datetime, end: datetime | None = None, venue: str = "", address: str = "",
               url: str = "", image: str | None = None, description: str = "", price: str = "", organizer: str = "",
               category: str = "", lat: float | None = None, lng: float | None = None, upstream_id: str = "") -> dict:
    """The single constructor of webhook events for primary sources: same fields the Layer-2 crawler posts, plus source_kind='primary'.
    The ISO start/end go along as JSON-LD so the webhook's extractor reads exact times instead of guessing from text."""
    d, t = amsterdam_parts(start)
    ev: dict[str, Any] = {
        "source": source, "source_kind": SOURCE_KIND, "title": clean(title, 200), "raw_date_string": d, "raw_time_string": t,
        "venue_name": clean(venue, 200), "address": clean(address, 300), "external_url": https_url(url) or "",
        "description": clean(description, 3000), "category_hint": category, "organizer_name": clean(organizer, 200),
        "jsonld": {"@type": "Event", "name": clean(title, 200), "startDate": start.isoformat(), **({"endDate": end.isoformat()} if end else {})},
    }
    if end:
        ed, et = amsterdam_parts(end)
        ev["end_date_string"] = ed if ed != d else f"{ed} {et}"
    if image and https_url(image):
        ev["image_url"] = https_url(image)
    if price:
        ev["price_text"] = clean(price, 80)
    if lat is not None and lng is not None and -90 <= lat <= 90 and -180 <= lng <= 180 and not (lat == 0 and lng == 0):
        ev["lat"], ev["lng"] = round(float(lat), 6), round(float(lng), 6)
    if upstream_id:
        ev["upstream_id"] = clean(upstream_id, 80)
    return ev


def validate_events(events: list[dict], *, source_prefix: str, now: datetime | None = None) -> list[str]:
    """Problems found in a mapped batch (empty list = good). Used by --validate, the offline tests and the live probe."""
    now = now or datetime.now(timezone.utc)
    problems: list[str] = []
    seen: set[str] = set()
    for i, e in enumerate(events):
        tag = f"#{i} {str(e.get('title'))[:40]!r}"
        for k in REQUIRED:
            if not e.get(k):
                problems.append(f"{tag}: missing {k}")
        if e.get("source_kind") != SOURCE_KIND:
            problems.append(f"{tag}: source_kind must be 'primary'")
        if not str(e.get("source", "")).startswith(source_prefix):
            problems.append(f"{tag}: source must start with {source_prefix!r}")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(e.get("raw_date_string", ""))):
            problems.append(f"{tag}: raw_date_string not YYYY-MM-DD")
        if not re.fullmatch(r"\d{2}:\d{2}", str(e.get("raw_time_string", ""))):
            problems.append(f"{tag}: raw_time_string not HH:MM")
        if not str(e.get("external_url", "")).startswith("https://"):
            problems.append(f"{tag}: external_url must be https")
        key = e.get("external_url", "")
        if key in seen:
            problems.append(f"{tag}: duplicate external_url")
        seen.add(key)
    return problems


def dedupe(events: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out = []
    for e in events:
        k = e.get("external_url") or f"{e.get('title')}|{e.get('raw_date_string')}"
        if k not in seen:
            seen.add(k)
            out.append(e)
    return out


def future_only(events: list[dict], now: datetime | None = None, grace_hours: int = 6) -> list[dict]:
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(hours=grace_hours)
    out = []
    for e in events:
        dt = parse_iso((e.get("jsonld") or {}).get("startDate"))
        if dt is None or dt >= cutoff:
            out.append(e)
    return out


def read_text(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def log(msg: str) -> None:
    print(msg, file=sys.stderr if msg.startswith("::") else sys.stdout, flush=True)


def env_off(slug: str) -> bool:
    """Per-source off switch from the environment: OUTLIST_PRIMARY_<SLUG>=off|false|0 (the workflow exposes it as a repo variable)."""
    v = os.environ.get(f"OUTLIST_PRIMARY_{slug.upper()}", "").strip().lower()
    return v in ("off", "false", "0", "no", "disabled")
