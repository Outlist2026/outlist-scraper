#!/usr/bin/env python3
"""Outlist Instagram scraper (S5): fetch the newest posts of the pages an admin added, let Outlist classify them, post the events.

Runs on GitHub Actions of the PUBLIC runner repository (free minutes) and, with the schedule off, in the private repository.
Standard library only: no browser, no login, no Instagram credentials. The posts come from Apify (adapter below); the decision
"is this post an event?" is made by Outlist's local model inside the ig-gateway edge function, never by an LLM and never here.

Flow
  1. ig-gateway `automation`   -> stop when IG scraping or the global automation switch is off.
  2. ig-gateway `pages`        -> active handles (added in the admin page).
  3. Adapter.fetch(handles)    -> posts (Apify `apify/instagram-scraper`, hard caps on pages and results).
  4. ig-gateway `classify`     -> RawEvent[] for posts that look like events (and stores the rest as seen).
  5. scraping-webhook          -> same contract and same approval queue as every other scraper (status in_review, never auto-published).
  6. ig-gateway `confirm`      -> only after the webhook accepted the events, so a failed POST is retried next run.
  7. ig-gateway `page_result`  -> per page outcome for the admin page.

Environment (names only, values live in repository secrets / the owner key file):
  APIFY_API_KEYS           Apify tokens, comma separated (free accounts in rotation: a quota answer moves to the next key)
  APIFY_API_KEY            single Apify token, used when APIFY_API_KEYS is not set
  SCRAPER_WEBHOOK_SECRET   same secret as scraper-gateway / scraping-webhook
  SUPABASE_URL             https://<project-ref>.supabase.co  (or IG_GATEWAY_URL + SCRAPING_WEBHOOK_URL)
  OUTLIST_DRY_RUN=true     fetch and classify, post nothing, store nothing (still spends Apify credit)
  OUTLIST_FORCE=true       run even if automation is off
  OUTLIST_HANDLES=a,b      only these pages (comma separated, with or without @); empty = all active pages. Use with OUTLIST_DRY_RUN to test a page
  IG_MAX_PAGES=150  IG_POSTS_PER_PAGE=8  IG_MAX_RESULTS=1200  IG_FIRST_RUN_DAYS=14
  IG_SOURCE=apify|file     `file` reads IG_FIXTURE (a saved Apify dataset) instead of calling Apify: offline tests

Offline self-check (no network, no key): python scripts/scrapers/instagram_scraper.py --validate
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURE_PATH = os.path.join(BASE_DIR, "fixtures", "apify_instagram_sample.json")
UA = "outlist-ig-scraper/1.0 (+https://outlist.org)"
APIFY_ACTOR = "apify~instagram-scraper"
APIFY_BASE = "https://api.apify.com/v2"
BATCH = 100


# ─── HTTP helpers ────────────────────────────────────────────────────────────

def http_json(url: str, payload=None, method: str = "POST", headers: dict | None = None, timeout: int = 90):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    hdr = {"Content-Type": "application/json", "User-Agent": UA, **(headers or {})}
    req = urllib.request.Request(url, data=data, method=method, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, {"error": raw[:300]}


class _KeyProblem(Exception):
    def __init__(self, why: str):
        super().__init__(why)
        self.why = why


class Gateway:
    def __init__(self, url: str, secret: str):
        self.url, self.secret = url, secret

    def call(self, action: str, **body):
        status, out = http_json(self.url, {"action": action, **body}, headers={"X-Webhook-Secret": self.secret}, timeout=120)
        if status != 200:
            raise RuntimeError(f"ig-gateway {action} returned HTTP {status}: {str(out)[:200]}")
        return out


# ─── Adapters: the only place that knows which vendor delivers the posts ─────

def normalize_apify_item(it: dict) -> dict | None:
    """Apify instagram-scraper item -> the IgPost shape the gateway expects. Returns None for items without a shortcode."""
    shortcode = it.get("shortCode") or it.get("shortcode")
    handle = (it.get("ownerUsername") or "").lower()
    if not shortcode or not handle:
        return None
    t = str(it.get("type") or "").lower()
    kind = "carousel" if t == "sidecar" else "video" if t == "video" else "image" if t == "image" else "unknown"
    ts = it.get("timestamp")
    return {
        "shortcode": shortcode,
        "handle": handle,
        "caption": (it.get("caption") or "")[:5000],
        "takenAt": ts if isinstance(ts, str) else None,
        "type": kind,
        "width": it.get("dimensionsWidth"),
        "height": it.get("dimensionsHeight"),
        "imageUrl": it.get("displayUrl"),
        "url": it.get("url") or f"https://www.instagram.com/p/{shortcode}/",
        "likes": it.get("likesCount") if isinstance(it.get("likesCount"), int) else None,
        "comments": it.get("commentsCount") if isinstance(it.get("commentsCount"), int) else None,
    }


# ─── Several Apify keys on the free plan (S7, BUG-F20261110-45) ──────────────
# Twin of supabase/functions/_shared/apify/keys.ts: keep the two in sync (same case table in tests/apify/keys.test.ts).
import re

_LIMIT_WORDS = re.compile(r"(usage|limit|credit|quota|exceed|payment|insufficient)", re.I)


class AllKeysExhausted(RuntimeError):
    pass


def parse_apify_keys(environ) -> list[str]:
    """APIFY_API_KEYS (comma separated) first, then the single APIFY_API_KEY. Trims, drops empty and duplicate entries."""
    for name in ("APIFY_API_KEYS", "APIFY_API_KEY"):
        keys: list[str] = []
        for k in (environ.get(name, "") or "").split(","):
            k = k.strip()
            if k and k not in keys:
                keys.append(k)
        if keys:
            return keys
    return []


def classify_apify_failure(status: int, body: str) -> str | None:
    """'quota' / 'invalid' = switch to the next key; None = not a key problem."""
    if status == 401:
        return "invalid"
    if status in (402, 429):
        return "quota"
    if status == 403 and _LIMIT_WORDS.search(body or ""):
        return "quota"
    return None


class KeyPool:
    """Next usable key from a rotating start; an exhausted key is remembered for this run only. Key values are never printed: only #index."""

    def __init__(self, keys: list[str], start: int = 0):
        self.keys = list(keys)
        self.start = start % len(self.keys) if self.keys else 0
        self.state: list[str | None] = [None] * len(self.keys)

    def next(self) -> int | None:
        for i in range(len(self.keys)):
            idx = (self.start + i) % len(self.keys)
            if not self.state[idx]:
                return idx
        return None

    def mark(self, index: int, why: str) -> None:
        self.state[index] = why

    def report(self) -> str:
        parts = [f"#{i + 1} {s}" for i, s in enumerate(self.state) if s]
        return ", ".join(parts) if parts else "none used"

    def scrub(self, text: str) -> str:
        for i, k in enumerate(self.keys):
            if k:
                text = text.replace(k, f"#{i + 1}")
        return text


class ApifyAdapter:
    """apify/instagram-scraper, asynchronous run (a 40-page run can exceed the 300 s limit of the synchronous endpoint).
    Takes a KeyPool: a quota answer on the start call, or a run that ends FAILED/ABORTED with a usage-limit message, moves to the next key."""

    def __init__(self, pool: "KeyPool | str", posts_per_page: int, newer_than: str | None, max_results: int, http=None, sleep=time.sleep):
        self.pool = pool if isinstance(pool, KeyPool) else KeyPool([pool])
        self.per_page, self.newer_than, self.max_results = posts_per_page, newer_than, max_results
        self.http = http or http_json
        self.sleep = sleep

    def fetch(self, handles: list[str]) -> list[dict]:
        while True:
            idx = self.pool.next()
            if idx is None:
                raise AllKeysExhausted(f"All Apify keys are unusable for this run ({self.pool.report()})")
            try:
                return self._fetch_with(self.pool.keys[idx], handles)
            except _KeyProblem as problem:
                self.pool.mark(idx, problem.why)

    def _fetch_with(self, token: str, handles: list[str]) -> list[dict]:
        http = self.http
        run_input = {
            "directUrls": [f"https://www.instagram.com/{h}/" for h in handles],
            "resultsType": "posts",
            "resultsLimit": self.per_page,
            "addParentData": False,
        }
        if self.newer_than:
            run_input["onlyPostsNewerThan"] = self.newer_than
        # hard spend cap: Apify stops the run once this many results are charged
        q = urllib.parse.urlencode({"token": token, "maxItems": self.max_results})
        status, run = http(f"{APIFY_BASE}/acts/{APIFY_ACTOR}/runs?{q}", run_input)
        why = classify_apify_failure(status, json.dumps(run) if run is not None else "")
        if why:
            raise _KeyProblem(why)
        if status not in (200, 201) or not run or "data" not in run:
            raise RuntimeError(f"Apify start failed: HTTP {status}")
        run_id, dataset_id = run["data"]["id"], run["data"]["defaultDatasetId"]
        deadline = time.time() + 20 * 60
        while time.time() < deadline:
            self.sleep(10)
            s, info = http(f"{APIFY_BASE}/actor-runs/{run_id}?token={token}", method="GET")
            data = (info or {}).get("data", {})
            st = data.get("status")
            if st in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        else:
            http(f"{APIFY_BASE}/actor-runs/{run_id}/abort?token={token}", {}, timeout=30)
            raise RuntimeError("Apify run did not finish in 20 minutes, aborted")
        if st != "SUCCEEDED":
            if st in ("FAILED", "ABORTED") and _LIMIT_WORDS.search(str(data.get("statusMessage") or "")):
                raise _KeyProblem("quota")
            raise RuntimeError(f"Apify run ended with status {st}")
        s, items = http(f"{APIFY_BASE}/datasets/{dataset_id}/items?token={token}&clean=true&limit={self.max_results}", method="GET", timeout=120)
        if s != 200 or not isinstance(items, list):
            raise RuntimeError(f"Apify dataset read failed: HTTP {s}")
        return [p for p in (normalize_apify_item(i) for i in items) if p]


class FileAdapter:
    """Offline: a saved Apify dataset. Used by --validate and by tests."""

    def __init__(self, path: str):
        self.path = path

    def fetch(self, handles: list[str]) -> list[dict]:
        with open(self.path, encoding="utf-8") as fh:
            items = json.load(fh)
        wanted = set(handles)
        return [p for p in (normalize_apify_item(i) for i in items) if p and (not wanted or p["handle"] in wanted)]


# ─── Core flow (adapter and gateway injected so tests can fake both) ─────────

ZERO_ALERT_AFTER = int(os.environ.get("OUTLIST_ZERO_ALERT_AFTER", "3"))


def write_step_summary(message: str) -> None:
    """Append a line to the GitHub run summary when running on Actions."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"### Instagram scraper\n\n{message}\n")
    except OSError:
        pass


DEFAULT_MAX_PAGES = 150
DEFAULT_MAX_RESULTS = 1200


def parse_handles(raw: str | None) -> list[str]:
    """OUTLIST_HANDLES -> lowercase handles without @, deduplicated, in order, empty entries dropped."""
    out: list[str] = []
    for part in (raw or "").split(","):
        h = part.strip().lstrip("@").strip().lower()
        if h and h not in out:
            out.append(h)
    return out


def exit_code(result: dict) -> int:
    return 1 if result.get("fetch_failed") or result.get("all_unknown") else 0


def run_flow(gw, adapter, post_events, *, max_pages: int, dry_run: bool, force: bool, log=print, only_handles: list[str] | None = None) -> dict:
    auto = gw.call("automation")
    if not auto.get("enabled") and not force:
        log("IG scraping is switched off (feature flag or automation): nothing to do")
        return {"skipped": True}
    pages = gw.call("pages").get("pages", [])
    unknown: list[str] = []
    if only_handles:  # targeted run: only the chosen active pages (BUG-F20261119-1)
        active = {str(p["handle"]).lower() for p in pages}
        unknown = [h for h in only_handles if h not in active]
        for h in unknown:
            log(f"not an active page: {h}")
        if unknown and len(unknown) == len(only_handles):
            msg = f"none of the requested handles is an active page: {', '.join(unknown)}"
            log(f"::error::{msg}")
            write_step_summary(msg)
            return {"pages": 0, "unknown_handles": unknown, "all_unknown": True}
        if unknown:
            msg = f"not an active page: {', '.join(unknown)}"
            log(f"::warning::{msg}")
            write_step_summary(msg)
        pages = [p for p in pages if str(p["handle"]).lower() in only_handles]
    pages = pages[:max_pages]
    if not pages:
        log("No active Instagram pages")
        return {"pages": 0, "unknown_handles": unknown} if only_handles else {"pages": 0}
    handles = [p["handle"] for p in pages]
    summary = {"pages": len(handles), "posts": 0, "events": 0, "posted": 0, "inserted": 0, "duplicates": 0, "filtered": 0, "errors": 0}
    if unknown:
        summary["unknown_handles"] = unknown
    answered = [False]  # True once the webhook told us what it really stored (BUG-F20261117-2)
    started = time.time()

    def finish(error: str | None = None) -> dict:
        """Write the run into the admin Scraping log (ig-gateway run_log -> scraping_runs) and raise the zero-result warning."""
        if dry_run:
            return summary
        pool = getattr(adapter, "pool", None)
        keys_note = f", keys: {pool.report()}" if pool is not None and len(pool.keys) > 1 else ""
        stored = f", inserted {summary['inserted']}, duplicates {summary['duplicates']}, filtered {summary['filtered']}" if answered[0] else ""
        notes = f"pages {summary['pages']}, posts {summary['posts']}, events {summary['events']}, posted {summary['posted']}{stored}{keys_note}"
        if error:
            notes = f"FAILED: {error[:200]} | {notes}"
        try:
            res = gw.call("run_log", events_inserted=summary["inserted"] if answered[0] else summary["posted"], events_skipped=max(0, summary["posts"] - summary["events"]),
                          events_errored=summary["errors"], duration_seconds=int(time.time() - started), notes=notes[:500])
        except Exception as exc:  # the log must never hide the run itself
            log(f"Could not write the run log: {exc}")
            return summary
        zero = int((res or {}).get("consecutive_zero") or 0)
        summary["consecutive_zero"] = zero
        if zero >= ZERO_ALERT_AFTER:
            msg = f"Instagram scraper found 0 events in the last {zero} runs"
            log(f"::warning::{msg}")
            write_step_summary(msg)
        return summary

    try:
        posts = adapter.fetch(handles)
    except Exception as exc:  # whole fetch failed: every page gets the error, nothing is posted
        log(f"Fetch failed: {exc}")
        for h in handles:
            if not dry_run:
                gw.call("page_result", handle=h, status="error", error=str(exc)[:400], events_found=0)
        summary["errors"] = len(handles)
        summary["fetch_failed"] = True
        return finish(str(exc))
    summary["posts"] = len(posts)
    per_handle: dict[str, dict] = {h: {"events": 0, "newest": None, "error": None} for h in handles}
    for i in range(0, len(posts), BATCH):
        batch = posts[i:i + BATCH]
        out = gw.call("classify", posts=batch, dry_run=dry_run)
        events, pending = out.get("events", []), out.get("pending_confirm", [])
        summary["events"] += len(events)
        for h, v in (out.get("per_handle") or {}).items():
            if h in per_handle:
                per_handle[h]["events"] += v.get("events", 0)
                if v.get("newest") and (not per_handle[h]["newest"] or v["newest"] > per_handle[h]["newest"]):
                    per_handle[h]["newest"] = v["newest"]
        if dry_run:
            log(f"[dry run] batch {i // BATCH + 1}: {len(batch)} posts -> {len(events)} events, counts {out.get('counts')}")
            for e in (events if only_handles else events[:5]):
                log(f"  - {e.get('title')} | date={e.get('raw_date_string')} time={e.get('raw_time_string')} venue={e.get('venue_name')} priority={e.get('priority')}")
            continue
        if events:
            status, body = post_events(events)
            if status == 200:
                summary["posted"] += len(events)
                if isinstance(body, dict) and "inserted" in body:
                    # "posted" only means sent: the webhook drops past events, spam, bad titles and merges duplicates, and says so in its answer
                    answered[0] = True
                    summary["inserted"] += int(body.get("inserted") or 0)
                    summary["duplicates"] += int(body.get("skipped_duplicates") or 0)
                    summary["filtered"] += int(body.get("filtered") or 0)
                    log(f"Webhook stored {body.get('inserted', 0)} of {len(events)} events (duplicates {body.get('skipped_duplicates', 0)}, filtered {body.get('filtered', 0)})")
                    for reason in (body.get("filter_log") or [])[:20]:
                        log(f"  dropped: {reason}")
                gw.call("confirm", posts=pending)
            else:
                summary["errors"] += 1
                log(f"Webhook returned HTTP {status}; events stay unconfirmed and are retried next run: {str(body)[:200]}")
                for e in events:
                    h = str(e.get("source", "")).split(":", 1)[-1]
                    if h in per_handle:
                        per_handle[h]["error"] = f"webhook HTTP {status}"
    if not dry_run:
        for h, v in per_handle.items():
            gw.call("page_result", handle=h, status="error" if v["error"] else "success", error=v["error"],
                    events_found=v["events"], newest_post_ts=v["newest"])
    finish()
    log(f"Done: {summary}")
    return summary


# ─── Entry points ────────────────────────────────────────────────────────────

def _validate_keys() -> None:
    """Apify key rotation with a fake HTTP layer (same cases as tests/apify/keys.test.ts)."""
    A, B = "apify_api_SENTINELaaaaaaaaaaaaaaaa", "apify_api_SENTINELbbbbbbbbbbbbbbbb"
    assert parse_apify_keys({"APIFY_API_KEYS": " k1 , k2,,k1 ,k3"}) == ["k1", "k2", "k3"], "parse list"
    assert parse_apify_keys({"APIFY_API_KEY": "solo"}) == ["solo"] and parse_apify_keys({"APIFY_API_KEYS": " , ", "APIFY_API_KEY": "solo"}) == ["solo"], "parse fallback"
    assert parse_apify_keys({}) == [], "parse none"
    assert classify_apify_failure(402, "") == "quota" and classify_apify_failure(429, "") == "quota"
    assert classify_apify_failure(403, "Monthly usage hard limit exceeded") == "quota" and classify_apify_failure(401, "") == "invalid"
    assert classify_apify_failure(403, "forbidden") is None and classify_apify_failure(500, "usage limit") is None and classify_apify_failure(200, "") is None

    def make_http(script: dict, calls: list):
        """script maps a token to the status of its start call; runs succeed at once with one item."""
        def http(url, payload=None, method="POST", headers=None, timeout=90):
            tok = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("token", [""])[0]
            calls.append((method, url.split("?")[0].rsplit("/", 1)[-1], tok))
            if url.endswith(f"{APIFY_ACTOR}/runs") or "/runs?" in url:
                st = script.get(tok, 200)
                return (st, {"data": {"id": "r1", "defaultDatasetId": "d1"}} if st == 200 else {"error": {"message": script.get("msg", "")}})
            if "/actor-runs/" in url:
                return 200, {"data": {"status": script.get("run:" + tok, "SUCCEEDED"), "statusMessage": script.get("runmsg", "")}}
            return 200, [{"shortCode": "AAAAA1", "ownerUsername": "esn", "caption": "x", "timestamp": "2026-11-10T10:00:00.000Z", "type": "Image"}]
        return http

    calls: list = []
    ad = ApifyAdapter(KeyPool(["k1", "k2"]), 8, None, 10, http=make_http({"k1": 402}, calls), sleep=lambda *_: None)
    assert len(ad.fetch(["esn"])) == 1 and calls[0][2] == "k1" and any(c[2] == "k2" for c in calls), "402 on key 1 then key 2"
    assert sum(1 for c in calls if c[2] == "k1") == 1, "key 1 tried once"
    assert ad.pool.report() == "#1 quota", ad.pool.report()
    calls.clear()
    ad = ApifyAdapter(KeyPool(["k1", "k2"]), 8, None, 10, http=make_http({"run:k1": "FAILED", "runmsg": "Monthly usage hard limit exceeded"}, calls), sleep=lambda *_: None)
    assert len(ad.fetch(["esn"])) == 1 and ad.pool.report() == "#1 quota", "run FAILED with a limit message switches"
    calls.clear()
    ad = ApifyAdapter(KeyPool(["bad", "good"]), 8, None, 10, http=make_http({"bad": 401}, calls), sleep=lambda *_: None)
    ad.fetch(["esn"]); assert ad.pool.report() == "#1 invalid"
    calls.clear()
    ad = ApifyAdapter(KeyPool(["k1", "k2"]), 8, None, 10, http=make_http({"k1": 500}, calls), sleep=lambda *_: None)
    try:
        ad.fetch(["esn"]); raise AssertionError("500 must raise")
    except RuntimeError as exc:
        assert "HTTP 500" in str(exc) and len(calls) == 1, "500 does not switch"
    ad = ApifyAdapter(KeyPool([A, B]), 8, None, 10, http=make_http({A: 402, B: 429}, []), sleep=lambda *_: None)
    try:
        ad.fetch(["esn"]); raise AssertionError("all exhausted must raise")
    except AllKeysExhausted as exc:
        assert "#1 quota, #2 quota" in str(exc) and "SENTINEL" not in str(exc), "indexes only"
    assert "SENTINEL" not in KeyPool([A, B]).scrub(f"x?token={A}&y={B}"), "scrub"
    p = KeyPool(["k1", "k2", "k3"], 4)
    assert p.next() == 1, "rotating start"
    a2 = KeyPool(["k1", "k2"]); a2.mark(0, "quota"); assert KeyPool(["k1", "k2"]).next() == 0 and a2.next() == 1, "a new pool tries the key again"


def validate() -> int:
    """Offline self-check on the saved Apify dataset: normalisation and the fake end-to-end flow."""
    items = json.load(open(FIXTURE_PATH, encoding="utf-8"))
    posts = FileAdapter(FIXTURE_PATH).fetch([])
    assert len(posts) == 4, f"expected 4 normalised posts, got {len(posts)}"
    assert {p["type"] for p in posts} == {"image", "carousel", "video"}, "types"
    assert all(p["url"].startswith("https://www.instagram.com/") for p in posts)
    assert normalize_apify_item({"ownerUsername": "x"}) is None, "item without shortcode"
    assert normalize_apify_item(items[0])["handle"] == items[0]["ownerUsername"].lower()

    calls: list = []

    class FakeGw:
        def call(self, action, **body):
            calls.append((action, body))
            if action == "automation":
                return {"enabled": True}
            if action == "pages":
                return {"pages": [{"handle": "esn_amsterdam", "last_post_ts": None}]}
            if action == "classify":
                ev = [{"title": "T", "source": "instagram:esn_amsterdam", "priority": 70}]
                return {"events": ev, "pending_confirm": [{"shortcode": "AAAAA1", "handle": "esn_amsterdam"}], "counts": {},
                        "per_handle": {"esn_amsterdam": {"events": 1, "posts": 4, "newest": "2026-11-10T10:00:00.000Z"}}}
            return {"ok": True}

    posted: list = []
    s = run_flow(FakeGw(), FileAdapter(FIXTURE_PATH), lambda ev: (posted.append(ev) or (200, "ok")), max_pages=10, dry_run=False, force=False, log=lambda *_: None)
    assert s["posted"] == 1 and posted, "event posted"
    assert [c[0] for c in calls] == ["automation", "pages", "classify", "confirm", "page_result", "run_log"], [c[0] for c in calls]
    calls.clear(); posted.clear()
    run_flow(FakeGw(), FileAdapter(FIXTURE_PATH), lambda ev: (500, "boom"), max_pages=10, dry_run=False, force=False, log=lambda *_: None)
    assert "confirm" not in [c[0] for c in calls], "no confirm after a failed webhook"
    calls.clear()
    run_flow(FakeGw(), FileAdapter(FIXTURE_PATH), lambda ev: (posted.append(ev) or (200, "ok")), max_pages=10, dry_run=True, force=False, log=lambda *_: None)
    assert not posted and "confirm" not in [c[0] for c in calls] and "page_result" not in [c[0] for c in calls], "dry run posts and stores nothing"
    _validate_keys()
    # run log (BUG-F20261110-44): normal run ends with run_log after page_result, a failed fetch is logged too, a dry run logs nothing
    calls.clear(); posted.clear()
    class ZeroGw(FakeGw):
        def call(self, action, **body):
            if action == "run_log":
                calls.append((action, body))
                return {"ok": True, "consecutive_zero": 3}
            return super().call(action, **body)
    lines: list = []
    s = run_flow(ZeroGw(), FileAdapter(FIXTURE_PATH), lambda ev: (posted.append(ev) or (200, "ok")), max_pages=10, dry_run=False, force=False, log=lines.append)
    names = [c[0] for c in calls]
    assert names[-2:] == ["page_result", "run_log"], names
    rl = [c[1] for c in calls if c[0] == "run_log"][0]
    assert rl["events_inserted"] == 1 and rl["notes"].startswith("pages 1"), rl
    assert any(str(l).startswith("::warning::Instagram scraper found 0 events in the last 3 runs") for l in lines), "zero alert text"
    assert not any("SENTINEL" in str(l) for l in lines)
    calls.clear()
    class BoomAdapter:
        pool = KeyPool(["a", "b"])
        def fetch(self, handles):
            raise AllKeysExhausted("All Apify keys are unusable for this run (#1 quota, #2 quota)")
    s = run_flow(FakeGw(), BoomAdapter(), lambda ev: (200, "ok"), max_pages=10, dry_run=False, force=False, log=lambda *_: None)
    assert s.get("fetch_failed") and "run_log" in [c[0] for c in calls], "failed fetch is logged and flagged"
    assert [c[1] for c in calls if c[0] == "run_log"][0]["notes"].startswith("FAILED: All Apify keys"), "failure note"
    calls.clear()
    run_flow(FakeGw(), FileAdapter(FIXTURE_PATH), lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lambda *_: None)
    assert "run_log" not in [c[0] for c in calls], "dry run writes no run log"
    # BUG-F20261117-2: "posted" means sent. What the webhook really stored (inserted), merged as duplicate or dropped (filtered, with the reason) must be reported
    calls.clear(); posted.clear()
    lines = []
    wh = {"inserted": 0, "skipped_duplicates": 0, "filtered": 1, "filter_log": ['PAST_EVENT: "T" - 2026-11-01']}
    s = run_flow(FakeGw(), FileAdapter(FIXTURE_PATH), lambda ev: (posted.append(ev) or (200, wh)), max_pages=10, dry_run=False, force=False, log=lines.append)
    assert s["posted"] == 1 and s["inserted"] == 0 and s["filtered"] == 1, s
    rl = [c[1] for c in calls if c[0] == "run_log"][0]
    assert rl["events_inserted"] == 0, "the run log counts stored events, not sent ones: " + str(rl)
    assert "filtered 1" in rl["notes"] and "inserted 0" in rl["notes"], rl["notes"]
    assert any("PAST_EVENT" in str(l) for l in lines), "the drop reason is printed in the run log"
    # BUG-F20261119-1: parse_handles and the targeted run (only_handles)
    assert parse_handles(" @IsaA.UvA, esn ,,esn") == ["isaa.uva", "esn"], parse_handles(" @IsaA.UvA, esn ,,esn")
    assert parse_handles("") == [] and parse_handles(None) == [], "blank handles"
    assert DEFAULT_MAX_PAGES == 150 and DEFAULT_MAX_RESULTS == 1200, "defaults are 150 pages and 1200 results"

    class MultiGw(FakeGw):
        def call(self, action, **body):
            if action == "pages":
                calls.append((action, body))
                return {"pages": [{"handle": "isaa.uva", "last_post_ts": None}, {"handle": "esn", "last_post_ts": None}, {"handle": "other", "last_post_ts": None}]}
            return super().call(action, **body)

    class RecAdapter:
        def __init__(self):
            self.fetched: list = []
        def fetch(self, handles):
            self.fetched.append(list(handles))
            return FileAdapter(FIXTURE_PATH).fetch([])

    calls.clear()
    rec = RecAdapter()
    s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lambda *_: None, only_handles=["esn"])
    assert rec.fetched == [["esn"]] and s["pages"] == 1, (rec.fetched, s)
    rec = RecAdapter(); lines = []
    s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lines.append, only_handles=["nope"])
    assert s == {"pages": 0, "unknown_handles": ["nope"], "all_unknown": True} and not rec.fetched, s
    assert any("not an active page: nope" in str(l) for l in lines), lines
    rec = RecAdapter()
    s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lambda *_: None, only_handles=[])
    assert rec.fetched == [["isaa.uva", "esn", "other"]], "blank list means all pages"
    rec = RecAdapter()
    s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lambda *_: None, only_handles=["esn", "nope"])
    assert rec.fetched == [["esn"]] and s["unknown_handles"] == ["nope"], (rec.fetched, s)
    # fix round 1: a targeted run that cannot scrape anything must fail, not end green
    import tempfile
    tmp_summary = os.path.join(tempfile.mkdtemp(), "summary.md")
    old_env = {k: os.environ.get(k) for k in ("GITHUB_STEP_SUMMARY", "OUTLIST_HANDLES", "SUPABASE_URL", "SCRAPER_WEBHOOK_SECRET", "IG_SOURCE")}
    try:
        os.environ["GITHUB_STEP_SUMMARY"] = tmp_summary
        rec = RecAdapter(); lines = []
        s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lines.append, only_handles=["nope", "nada"])
        assert s.get("all_unknown") and exit_code(s) == 1 and not rec.fetched, "all unknown handles exits non-zero"
        assert any(str(l) == "::error::none of the requested handles is an active page: nope, nada" for l in lines), lines
        assert "none of the requested handles is an active page: nope, nada" in open(tmp_summary, encoding="utf-8").read(), "step summary"
        rec = RecAdapter(); lines = []
        s = run_flow(MultiGw(), rec, lambda ev: (200, "ok"), max_pages=10, dry_run=True, force=False, log=lines.append, only_handles=["esn", "nope"])
        assert rec.fetched == [["esn"]] and exit_code(s) == 0, "mixed handles still run the valid ones"
        assert any(str(l) == "::warning::not an active page: nope" for l in lines), lines
        assert "not an active page: nope" in open(tmp_summary, encoding="utf-8").read()
        os.environ.update({"SUPABASE_URL": "https://x.invalid", "SCRAPER_WEBHOOK_SECRET": "t", "IG_SOURCE": "file", "OUTLIST_HANDLES": " , "})
        assert main([]) == 1, "non-blank but empty handles is an error"
        assert "OUTLIST_HANDLES has no valid handle" in open(tmp_summary, encoding="utf-8").read()
    finally:
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    print("instagram_scraper --validate: OK")
    return 0


def main(argv: list[str] | None = None) -> int:
    if "--validate" in (sys.argv if argv is None else argv):
        return validate()
    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    secret = os.environ.get("SCRAPER_WEBHOOK_SECRET", "")
    gateway_url = os.environ.get("IG_GATEWAY_URL", "").strip() or (f"{supabase_url}/functions/v1/ig-gateway" if supabase_url else "")
    webhook_url = os.environ.get("SCRAPING_WEBHOOK_URL", "").strip() or (f"{supabase_url}/functions/v1/scraping-webhook" if supabase_url else "")
    source = os.environ.get("IG_SOURCE", "apify")
    keys = parse_apify_keys(os.environ)
    missing = [n for n, v in (("SUPABASE_URL", gateway_url), ("SCRAPER_WEBHOOK_SECRET", secret), ("APIFY_API_KEYS or APIFY_API_KEY", "x" if keys or source != "apify" else "")) if not v]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        return 1
    dry_run = os.environ.get("OUTLIST_DRY_RUN", "").lower() in ("1", "true", "yes")
    force = os.environ.get("OUTLIST_FORCE", "").lower() in ("1", "true", "yes")
    max_pages = int(os.environ.get("IG_MAX_PAGES", str(DEFAULT_MAX_PAGES)))
    per_page = int(os.environ.get("IG_POSTS_PER_PAGE", "8"))
    max_results = int(os.environ.get("IG_MAX_RESULTS", str(DEFAULT_MAX_RESULTS)))
    raw_handles = os.environ.get("OUTLIST_HANDLES")
    only_handles = parse_handles(raw_handles)
    if (raw_handles or "").strip() and not only_handles:  # typed something, but nothing usable: never fall back to all pages
        msg = f"OUTLIST_HANDLES has no valid handle: {raw_handles!r}"
        print(f"::error::{msg}", file=sys.stderr)
        write_step_summary(msg)
        return 1
    days = int(os.environ.get("IG_FIRST_RUN_DAYS", "14"))
    newer = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    gw = Gateway(gateway_url, secret)
    adapter = FileAdapter(os.environ.get("IG_FIXTURE", FIXTURE_PATH)) if source == "file" else ApifyAdapter(KeyPool(keys, int(time.time() // 86400)), per_page, newer, max_results)

    def post_events(events: list[dict]):
        # same door and headers as every other scraper; X-Auto-Approve false, and the webhook forces in_review for source_kind instagram anyway
        status, body = http_json(webhook_url, events, headers={"X-Webhook-Secret": secret, "X-Auto-Approve": "false"}, timeout=120)
        return status, body

    try:
        result = run_flow(gw, adapter, post_events, max_pages=max_pages, dry_run=dry_run, force=force, only_handles=only_handles)
    except Exception as exc:
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1
    return exit_code(result)


if __name__ == "__main__":
    sys.exit(main())
