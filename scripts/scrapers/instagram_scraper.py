#!/usr/bin/env python3
"""Outlist Instagram scraper (S5): fetch the newest posts of the pages an admin added, let Outlist classify them, post the events.

Runs on GitHub Actions of the PUBLIC runner repository (free minutes) and, with the schedule off, in the private repository.
Standard library only (OCR is an optional extra: requirements-ocr.txt): no browser, no login, no Instagram credentials. The posts come first
from Instagram's public web endpoint without login (DirectFetcher), and from Apify (adapter below) for the pages it cannot read; the decision
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
  APIFY_API_KEYS           Apify tokens, comma separated (free accounts in rotation: a quota answer moves to the next key). The regional keys stay on the
                           server (ig-gateway `apify_fetch`, R38 #211/#214); the runner no longer needs any Apify key
  APIFY_API_KEY            single Apify token, used when APIFY_API_KEYS is not set
  SCRAPER_WEBHOOK_SECRET   same secret as scraper-gateway / scraping-webhook
  SUPABASE_URL             https://<project-ref>.supabase.co  (or IG_GATEWAY_URL + SCRAPING_WEBHOOK_URL)
  OUTLIST_DRY_RUN=true     fetch and classify, post nothing, store nothing (still spends Apify credit)
  OUTLIST_FORCE=true       run even if automation is off
  OUTLIST_HANDLES=a,b      only these pages (comma separated, with or without @); empty = all active pages. Use with OUTLIST_DRY_RUN to test a page
  IG_MAX_PAGES=150  IG_POSTS_PER_PAGE=8  IG_MAX_RESULTS=1200  IG_FIRST_RUN_DAYS=14
  Cost: Apify bills per result, so a run costs about pages x posts per page results (150 x 8 = 1200, the IG_MAX_RESULTS cap); 12 posts per page costs about $3.2 per run at the FREE rate; raise IG_POSTS_PER_PAGE only together with a shorter IG_FIRST_RUN_DAYS window and a funded Apify plan
  IG_FETCH=auto|direct|apify   auto (default) = direct fetch without login first, Apify for blocked pages; direct = never Apify; apify = Apify only (old behaviour)
  IG_DIRECT_MAX=60  IG_DIRECT_MIN_DELAY=2.5  IG_DIRECT_MAX_DELAY=6   per-run cap of direct pages and the jittered pause between them
  IG_OCR=auto|on|off       RapidOCR ONNX on the cover + first 3 carousel images (auto: only when the package is installed)
  IG_OCR_CACHE_DIR=.ocr_cache  IG_OCR_MAX_IMAGES=800  IG_OCR_MAX_SECONDS=900
  IG_SOURCE=apify|file     `file` reads IG_FIXTURE (a saved Apify dataset) instead of calling Apify: offline tests

Offline self-check (no network, no key): python scripts/scrapers/instagram_scraper.py --validate
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
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
        "children": [u for u in (_https((c or {}).get("displayUrl")) for c in (it.get("childPosts") or [])) if u][:10],
        "collaborators": _handles([(c or {}).get("username") for c in (it.get("coauthorProducers") or [])]),
        "source": "apify",
    }


# ─── Several Apify keys on the free plan (S7, BUG-F20261110-45) ──────────────
# Twin of supabase/functions/_shared/apify/keys.ts: keep the two in sync (same case table in tests/apify/keys.test.ts).

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


# ─── Direct fetch without login (R38 B1) ─────────────────────────────────────
# The web app of Instagram asks `web_profile_info` for a public profile; the answer holds the profile (bio, link) and the newest ~12 posts
# with carousel children and co-authors. No login, no cookie. Polite: jittered pause between pages, a per-run cap, and a block detector that
# stops direct fetching for the rest of the run at the first sign of trouble (the remaining pages go to Apify, see HybridAdapter).

IG_APP_ID = "936619743392459"  # the public app id of the Instagram web app (sent by every browser visiting instagram.com)
DIRECT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
DIRECT_URL = "https://www.instagram.com/api/v1/users/web_profile_info/?username={handle}"
_HANDLE_OK = re.compile(r"^[a-z0-9._]{1,30}$")
_CDN_HOSTS = (".cdninstagram.com", ".fbcdn.net", ".instagram.com")


class DirectBlocked(Exception):
    """Instagram refused a login-less request (401/429/redirect to login/checkpoint): stop direct fetching, use the fallback."""
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class DirectError(Exception):
    """This page cannot be read directly. fallback=True: transient (5xx, network), Apify may still get it; False: the page is missing or private."""
    def __init__(self, why: str, fallback: bool = False):
        super().__init__(why)
        self.fallback = fallback


def http_get_raw(url: str, headers: dict | None = None, timeout: int = 30):
    """GET that follows redirects and returns (status, final_url, text). Never raises on an HTTP error status."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.geturl(), resp.read(3_000_000).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.geturl() or url, exc.read(200_000).decode("utf-8", errors="replace")


def classify_direct_response(status: int, final_url: str, text: str) -> str | None:
    """None = a normal answer. Otherwise the reason of the block: auth, rate_limit, forbidden, login, checkpoint."""
    if status == 401:
        return "auth"
    if status == 429:
        return "rate_limit"
    if status == 403:
        return "forbidden"
    if "/accounts/login" in (final_url or ""):
        return "login"
    body = (text or "").lstrip()
    data = None
    if body[:1] == "{":
        try:
            data = json.loads(body)
        except ValueError:
            data = None
    if isinstance(data, dict):
        msg = str(data.get("message") or "")
        if "checkpoint" in msg or "challenge" in msg:
            return "checkpoint"
        if data.get("require_login") is True:
            return "login"
        return None
    if status == 200 and body[:1] == "<":  # an HTML page where JSON was expected: the login wall
        return "login"
    return None


def _iso(ts) -> str | None:
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z") if isinstance(ts, (int, float)) and ts > 0 else None


def _https(url) -> str | None:
    return url if isinstance(url, str) and url.startswith("https://") and len(url) <= 1000 else None


def _handles(names) -> list[str]:
    out: list[str] = []
    for n in names:
        h = str(n or "").strip().lower()
        if _HANDLE_OK.match(h) and h not in out:
            out.append(h)
    return out[:10]


def parse_web_profile(payload: dict, page: str) -> tuple[dict, list[dict]]:
    """web_profile_info JSON -> (page info, IgPost list). Raises DirectError for a missing or private page."""
    user = ((payload or {}).get("data") or {}).get("user")
    if not isinstance(user, dict):
        raise DirectError("page not found")
    if user.get("is_private"):
        raise DirectError("page is private")
    info = {"bio": (user.get("biography") or "")[:500] or None, "externalUrl": _https(user.get("external_url")), "fullName": user.get("full_name")}
    posts: list[dict] = []
    for edge in (((user.get("edge_owner_to_timeline_media") or {}).get("edges")) or []):
        n = edge.get("node") or {}
        sc = n.get("shortcode")
        if not sc:
            continue
        owner = str((n.get("owner") or {}).get("username") or page).lower()
        typ = str(n.get("__typename") or "")
        kind = "carousel" if typ == "GraphSidecar" else "video" if typ == "GraphVideo" or n.get("is_video") else "image"
        collabs = _handles([(c or {}).get("username") for c in (n.get("coauthor_producers") or [])])
        if owner != page and page not in collabs:  # it is in this page's feed, so this page shares it
            collabs.append(page)
        caps = ((n.get("edge_media_to_caption") or {}).get("edges")) or []
        dims = n.get("dimensions") or {}
        like = (n.get("edge_media_preview_like") or {}).get("count")
        com = (n.get("edge_media_to_comment") or {}).get("count")
        posts.append({
            "shortcode": sc, "handle": owner, "caption": ((caps[0].get("node") or {}).get("text") or "")[:5000] if caps else "",
            "takenAt": _iso(n.get("taken_at_timestamp")), "type": kind, "width": dims.get("width"), "height": dims.get("height"),
            "imageUrl": _https(n.get("display_url")), "url": f"https://www.instagram.com/p/{sc}/",
            "likes": like if isinstance(like, int) else None, "comments": com if isinstance(com, int) else None,
            "children": [u for u in (_https(((c or {}).get("node") or {}).get("display_url")) for c in ((n.get("edge_sidecar_to_children") or {}).get("edges") or [])) if u][:10],
            "collaborators": collabs, "bio": info["bio"], "externalUrl": info["externalUrl"], "source": "direct",
        })
    return info, posts


class DirectFetcher:
    def __init__(self, http=None, sleep=time.sleep, rng=random.uniform, min_delay: float = 2.5, max_delay: float = 6.0):
        self.http, self.sleep, self.rng = http or http_get_raw, sleep, rng
        self.min_delay, self.max_delay = min_delay, max_delay
        self.requests = 0

    def fetch_profile(self, handle: str) -> tuple[dict, list[dict]]:
        if self.requests:  # polite: a jittered pause between two pages (none before the first)
            self.sleep(self.rng(self.min_delay, self.max_delay))
        self.requests += 1
        headers = {"User-Agent": DIRECT_UA, "X-IG-App-ID": IG_APP_ID, "X-Requested-With": "XMLHttpRequest", "Accept": "*/*",
                   "Referer": f"https://www.instagram.com/{handle}/"}
        try:
            status, final_url, text = self.http(DIRECT_URL.format(handle=urllib.parse.quote(handle)), headers=headers, timeout=30)
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise DirectError(f"network: {exc}", fallback=True)
        reason = classify_direct_response(status, final_url, text)
        if reason:
            raise DirectBlocked(reason)
        if status == 404:
            raise DirectError("page not found")
        if status != 200:
            raise DirectError(f"HTTP {status}", fallback=True)
        try:
            return parse_web_profile(json.loads(text), handle)
        except ValueError:
            raise DirectError("not JSON", fallback=True)


class RegionApifyFallback:
    """Apify fetch by the server (R38 #211, #214): ig-gateway `apify_fetch` starts one Apify run per region with that region's key (Vault), the runner
    polls the signed run tickets and receives the posts. A regional key NEVER reaches this process, and there is no key fallback here: a region with no
    key, an exhausted key, or over its daily run cap is skipped by the server with a reason, which is logged per page. Runs started by an earlier
    run and never collected come back too (`orphan`)."""

    MAX_RESTARTS = 3

    def __init__(self, gw, log=print, per_page: int = 12, newer_than: str | None = None, max_results: int = 600, sleep=time.sleep,
                 poll_seconds: int = 10, deadline_seconds: int = 20 * 60):
        self.gw, self.log = gw, log
        self.per_page, self.newer_than, self.max_results = per_page, newer_than, max_results
        self.sleep, self.poll_seconds, self.deadline_seconds = sleep, poll_seconds, deadline_seconds
        self.failed: dict[str, str] = {}
        self.pool = None

    def _start(self, handles: list[str]) -> dict:
        body = {"handles": handles, "per_page": self.per_page, "max_results": self.max_results}
        if self.newer_than:
            body["newer_than"] = self.newer_than
        return self.gw.call("apify_fetch", **body) or {}

    def _note_skipped(self, resp: dict) -> None:
        for h, why in (resp.get("skipped") or {}).items():
            self.log(f"Apify skipped for {h}: {str(why)[:200]}")

    def fetch(self, handles: list[str]) -> list[dict]:
        try:
            first = self._start(handles)
        except Exception as exc:
            raise RuntimeError(f"Apify fallback unavailable: {str(exc)[:200]}")
        self._note_skipped(first)
        posts: list[dict] = []
        ok, last = 0, ""
        for h, why in (first.get("failed") or {}).items():
            self.failed[h] = str(why)[:300]
            last = str(why)
        pending = [{"ticket": r["ticket"], "handles": list(r.get("handles") or []), "restarts": 0} for r in (first.get("runs") or []) if isinstance(r, dict) and r.get("ticket")]
        deadline = time.time() + self.deadline_seconds
        while pending and time.time() < deadline:
            nxt = []
            for run in pending:
                out = self.gw.call("apify_fetch", ticket=run["ticket"]) or {}
                state = out.get("state")
                if state == "running":
                    nxt.append(run)
                elif state == "done":
                    ok += 1
                    posts += [p for p in (normalize_apify_item(i) for i in (out.get("items") or [])) if p]
                elif state == "failed" and out.get("key_exhausted") and run["restarts"] < self.MAX_RESTARTS:
                    again = self._start(run["handles"])  # the server marked that key exhausted: the next key of the region
                    self._note_skipped(again)
                    for h, why in (again.get("failed") or {}).items():
                        self.failed[h] = str(why)[:300]
                        last = str(why)
                    nxt += [{"ticket": r["ticket"], "handles": list(r.get("handles") or []), "restarts": run["restarts"] + 1} for r in (again.get("runs") or []) if isinstance(r, dict) and r.get("ticket")]
                else:
                    last = str(out.get("reason") or "Apify run failed")[:300]
                    for h in run["handles"]:
                        self.failed[h] = last
            pending = nxt
            if pending:
                self.sleep(self.poll_seconds)
        for run in pending:  # still running after the deadline
            last = "Apify run did not finish in 20 minutes"
            for h in run["handles"]:
                self.failed[h] = last
        if not ok:
            raise AllKeysExhausted(last or "Apify returned nothing")
        return posts


class HybridAdapter:
    """Direct first, Apify for what direct cannot do. Telemetry in .stats: direct / fallback / failed pages, blocks and their reasons."""

    def __init__(self, direct, fallback, direct_cap: int | None = None, log=print):
        self.direct, self.fallback, self.direct_cap, self.log = direct, fallback, direct_cap, log
        self.stats = {"direct": 0, "fallback": 0, "failed": 0, "blocked": 0, "block_reasons": []}
        self.failed: dict[str, str] = {}

    @property
    def pool(self):
        return getattr(self.fallback, "pool", None)

    def fetch(self, handles: list[str]) -> list[dict]:
        posts: list[dict] = []
        todo: list[str] = []
        tripped, tried = False, 0
        for h in handles:
            if tripped or (self.direct_cap is not None and tried >= self.direct_cap):
                todo.append(h)
                continue
            tried += 1
            try:
                _info, ps = self.direct.fetch_profile(h)
                posts += ps
                self.stats["direct"] += 1
            except DirectBlocked as blk:
                tripped = True
                self.stats["blocked"] += 1
                self.stats["block_reasons"].append(blk.reason)
                self.log(f"Instagram blocked the direct fetch ({blk.reason}): the rest of the pages go through Apify")
                todo.append(h)
            except DirectError as err:
                if err.fallback:
                    todo.append(h)
                else:
                    self.stats["failed"] += 1
                    self.failed[h] = str(err)
        last: Exception | None = None
        if todo:
            try:
                got = self.fallback.fetch(todo)
            except Exception as exc:  # fallback unusable (all keys used up ...): those pages failed, direct posts are kept
                last, got = exc, []
                for h in todo:
                    self.failed[h] = str(exc)[:300]
                self.stats["failed"] += len(todo)
            else:
                posts += got
                gone = {h: m for h, m in (getattr(self.fallback, "failed", {}) or {}).items() if h in todo}
                self.failed.update(gone)
                self.stats["failed"] += len(gone)
                self.stats["fallback"] += len(todo) - len(gone)
        if not posts and handles and len(self.failed) == len(handles):
            raise last or RuntimeError("no page could be fetched: " + "; ".join(sorted(set(self.failed.values()))[:3]))
        return posts


# ─── OCR of the poster (R38 B2): RapidOCR ONNX on the CPU, optional ──────────
# Reads the cover and the first 3 carousel images. Text + boxes go to the gateway; results are cached by the SHA-256 of the image bytes
# (Instagram rotates CDN urls, the picture stays), in memory and in a directory the workflow keeps between runs. Without the package the
# run goes on without OCR (`pip install -r scripts/scrapers/requirements-ocr.txt`).

class RapidOcrEngine:
    name = "rapidocr-onnx"
    MAX_SIDE = 1600

    def __init__(self):
        from rapidocr_onnxruntime import RapidOCR  # optional dependency
        self.ocr = RapidOCR()

    def read(self, data: bytes) -> dict:
        import io
        import numpy
        from PIL import Image
        img = Image.open(io.BytesIO(data)).convert("RGB")
        if max(img.size) > self.MAX_SIDE:
            f = self.MAX_SIDE / max(img.size)
            img = img.resize((max(1, int(img.width * f)), max(1, int(img.height * f))))
        result, _ = self.ocr(numpy.array(img)[:, :, ::-1])
        lines = []
        for box, text, conf in (result or []):
            xs, ys = [p[0] for p in box], [p[1] for p in box]
            lines.append({"text": str(text)[:300], "x0": round(min(xs)), "y0": round(min(ys)), "x1": round(max(xs)), "y1": round(max(ys)), "conf": round(float(conf), 3)})
        return {"lines": lines, "width": img.width, "height": img.height}


def load_ocr_engine(mode: str = "auto"):
    """'off' -> None. 'auto' -> the engine, or None when the package is missing. 'on' -> the engine, or an error."""
    if mode == "off":
        return None
    try:
        return RapidOcrEngine()
    except Exception as exc:
        if mode == "on":
            raise RuntimeError(f"IG_OCR=on but RapidOCR is not usable: {exc}")
        return None


def http_bytes(url: str, max_bytes: int = 8_000_000, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": DIRECT_UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise OSError("image too large")
    return data


def make_thumb(data: bytes) -> list[float] | None:
    """28x28 grayscale of an image, 784 floats 0..1 row-major (the layout input of the v2 model, R38 #207). Needs Pillow (optional): None without it
    or for bytes that are not an image."""
    try:
        import io
        from PIL import Image
        with Image.open(io.BytesIO(data)) as img:
            g = img.convert("L").resize((28, 28), Image.LANCZOS)
            return [round(v / 255.0, 2) for v in g.getdata()]
    except Exception:
        return None


def _cdn_url(url) -> bool:
    if not isinstance(url, str) or not url.startswith("https://"):
        return False
    host = urllib.parse.urlparse(url).hostname or ""
    return any(host.endswith(s) or host == s.lstrip(".") for s in _CDN_HOSTS)


class OcrReader:
    def __init__(self, engine, get_bytes=http_bytes, cache_dir: str | None = None, max_images: int = 800, max_seconds: float | None = None, clock=time.time):
        self.engine, self.get, self.cache_dir, self.max_images = engine, get_bytes, cache_dir, max_images
        self.memory: dict[str, dict] = {}
        self.used = 0
        self.deadline = clock() + max_seconds if max_seconds else None
        self.clock = clock
        self.stats = {"images": 0, "cache_hits": 0, "skipped": 0}
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    @staticmethod
    def _store(path: str | None, hit: dict) -> None:
        if path:
            try:
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(hit, fh)
            except OSError:
                pass

    def _read_image(self, url: str) -> dict | None:
        if self.used >= self.max_images or (self.deadline and self.clock() > self.deadline) or not _cdn_url(url):
            self.stats["skipped"] += 1
            return None
        self.used += 1
        try:
            data = self.get(url)
        except Exception:
            self.stats["skipped"] += 1
            return None
        digest = hashlib.sha256(data).hexdigest()
        hit = self.memory.get(digest)
        path = os.path.join(self.cache_dir, digest + ".json") if self.cache_dir else None
        if hit is None and path and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    hit = json.load(fh)
            except (OSError, ValueError):
                hit = None
        if hit is not None:
            self.stats["cache_hits"] += 1
            if "thumb" not in hit:  # a cache entry from before the thumb existed
                hit["thumb"] = make_thumb(data)
                self._store(path, hit)
        else:
            try:
                hit = self.engine.read(data)
            except Exception:
                self.stats["skipped"] += 1
                return None
            hit["thumb"] = make_thumb(data)
            self._store(path, hit)
        self.memory[digest] = hit
        self.stats["images"] += 1
        return hit

    def read_post(self, post: dict) -> dict | None:
        urls: list[str] = []
        for u in [post.get("imageUrl")] + list(post.get("children") or []):
            if u and u not in urls:
                urls.append(u)
        results = [r for r in (self._read_image(u) for u in urls[:4]) if r]
        if not results:
            return None
        cover = results[0]
        texts = ["\n".join(l["text"] for l in sorted(r["lines"], key=lambda l: (l["y0"], l["x0"]))) for r in results]
        area = sum(max(0, l["x1"] - l["x0"]) * max(0, l["y1"] - l["y0"]) for l in cover["lines"])
        size = (cover.get("width") or 0) * (cover.get("height") or 0)
        out = {
            "text": "\n".join(t for t in texts if t)[:4000],
            "lines": sorted(cover["lines"], key=lambda l: (l["y0"], l["x0"]))[:60],
            "textAreaRatio": round(min(1.0, area / size), 6) if size else None,
            "images": len(results),
            "engine": getattr(self.engine, "name", "ocr"),
        }
        if cover.get("thumb"):
            out["thumb"] = cover["thumb"]  # moved onto the post by read_posters: the gateway reads it at post level
        return out


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
DEFAULT_POSTS_PER_PAGE = 8  # cost = pages x posts per page results; 12 posts per page costs about $3.2 per run at the FREE rate; raise only with a shorter IG_FIRST_RUN_DAYS and a funded Apify plan
DEFAULT_MAX_RESULTS = 1200  # DEFAULT_MAX_PAGES x DEFAULT_POSTS_PER_PAGE


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


def webhook_outcome(body) -> str:
    """What happened to the events of one webhook call, for `confirm` (R38 B2 fix): a post is seen only if the webhook stored it or dropped it for a
    final reason. stored = inserted, merged into an existing event or a duplicate. rejected = dropped (past event, spam, bad title, outside NL ...).
    retry = the webhook reported errors: the post must be sent again."""
    if isinstance(body, dict):
        if body.get("errors"):
            return "retry"
        if "inserted" in body and not (int(body.get("inserted") or 0) + int(body.get("skipped_duplicates") or 0) + int(body.get("merged") or 0)):
            return "rejected"
    return "stored"


def read_posters(gw, ocr, posts: list[dict], log=print) -> None:
    """OCR only the posts the gateway has not judged yet (ig-gateway `unseen`): most posts of a run are old. A gateway without the action: all posts."""
    wanted = None
    try:
        codes = [p["shortcode"] for p in posts]
        wanted = set()
        for i in range(0, len(codes), 500):
            out = gw.call("unseen", shortcodes=codes[i:i + 500])
            if not isinstance(out, dict) or not isinstance(out.get("unseen"), list):
                wanted = None
                break
            wanted.update(out["unseen"])
    except Exception as exc:
        log(f"unseen list not available ({str(exc)[:80]}): reading every poster")
        wanted = None
    for p in posts:
        if wanted is not None and p["shortcode"] not in wanted:
            continue
        try:
            res = ocr.read_post(p)
        except Exception as exc:  # OCR must never stop a run
            log(f"OCR failed for {p['shortcode']}: {str(exc)[:80]}")
            continue
        if res:
            thumb = res.pop("thumb", None)
            if thumb:
                p["thumb"] = thumb
            p["ocr"] = res


def run_flow(gw, adapter, post_events, *, max_pages: int, dry_run: bool, force: bool, log=print, only_handles: list[str] | None = None, ocr=None) -> dict:
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
        fs = summary.get("fetch")
        if fs:  # per-run telemetry (R38 B1): how many pages came directly, through Apify, or not at all
            keys_note += f", direct {fs['direct']}, fallback {fs['fallback']}, failed {fs['failed']}"
            if fs.get("block_reasons"):
                keys_note += f" (blocked: {','.join(fs['block_reasons'])})"
        if summary.get("retry"):
            keys_note += f", retry {summary['retry']}"
        ost = getattr(ocr, "stats", None)
        if ost:
            keys_note += f", ocr {ost['images']} images ({ost['cache_hits']} cached)"
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
    stats = getattr(adapter, "stats", None)
    if stats:
        summary["fetch"] = dict(stats)
    for h, why in (getattr(adapter, "failed", None) or {}).items():  # pages that could not be fetched at all: reported in the admin page
        if h in per_handle:
            per_handle[h]["error"] = f"fetch failed: {why}"[:400]
            summary["errors"] += 1
    if ocr is not None and posts:
        read_posters(gw, ocr, posts, log)
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
                outcome = webhook_outcome(body)
                if outcome == "retry":
                    summary["retry"] = summary.get("retry", 0) + len(pending)
                    summary["errors"] += 1
                    log("Webhook reported errors: these posts stay unseen and are sent again next run")
                    for e in events:
                        h = str(e.get("source", "")).split(":", 1)[-1]
                        if h in per_handle:
                            per_handle[h]["error"] = "webhook error, retry next run"
                else:
                    gw.call("confirm", posts=[{**p, "outcome": outcome} for p in pending])
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


class _NoFallback:
    """IG_FETCH=direct: pages direct fetching cannot read are failed, never sent to Apify."""
    failed: dict = {}

    def fetch(self, handles):
        raise RuntimeError("Apify fallback is switched off (IG_FETCH=direct)")


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


def _validate_fetch() -> None:
    """Direct fetch (R38): block detector and the direct -> Apify handover, no network (full table in tests/ig/fetch_test.py)."""
    assert classify_direct_response(429, "u", "{}") == "rate_limit" and classify_direct_response(200, "https://www.instagram.com/accounts/login/", "") == "login"
    assert classify_direct_response(200, "u", '{"message":"checkpoint_required"}') == "checkpoint" and classify_direct_response(200, "u", '{"data":{}}') is None
    node = {"shortcode": "AAAAA1", "__typename": "GraphSidecar", "owner": {"username": "bigclub"}, "taken_at_timestamp": 1764000000, "display_url": "https://x.cdninstagram.com/a.jpg",
            "edge_sidecar_to_children": {"edges": [{"node": {"display_url": "https://x.cdninstagram.com/b.jpg"}}]}}
    payload = {"data": {"user": {"username": "esn", "biography": "bio", "external_url": "https://linktr.ee/x", "edge_owner_to_timeline_media": {"edges": [{"node": node}]}}}}
    info, posts = parse_web_profile(payload, "esn")
    assert info["bio"] == "bio" and posts[0]["type"] == "carousel" and posts[0]["children"] and posts[0]["collaborators"] == ["esn"], "profile parsing"

    class Direct:
        def fetch_profile(self, h):
            if h == "blocked":
                raise DirectBlocked("rate_limit")
            return {}, [{"shortcode": "D" + h, "handle": h, "source": "direct"}]

    class Fallback:
        failed: dict = {}
        def fetch(self, hs):
            return [{"shortcode": "F" + h, "handle": h, "source": "apify"} for h in hs]

    h = HybridAdapter(Direct(), Fallback(), log=lambda *_: None)
    got = h.fetch(["ok", "blocked", "later"])
    assert h.stats["direct"] == 1 and h.stats["fallback"] == 2 and h.stats["blocked"] == 1 and len(got) == 3, h.stats
    assert webhook_outcome({"inserted": 1}) == "stored" and webhook_outcome({"inserted": 0, "filtered": 1}) == "rejected" and webhook_outcome({"errors": ["x"]}) == "retry"


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
    _validate_fetch()
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
    assert DEFAULT_MAX_PAGES == 150 and DEFAULT_MAX_RESULTS == 1200 and DEFAULT_POSTS_PER_PAGE == 8, "defaults are 150 pages, 8 posts per page and 1200 results"

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
    fetch_mode = os.environ.get("IG_FETCH", "auto").lower()  # auto = direct first, Apify fallback | direct | apify (old behaviour)
    missing = [n for n, v in (("SUPABASE_URL", gateway_url), ("SCRAPER_WEBHOOK_SECRET", secret)) if not v]  # no Apify key needed here: the regional keys live on the server (apify_fetch)
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        return 1
    dry_run = os.environ.get("OUTLIST_DRY_RUN", "").lower() in ("1", "true", "yes")
    force = os.environ.get("OUTLIST_FORCE", "").lower() in ("1", "true", "yes")
    max_pages = int(os.environ.get("IG_MAX_PAGES", str(DEFAULT_MAX_PAGES)))
    per_page = int(os.environ.get("IG_POSTS_PER_PAGE", str(DEFAULT_POSTS_PER_PAGE)))
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
    if source == "file":
        adapter = FileAdapter(os.environ.get("IG_FIXTURE", FIXTURE_PATH))
    else:
        fallback = RegionApifyFallback(gw, per_page=per_page, newer_than=newer, max_results=max_results)
        if fetch_mode == "apify":
            adapter = fallback
        else:
            direct = DirectFetcher(min_delay=float(os.environ.get("IG_DIRECT_MIN_DELAY", "2.5")), max_delay=float(os.environ.get("IG_DIRECT_MAX_DELAY", "6")))
            adapter = HybridAdapter(direct, fallback if fetch_mode != "direct" else _NoFallback(), direct_cap=int(os.environ.get("IG_DIRECT_MAX", "60")))
    ocr = None
    engine = load_ocr_engine(os.environ.get("IG_OCR", "auto").lower())
    if engine is not None:
        ocr = OcrReader(engine, cache_dir=os.environ.get("IG_OCR_CACHE_DIR", ".ocr_cache"), max_images=int(os.environ.get("IG_OCR_MAX_IMAGES", "800")),
                        max_seconds=float(os.environ.get("IG_OCR_MAX_SECONDS", "900")))
    else:
        print("OCR is off (RapidOCR not installed or IG_OCR=off): posters are classified from the caption only")

    def post_events(events: list[dict]):
        # same door and headers as every other scraper; X-Auto-Approve false, and the webhook forces in_review for source_kind instagram anyway
        status, body = http_json(webhook_url, events, headers={"X-Webhook-Secret": secret, "X-Auto-Approve": "false"}, timeout=120)
        return status, body

    try:
        result = run_flow(gw, adapter, post_events, max_pages=max_pages, dry_run=dry_run, force=force, only_handles=only_handles, ocr=ocr)
    except Exception as exc:
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1
    return exit_code(result)


if __name__ == "__main__":
    sys.exit(main())
