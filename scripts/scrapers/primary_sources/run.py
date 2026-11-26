#!/usr/bin/env python3
"""Primary-sources runner (release 3.8, Train C). Runs on GitHub Actions of the PUBLIC scraper repo (workflow primary-scrape.yml).

Flow, per run:
  1. gateway `automation`            -> stop when the global automation switch is off (OUTLIST_FORCE=true overrides)
  2. gateway `primary_sources`       -> the active scraping_sources rows with is_primary=true (the DB off switch: is_active)
  3. per registered source           -> skip when meta.enabled=false, when OUTLIST_PRIMARY_<SLUG>=off, or when it has no active row
                                        client.fetch -> mapper.map_events -> validate -> cap -> POST scraping-webhook (source_kind='primary')
  4. gateway `source_status` + `run_log` per source.

The database service key never reaches this repository: only SUPABASE_URL and SCRAPER_WEBHOOK_SECRET (same as the other scrapers).

Usage:
  python scripts/scrapers/primary_sources/run.py --validate            offline self-checks (no network)
  python scripts/scrapers/primary_sources/run.py --probe <slug>        ONE live read-only fetch + map, prints a summary, posts nothing
  python scripts/scrapers/primary_sources/run.py                       the real run (env below)
Env: SUPABASE_URL, SCRAPER_WEBHOOK_SECRET, OUTLIST_DRY_RUN=true (fetch+map, post nothing), OUTLIST_FORCE=true, OUTLIST_ONLY_SOURCE=<slug>,
     SCRAPING_WEBHOOK_URL / SCRAPER_GATEWAY_URL (optional overrides), OUTLIST_PRIMARY_<SLUG>=off (per-source off switch).
"""

from __future__ import annotations

import json
import os
import sys
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # so `primary_sources` imports with `python -I` too

from primary_sources import base, registry  # noqa: E402


# ─── selection (pure, tested) ────────────────────────────────────────────────────────────────────────────────────────
def select_sources(sources, rows: list[dict], *, only: str = "", live: bool = True):
    """[(source, db_row_or_None, skip_reason_or_None)]. In live mode a source needs an active, primary DB row with the same primary_slug."""
    by_slug = {r.get("primary_slug"): r for r in rows if r.get("is_primary", True)}
    out = []
    for s in sources:
        if only and only != s.slug:
            continue
        row = by_slug.get(s.slug)
        if s.meta.get("enabled") is not True:
            out.append((s, row, "disabled in meta.json (enabled=false)"))
        elif base.env_off(s.slug):
            out.append((s, row, f"switched off by {s.meta.get('env_switch') or 'OUTLIST_PRIMARY_' + s.slug.upper()}"))
        elif live and row is None:
            out.append((s, None, "no active scraping_sources row (is_active=false or not seeded)"))
        else:
            out.append((s, row, None))
    return out


def collect(src, http, now: datetime | None = None) -> tuple[list[dict], list[str]]:
    """fetch -> map -> future only -> dedupe -> cap -> validate. Raises on a bad batch so nothing half-valid is posted."""
    raw = src.client.fetch(http, src.meta)
    events = src.mapper.map_events(raw, src.meta)
    events = base.dedupe(base.future_only(events, now))[: int(src.meta.get("event_cap") or base.EVENT_CAP_DEFAULT)]
    problems = base.validate_events(events, source_prefix=src.meta["source_prefix"], now=now)
    if problems:
        raise RuntimeError(f"{len(problems)} invalid event(s), first: {problems[0]}")
    return events, []


def http_for(meta: dict) -> base.Http:
    rate = meta.get("rate") or {}
    return base.Http(min_interval_s=float(rate.get("min_interval_s", 1.5)), max_requests=int(rate.get("max_requests", 40)),
                     enforce_robots=(meta.get("robots") or {}).get("verdict") != "not_applicable")


# ─── gateway + webhook ───────────────────────────────────────────────────────────────────────────────────────────────
class Gateway:
    def __init__(self, url: str, secret: str, post=None):
        self.url, self.secret, self._post = url, secret, post or self._urllib

    @staticmethod
    def _urllib(url: str, headers: dict, body: bytes, timeout: int) -> tuple[int, str]:
        req = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", errors="replace")

    def call(self, action: str, **payload):
        status, raw = self._post(self.url, {"Content-Type": "application/json", "X-Webhook-Secret": self.secret, "User-Agent": base.UA},
                                 json.dumps({"action": action, **payload}).encode("utf-8"), 30)
        try:
            return status, (json.loads(raw) if raw else None)
        except ValueError:
            return status, None


def post_events(webhook_url: str, secret: str, events: list[dict], batch: int = 25, post=None) -> tuple[bool, str]:
    """POST in batches (the webhook dedupes, so a retry of a finished batch is harmless). Returns (ok, one-line summary)."""
    post = post or Gateway._urllib
    totals = {"inserted": 0, "skipped_duplicates": 0, "filtered": 0, "merged": 0}
    failed: list[str] = []
    for b in range(0, len(events), batch):
        chunk = events[b:b + batch]
        err = ""
        for attempt in (1, 2):
            try:
                status, raw = post(webhook_url, {"Content-Type": "application/json", "X-Webhook-Secret": secret, "X-Auto-Approve": "false", "User-Agent": base.UA},
                                   json.dumps(chunk).encode("utf-8"), 120)
            except Exception as exc:  # noqa: BLE001 - timeout or reset, retried once
                err = f"batch {b // batch + 1} attempt {attempt}: {exc}"
                continue
            if 200 <= status < 300:
                try:
                    data = json.loads(raw)
                    for k in totals:
                        totals[k] += int(data.get(k) or 0)
                except (ValueError, TypeError, AttributeError):
                    pass
                err = ""
                break
            err = f"batch {b // batch + 1} attempt {attempt}: HTTP {status} {raw[:100]}"
        if err:
            failed.append(err)
    summary = f"{len(events)} event(s): " + " ".join(f"{k}={v}" for k, v in totals.items())
    return not failed, summary + (" FAILED " + " | ".join(failed[:3]) if failed else "")


# ─── commands ────────────────────────────────────────────────────────────────────────────────────────────────────────
def probe(slug: str) -> int:
    """One live READ-ONLY fetch of a source (what the build evidence uses). No secrets needed, nothing is posted."""
    src = registry.load(slug)
    if src.meta["tos"]["verdict"] == "restricted":  # a source whose terms forbid bots gets the smallest possible probe: one page, one request
        src.meta = {**src.meta, "params": {**(src.meta.get("params") or {}), "max_pages": 1}}
    events, _ = collect(src, http_for(src.meta))
    print(json.dumps({"slug": slug, "events": len(events), "sample": [{k: e.get(k) for k in ("title", "raw_date_string", "raw_time_string", "venue_name", "external_url")} for e in events[:3]],
                      "tos": src.meta["tos"]["verdict"], "risk": src.meta["risk"]["level"]}, ensure_ascii=False, indent=1))
    return 0 if events else 2


def run() -> int:
    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    secret = os.environ.get("SCRAPER_WEBHOOK_SECRET", "")
    webhook_url = os.environ.get("SCRAPING_WEBHOOK_URL", "").strip() or (f"{supabase_url}/functions/v1/scraping-webhook" if supabase_url else "")
    gateway_url = os.environ.get("SCRAPER_GATEWAY_URL", "").strip() or (f"{supabase_url}/functions/v1/scraper-gateway" if supabase_url else "")
    dry = os.environ.get("OUTLIST_DRY_RUN", "").lower() in ("1", "true", "yes")
    force = os.environ.get("OUTLIST_FORCE", "").lower() in ("1", "true", "yes")
    only = os.environ.get("OUTLIST_ONLY_SOURCE", "").strip().lower()
    if not (webhook_url and gateway_url and secret):
        print("Missing SUPABASE_URL (or SCRAPING_WEBHOOK_URL + SCRAPER_GATEWAY_URL) or SCRAPER_WEBHOOK_SECRET", file=sys.stderr)
        return 1
    gw = Gateway(gateway_url, secret)
    if not force:
        status, body = gw.call("automation")
        if status == 200 and isinstance(body, dict) and body.get("enabled") is not True:
            print("SYSTEM_AUTOMATION_ENABLED=false - primary scraping intentionally disabled")
            return 0
    status, body = gw.call("primary_sources")
    if status != 200 or not isinstance(body, dict) or not isinstance(body.get("sources"), list):
        print(f"::error::gateway primary_sources returned HTTP {status}", file=sys.stderr)
        return 1
    plan = select_sources(registry.all_sources(), body["sources"], only=only)
    ran = errors = 0
    for src, row, why in plan:
        if why:
            print(f"[{src.slug}] skipped: {why}")
            continue
        ran += 1
        try:
            if src.meta.get("mode") == "delegate":  # events are produced and posted by another runner (instagram -> S5's scraper + ig-gateway)
                rc, summary = src.client.run(src.meta, dry_run=dry, force=force)
                print(f"[{src.slug}] delegated: {summary}")
                if rc != 0:
                    raise RuntimeError(f"delegate exit code {rc}")
                if not dry:
                    gw.call("source_status", id=row["id"], last_status="success", events_found_last_run=0)
                continue
            events, _ = collect(src, http_for(src.meta))
            print(f"[{src.slug}] {len(events)} event(s) fetched{' [DRY RUN]' if dry else ''}")
            if dry:
                for e in events[:10]:
                    print(f"   - {e['raw_date_string']} {e['raw_time_string']} {e['title']} @ {e['venue_name']}")
                continue
            ok, summary = (True, "nothing to post") if not events else post_events(webhook_url, secret, events)
            print(f"[{src.slug}] {summary}")
            if not ok:
                raise RuntimeError(summary)
            gw.call("source_status", id=row["id"], last_status="success", events_found_last_run=len(events))
            gw.call("run_log", source_id=row["id"], http_status=200, events_found=len(events), with_image=sum(1 for e in events if e.get("image_url")),
                    with_link=len(events), with_time=len(events), error=None)
        except Exception as exc:  # noqa: BLE001 - one source failing must not stop the others
            errors += 1
            print(f"::error::[{src.slug}] {exc}", file=sys.stderr)
            if not dry and row:
                gw.call("source_status", id=row["id"], last_status="error", last_error=str(exc)[:500], events_found_last_run=0)
                gw.call("run_log", source_id=row["id"], http_status=0, events_found=0, with_image=0, with_link=0, with_time=0, error=str(exc)[:500])
    print(f"Primary run complete: {ran} source(s), {errors} error(s)")
    return 1 if ran and errors == ran else 0


def validate() -> int:
    """Offline self-checks: registry vs folders, meta.json contract, every source's test_offline, plus the framework tests. No network."""
    problems: list[str] = []
    on_disk = sorted(d for d in os.listdir(HERE) if os.path.isfile(os.path.join(HERE, d, "meta.json")))
    if sorted(registry.SLUGS) != on_disk:
        problems.append(f"registry.SLUGS {sorted(registry.SLUGS)} differs from the folders with a meta.json {on_disk}")
    suite = unittest.TestSuite()
    loader = unittest.defaultTestLoader
    for slug in registry.SLUGS:
        try:
            src = registry.load(slug)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{slug}: cannot load ({exc})")
            continue
        problems += [f"{slug}: {p}" for p in registry.meta_problems(src.meta, slug)]
        for req in ("client.py", "mapper.py", "test_offline.py"):
            if not os.path.isfile(os.path.join(HERE, slug, req)):
                problems.append(f"{slug}: missing {req}")
        if not os.listdir(os.path.join(HERE, slug, "fixtures")):
            problems.append(f"{slug}: fixtures/ is empty")
        suite.addTests(loader.loadTestsFromName(f"primary_sources.{slug}.test_offline"))
    suite.addTests(loader.loadTestsFromName("primary_sources.test_framework"))
    result = unittest.TextTestRunner(verbosity=1, stream=sys.stdout).run(suite)
    for p in problems:
        print(f"::error::{p}", file=sys.stderr)
    ok = result.wasSuccessful() and not problems
    print(f"primary_sources --validate: {'OK' if ok else 'FAILED'} ({len(registry.SLUGS)} source(s), {result.testsRun} test(s))")
    return 0 if ok else 1


if __name__ == "__main__":
    if "--validate" in sys.argv:
        sys.exit(validate())
    if "--probe" in sys.argv:
        sys.exit(probe(sys.argv[sys.argv.index("--probe") + 1]))
    sys.exit(run())
