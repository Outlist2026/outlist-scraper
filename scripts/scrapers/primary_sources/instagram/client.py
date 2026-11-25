"""Instagram registry wrapper (release 3.8). It does NOT fetch or classify anything itself: the direct `web_profile_info` fetch, the OCR and the
ig-gateway classification are S5's code in scripts/scrapers/instagram_scraper.py (branch feat/r38-ig-fetch). This wrapper only gives that flow
what every primary source has: a registry entry, a meta.json with the ToS/robots note, a handle allow-list, a per-source off switch and
one place (run.py) that starts it.

Events from this source are posted by instagram_scraper.main() with source_kind='instagram' and the IG pipeline's own priority, so the webhook's
+0.25 primary boost does not apply to them (see the report, open item).
"""

from __future__ import annotations

import importlib
import os
import re

HANDLE_RE = re.compile(r"^[a-z0-9._]{1,30}$")
MAX_HANDLES = 12


def handles_from(meta: dict) -> list[str]:
    raw = (meta.get("params") or {}).get("handles") or []
    out = []
    for h in raw:
        h = str(h).strip().lstrip("@").lower()
        if HANDLE_RE.match(h) and h not in out:
            out.append(h)
    return out[:MAX_HANDLES]


def run(meta: dict, dry_run: bool = False, force: bool = False, environ=None, module=None) -> tuple[int, str]:
    """Run S5's scraper for the allow-listed handles only, direct fetch only (no Apify credit). Returns (exit code, one-line summary)."""
    handles = handles_from(meta)
    if not handles:
        return 1, "no valid handle in meta.json params.handles (fail closed: never run on all pages)"
    env = os.environ if environ is None else environ
    env["OUTLIST_HANDLES"] = ",".join(handles)
    env["IG_FETCH"] = "direct"
    env["OUTLIST_DRY_RUN"] = "true" if dry_run else "false"
    env["OUTLIST_FORCE"] = "true" if force else env.get("OUTLIST_FORCE", "false")
    mod = module or importlib.import_module("instagram_scraper")  # scripts/scrapers is on sys.path (registry adds it)
    rc = int(mod.main([]))
    return rc, f"{len(handles)} handle(s) {'dry-run' if dry_run else 'run'} exit={rc}"


def fetch(http, meta: dict, **_):
    raise RuntimeError("instagram is a delegate source: run.py calls client.run(), not client.fetch()")
