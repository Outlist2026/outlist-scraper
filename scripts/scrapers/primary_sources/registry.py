"""The list of primary sources. `/primary-source <url> <region>` appends one line to SLUGS (the protocol, step 5); nothing else edits it.

A slug is also the folder name, so it must be a valid Python identifier (lowercase, digits, underscore).
"""

from __future__ import annotations

import importlib
import json
import os
import re
from dataclasses import dataclass
from types import ModuleType

SLUGS: list[str] = [
    "ra_co",
    "iamsterdam",
    "instagram",
    "eventbrite",
]

HERE = os.path.dirname(os.path.abspath(__file__))
SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
META_REQUIRED = ("slug", "name", "region_slug", "site_url", "source_url", "enabled", "robots", "tos", "risk", "rate")


@dataclass
class Source:
    slug: str
    meta: dict
    client: ModuleType
    mapper: ModuleType


def meta_problems(meta: dict, slug: str) -> list[str]:
    p = [f"meta.json: missing {k}" for k in META_REQUIRED if k not in meta]
    if meta.get("slug") != slug:
        p.append("meta.json: slug differs from the folder name")
    if not str(meta.get("source_url", "")).startswith("https://"):
        p.append("meta.json: source_url must be https")
    if (meta.get("risk") or {}).get("level") not in ("low", "medium", "high"):
        p.append("meta.json: risk.level must be low|medium|high")
    if (meta.get("tos") or {}).get("verdict") not in ("allowed", "unclear", "restricted"):
        p.append("meta.json: tos.verdict must be allowed|unclear|restricted")
    if (meta.get("robots") or {}).get("verdict") not in ("allowed", "not_found", "disallowed", "not_applicable"):
        p.append("meta.json: robots.verdict must be allowed|not_found|disallowed|not_applicable")
    if (meta.get("robots") or {}).get("verdict") == "disallowed":
        # Only with a written owner decision, and only while the source ships OFF and flagged high risk (Instagram pilot).
        if not (meta.get("enabled") is False and (meta.get("risk") or {}).get("level") == "high" and meta.get("owner_override")):
            p.append("meta.json: robots.txt disallows the endpoint; allowed only with owner_override, enabled=false and risk.level=high")
    if not isinstance(meta.get("enabled"), bool):
        p.append("meta.json: enabled must be true/false")
    return p


def load(slug: str) -> Source:
    if not SLUG_RE.match(slug):
        raise ValueError(f"bad slug {slug!r}")
    with open(os.path.join(HERE, slug, "meta.json"), encoding="utf-8") as fh:
        meta = json.load(fh)
    pkg = "primary_sources"
    return Source(slug, meta, importlib.import_module(f"{pkg}.{slug}.client"), importlib.import_module(f"{pkg}.{slug}.mapper"))


def all_sources() -> list[Source]:
    return [load(s) for s in SLUGS]


if __name__ == "__main__":  # `python scripts/scrapers/primary_sources/registry.py --validate` (the entry point DEPLOY_R38.ps1 calls) = run.py --validate
    import sys

    sys.path.insert(0, os.path.dirname(HERE))
    from primary_sources import run as _run

    sys.exit(_run.validate() if "--validate" in sys.argv else 2)
