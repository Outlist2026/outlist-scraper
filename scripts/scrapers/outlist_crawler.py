#!/usr/bin/env python3
"""Outlist Layer-2 crawler — Crawl4AI (headless Chromium) pushing into the existing scraping-webhook.

Replaces the partyflock.py stub as the payload producer for GitHub Actions
(.github/workflows/scraping-layer2.yml). The webhook contract, validators, dedup
and the edge-function pipeline are untouched: this script is just a smarter
producer of the same RawEvent[] payload that run-configured-scrapers posts.

Per source (scraping_sources rows, method='html_scrape', is_active=true):
  1. JSON-LD events parsed from the rendered HTML (preferred, mirrors the edge fn).
  2. JsonCssExtractionStrategy built from DB selectors, or from the default pack
     in source_schemas.json when the row has no event_container (FM-1).
  3. Heuristic container discovery fallback (same 9 candidates as the edge fn).
Image (src/data-src/srcset-largest), link (first a[href]) and time hardening run
on every candidate (FM-3/FM-4/FM-5); raw_time_string is consumed by the webhook's
parseTimeString. Per-source try/except — one blocked site never kills the run.

Offline validation (no crawl4ai, no network, REGOLA ZERO friendly):
  python scripts/scrapers/outlist_crawler.py --validate
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from html import unescape

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMAS_PATH = os.path.join(BASE_DIR, "source_schemas.json")
FIXTURE_PATH = os.path.join(BASE_DIR, "fixtures", "occii_sample.html")

CRAWL_TIMEOUT_MS = 30000
HEURISTIC_CANDIDATES = [
    "[data-event-id]", "[data-event]", '[itemtype*="Event"]',
    "article.event", ".event-card", ".event-item", ".event-listing",
    "article", "li.event",
]

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ─── generic helpers ─────────────────────────────────────────────────────────

_DOMAIN_RE = re.compile(r"^(https?://|www\.)|^[a-z0-9-]+(\.[a-z0-9-]+)*\.(com|nl|org|net|eu|io|co|info|be|de|uk)(/\S*)?$", re.I)


def venue_fallback(source_label: str) -> str:
    """Source label as venue only when it is a place name; a domain ("site.com") is never a venue."""
    label = (source_label or "").strip()
    return "" if _DOMAIN_RE.search(label) else label


def as_string(value: object) -> str:
    """Collapse JSON-LD scalars/lists/objects to text (port of edge fn asString)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return as_string(value[0]) if value else ""
    if isinstance(value, dict):
        for key in ("name", "text", "url", "@id"):
            if key in value:
                s = as_string(value[key])
                if s:
                    return s
    return ""


def absolutize(base_url: str, url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "data:", "mailto:", "tel:")):
        return url
    try:
        return urllib.parse.urljoin(base_url, url)
    except ValueError:
        return url


def collapse_text(value: object) -> str:
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def html_text(fragment: str) -> str:
    return collapse_text(re.sub(r"<[^>]+>", " ", fragment or ""))


# ─── title sanitizer (mirror of supabase/functions/_shared/extract/title.ts) ─
_T_SEP = r"[\s\-–—|:·•,;/]"
_T_CTA = (r"(?:koop\s+(?:je\s+)?tickets?|tickets?\s+kopen|bestel\s+tickets?|buy\s+(?:your\s+)?tickets?|"
          r"get\s+(?:your\s+)?tickets?|book\s+(?:now|tickets?)|boek\s+nu|meer\s+info(?:rmatie)?|"
          r"more\s+info(?:rmation)?|lees\s+meer|read\s+more|view\s+more|bekijk\s+(?:meer|event|evenement)|"
          r"uitverkocht|sold\s+out|accept(?:eer)?\s+(?:all\s+)?(?:cookies?|alles)|cookies?|menu)")
_T_PREFIX = re.compile(r"^(?:" + _T_CTA + r"|tickets?(?=\s*[-–—|:·•,;/]))(?:" + _T_SEP + r"+|$)", re.I)
_T_SUFFIX = re.compile(r"(?:^|" + _T_SEP + r"+)" + _T_CTA + r"$", re.I)
_T_SUFFIX_BARE = re.compile(r"[-–—|:·•,;/]\s*tickets?$", re.I)
_T_EDGE = re.compile(r"^" + _T_SEP + r"+|" + _T_SEP + r"+$")
# "waitlist" / "wachtlijst" (S6 rule, BUG-F20261110-41; mirror of stripWaitlist in _shared/extract/title.ts, cases in
# test_fixtures/extraction/title_cases.json): whole word, case-insensitive, also mid-title.
_WAIT = r"(?:wait[\s-]?list|wachtlijst)"
_T_WAIT_BRACKETED = re.compile(r"[(\[{]\s*(?<!\w)" + _WAIT + r"(?!\w)\s*[)\]}]", re.I)
_T_WAIT = re.compile(r"(?<!\w)" + _WAIT + r"(?!\w)", re.I)
_T_EMPTY_BRACKETS = re.compile(r"[(\[{]\s*[)\]}]")
_T_DOUBLE_SEP = re.compile(r"([-–—|:·•,;/])(?:\s+[-–—|:·•,;/])+")


def strip_waitlist(s: str) -> str:
    if not _T_WAIT.search(s):
        return s
    s = _T_WAIT_BRACKETED.sub(" ", s)
    s = _T_WAIT.sub(" ", s)
    s = _T_EMPTY_BRACKETS.sub(" ", s)
    s = _T_DOUBLE_SEP.sub(r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


_T_JUNK = {
    "home", "menu", "agenda", "tickets", "ticket", "cookies", "cookie", "search", "zoeken", "login", "inloggen",
    "contact", "over ons", "about", "nieuws", "news", "programma", "program", "programme", "events", "evenementen",
    "meer", "more", "info", "tba", "tbd", "amsterdam", "online", "virtual", "zoom", "teams", "skip to content",
}


def sanitize_title(value: object) -> str:
    s = str(value or "")
    for _ in range(2):  # entities may reveal a second layer of markup
        s = re.sub(r"<!--[\s\S]*?(?:-->|$)", " ", s)
        s = re.sub(r"<(script|style)\b[\s\S]*?(?:</\1\s*>|$)", " ", s, flags=re.I)
        s = re.sub(r"</?[a-z!][^>]*>", " ", s, flags=re.I)
        s = re.sub(r"</?[a-z!][^>]*$", " ", s, flags=re.I)
        s = unescape(s)
        if not re.search(r"</?[a-z!]", s, re.I):
            break
    s = re.sub(r"[\x00-\x1f\x7f\u200b-\u200d\ufeff]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for _ in range(4):
        before = s
        s = strip_waitlist(s)
        s = _T_PREFIX.sub("", s).strip()
        s = _T_SUFFIX_BARE.sub("", _T_SUFFIX.sub("", s)).strip()
        s = _T_EDGE.sub("", s).strip()
        if s == before:
            break
    return re.sub(r"\s+", " ", s).strip()


def is_bad_title(title: str) -> bool:
    t = (title or "").strip()
    if len(t) < 2 or len(t) > 120 or "<" in t or ">" in t:
        return True
    if not re.search(r"[^\W\d_]", t):
        return True
    if re.fullmatch(r"[\d\s/\-:|.,]+", t):
        return True
    return re.sub(r"[!.:]+$", "", t.lower()).strip() in _T_JUNK


def clean_title(value: object) -> str:
    """Sanitized title, or '' when it must be rejected (bad_title: never auto-published)."""
    t = sanitize_title(value)
    return "" if is_bad_title(t) else t


# ─── mini selector engine (parity port of run-configured-scrapers) ──────────

def classes_match(attrs: str, classes: list[str]) -> bool:
    if not classes:
        return True
    m = re.search(r'class="([^"]*)"', attrs) or re.search(r"class='([^']*)'", attrs)
    if not m:
        return False
    el_cls = m.group(1).split()
    return all(c in el_cls for c in classes)


def _elements_with_close(html: str, tag: str, classes: list[str]) -> list[tuple[int, int, int, str]]:
    """Return (open_match_start, inner_start, close_end, attrs) for each balanced element."""
    tag_pat = tag if tag else r"[a-zA-Z][a-zA-Z0-9]*"
    open_re = re.compile(rf"<({tag_pat})(\s[^>]*)?>", re.I)
    close_scan = re.compile(rf"<(/?)({tag_pat})(\s[^>]*)?>", re.I)
    out: list[tuple[int, int, int, str]] = []
    for m in open_re.finditer(html):
        el_tag = m.group(1).lower()
        attrs = m.group(2) or ""
        if not classes_match(attrs, classes):
            continue
        depth = 1
        cm = None
        for cm in close_scan.finditer(html, m.end()):
            if cm.group(2).lower() != el_tag:
                continue
            depth += -1 if cm.group(1) else 1
            if depth == 0:
                break
        if depth != 0 or cm is None:
            continue
        out.append((m.start(), m.end(), cm.end(), attrs))
    return out


def find_by_part(html: str, part: str) -> list[tuple[int, int, int, str]]:
    if not part or "[" in part or ":" in part:
        # attribute/pseudo selectors are outside the mini-engine's tag+class subset
        # (same practical limitation as the edge fn) — report "no match" so callers
        # fall through to the next candidate.
        return []
    dot_idx = part.find(".")
    if dot_idx > 0:
        tag, classes_raw = part[:dot_idx], part[dot_idx + 1:]
    elif dot_idx == 0:
        tag, classes_raw = None, part[1:]
    else:
        tag, classes_raw = part, ""
    classes = [c for c in classes_raw.split(".") if c] if dot_idx >= 0 else []
    return _elements_with_close(html, tag, classes)


def selector_alternatives(selector: str) -> list[str]:
    """Comma-list selectors ('.a, h2.title') are tried as ordered alternatives."""
    return [a.strip() for a in (selector or "").split(",") if a.strip()]


def find_elements_spans(html: str, selector: str) -> list[tuple[int, int, int, str]]:
    """Descendant-combinator walk; returns full-span tuples of the last part.
    Comma-separated alternatives are unioned and de-duplicated."""
    seen: set[tuple[int, int, int, str]] = set()
    ordered: list[tuple[int, int, int, str]] = []
    for alt in selector_alternatives(selector):
        parts = alt.split()
        current = [(0, 0, len(html), "")]
        for part in parts:
            nxt: list[tuple[int, int, int, str]] = []
            for (_ps, _ie, pe, _pa) in current:
                parent_html = html[:pe]
                for (os_, ie, ce, attrs) in find_by_part(parent_html, part):
                    if os_ >= pe:
                        continue
                    nxt.append((os_, ie, ce, attrs))
            current = nxt
            if not current:
                break
        for span in current:
            if span not in seen:
                seen.add(span)
                ordered.append(span)
    return ordered


def slice_html(html: str, span: tuple[int, int, int, str]) -> str:
    _os, ie, ce, _pa = span
    return html[ie:ce]


def _extract_first_alt(parent_html: str, selector: str, base_url: str) -> dict:
    parts = selector.strip().split()
    parents: list[tuple[int, int, int, str]] = [(0, 0, len(parent_html), "")]
    result: dict | None = None
    for i, part in enumerate(parts):
        is_last = i == len(parts) - 1
        nxt: list[tuple[int, int, int, str]] = []
        for (ps, _ie, pe, _pa) in parents:
            scope = parent_html[:pe]
            for span in find_by_part(scope, part):
                os_, ie, ce, attrs = span
                if ps and os_ < ps:
                    continue
                if is_last:
                    inner = parent_html[ie:ce]
                    full_el = parent_html[os_:ce]
                    href_m = (re.search(r'href="([^"]*)"', attrs, re.I)
                              or re.search(r'href="([^"]*)"', full_el, re.I))
                    title_m = re.search(r'title="([^"]*)"', attrs, re.I)
                    # F1-09: never the raw `src` (lazy-load placeholders are data: URIs,
                    # `src="` also matched inside `data-src="`); rank every candidate
                    # incl. the enclosing <picture><source srcset>.
                    resolved = best_image_from_html(
                        enclosing_picture(parent_html, os_) + (full_el or f"<x {attrs}>"), base_url)
                    result = {
                        "text": collapse_text(title_m.group(1)) if title_m else html_text(inner),
                        "href": absolutize(base_url, href_m.group(1)) if href_m else "",
                        "src": resolved,
                    }
                    nxt.append(span)
                    break
                nxt.append(span)
                break
            if is_last and result:
                break
        parents = nxt
        if not parents:
            return {"text": "", "href": "", "src": ""}
    if result is not None:
        return result
    span = parents[0]
    return {"text": html_text(parent_html[span[1]:span[2]]), "href": "", "src": ""}


def extract_first(parent_html: str, selector: str, base_url: str) -> dict:
    """Port of the edge fn extractFirst: text / href / src from first match.
    Comma-list selectors try each alternative and take the first non-empty hit."""
    if not selector or not selector.strip():
        return {"text": "", "href": "", "src": ""}
    alts = selector_alternatives(selector)
    fallback = {"text": "", "href": "", "src": ""}
    for alt in alts:
        res = _extract_first_alt(parent_html, alt, base_url)
        if res["text"] or res["href"] or res["src"]:
            return res
        fallback = res
    return fallback


# ─── hardening probes (FM-3 / FM-4 / FM-5) ───────────────────────────────────

def pick_srcset_largest(srcset: str) -> str:
    best_url, best_width = "", -1
    for candidate in (srcset or "").split(","):
        bits = candidate.strip().split()
        if not bits:
            continue
        url = bits[0]
        width = 0
        if len(bits) > 1 and bits[1].endswith("w"):
            try:
                width = int(bits[1][:-1])
            except ValueError:
                width = 0
        if width >= best_width:
            best_url, best_width = url, width
    return best_url


# ─── F1-09: image candidate selection ───────────────────────────────────────
# Root cause of "scraped images never become the cover": the crawler took the
# literal `src` attribute. On lazy-loading sites that is a placeholder
# (OCCII/Thuishaven: data:image/svg+xml 1x1; Paradiso: 73x91 / 10x10 transforms;
# Melkweg: width:150 thumbnails) while the real image sits in data-src /
# data-srcset / <picture><source srcset>. The placeholder was stored in
# events.image_url, the app rendered an invisible/blurry image, and
# vertex-ai-enrich skipped the event because image_url was non-empty.
# Every candidate is now collected and ranked by its real width.

_PLACEHOLDER_RE = re.compile(
    r"(^data:|^about:|^javascript:|placeholder|/blank\.(?:gif|png)|spacer\.(?:gif|png)|"
    r"transparent\.(?:gif|png)|pixel\.(?:gif|png)|/1x1\.|loading[^/]*\.gif|lazy[-_]?placeholder|"
    r"_(?:\d|1\d)x(?:\d|1\d)_)", re.I)
_SIZE_HINT_RES = [
    re.compile(r"_(\d{2,4})x(\d{2,4})_"),                       # Craft: _1392x1740_crop
    re.compile(r"-(\d{2,4})x(\d{2,4})\.(?:jpe?g|png|webp|gif|avif)", re.I),  # WordPress
    re.compile(r"width[:=](\d{2,4})", re.I),                     # melkweg /scaled/width:450
    re.compile(r"[?&](?:w|width)=(\d{2,4})", re.I),             # imgix / datocms
    re.compile(r"-p-(\d{3,4})\.", re.I),                         # webflow responsive
]
_IMG_ATTRS = ("data-lazy-srcset", "data-srcset", "srcset", "data-lazy-src", "data-src",
              "data-original", "data-bg", "src")


def url_width_hint(url: str) -> int:
    for rx in _SIZE_HINT_RES:
        m = rx.search(url or "")
        if m:
            return int(m.group(1))
    return 0


def is_placeholder_image(url: str) -> bool:
    return not url or bool(_PLACEHOLDER_RE.search(url.strip()))


def srcset_candidates(srcset: str) -> list[tuple[str, int]]:
    """(url, score) per srcset entry: 'w' descriptor → width, 'x' → url hint or 1000·x."""
    out: list[tuple[str, int]] = []
    for candidate in re.split(r",\s+(?=\S)", (srcset or "").strip()):
        bits = candidate.strip().split()
        if not bits:
            continue
        url, score = bits[0].rstrip(","), 0
        desc = bits[1].lower() if len(bits) > 1 else ""
        try:
            if desc.endswith("w"):
                score = int(float(desc[:-1]))
            elif desc.endswith("x"):
                score = url_width_hint(url) or int(float(desc[:-1]) * 1000)
        except ValueError:
            score = 0
        out.append((url, max(score, url_width_hint(url))))
    return out


def image_candidates_from_html(fragment: str) -> list[tuple[str, int]]:
    cands: list[tuple[str, int]] = []
    for tag_m in re.finditer(r"<(?:img|source)\b([^>]*)>", fragment or "", re.I):
        attrs = tag_m.group(1)
        for name in _IMG_ATTRS:
            # (?<![\w-]) so `src=` never matches inside `data-src=`
            m = re.search(r'(?<![\w-])' + re.escape(name) + r'\s*=\s*"([^"]*)"', attrs, re.I) \
                or re.search(r"(?<![\w-])" + re.escape(name) + r"\s*=\s*'([^']*)'", attrs, re.I)
            if not m or not m.group(1).strip():
                continue
            value = unescape(m.group(1).strip())
            if name.endswith("srcset"):
                cands.extend(srcset_candidates(value))
            else:
                w_m = re.search(r'(?<![\w-])width\s*=\s*"(\d+)"', attrs, re.I)
                hint = url_width_hint(value)
                # the literal src of a lazy <img> reports the layout width, not the file's
                cands.append((value, hint or (int(w_m.group(1)) if w_m and name != "src" else 0)))
    return cands


def pick_image_candidate(cands: list[tuple[str, int]], base_url: str) -> str:
    best, best_score = "", -1
    for url, score in cands:
        if is_placeholder_image(url):
            continue
        abs_url = absolutize(base_url, url)
        if not abs_url.startswith(("http://", "https://")):
            continue
        if score > best_score:
            best, best_score = abs_url, score
    return best


def best_image_from_html(fragment: str, base_url: str) -> str:
    return pick_image_candidate(image_candidates_from_html(fragment), base_url)


def enclosing_picture(page_html: str, pos: int) -> str:
    """The <picture>…</picture> wrapping position `pos` (its <source srcset> live there)."""
    start = page_html.rfind("<picture", 0, pos)
    if start < 0 or page_html.rfind("</picture", start, pos) >= 0:
        return ""
    end = page_html.find("</picture", pos)
    return page_html[start:end if end >= 0 else pos]


def jsonld_image_url(value: object) -> str:
    """schema.org image: URL string | ImageObject{url|contentUrl} | list. Never `name`."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            url = jsonld_image_url(item)
            if url and not is_placeholder_image(url):
                return url
        return ""
    if isinstance(value, dict):
        for key in ("url", "contentUrl", "@id", "thumbnailUrl"):
            url = jsonld_image_url(value.get(key))
            if url:
                return url
    return ""


def probe_image(container: str, base_url: str) -> str:
    return best_image_from_html(container, base_url)


def probe_link(container: str, base_url: str) -> str:
    for a_m in re.finditer(r"<a\b([^>]*)>", container, re.I):
        href_m = re.search(r'href="([^"]*)"', a_m.group(1), re.I)
        if href_m:
            url = absolutize(base_url, href_m.group(1))
            if url.startswith("http"):
                return url
    return ""


# Same Europe/Amsterdam rule as supabase/functions/_shared/extract/datetime.ts:
# CEST (+02:00) from the last Sunday of March 01:00 UTC to the last Sunday of
# October 01:00 UTC, CET (+01:00) otherwise. No tz database needed.
_ISO_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})?", re.I)


def _last_sunday(year: int, month: int) -> int:
    nxt = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
    last = nxt - timedelta(days=1)
    return last.day - (last.weekday() + 1) % 7


def amsterdam_offset_minutes(utc: datetime) -> int:
    y = utc.year
    start = datetime(y, 3, _last_sunday(y, 3), 1, tzinfo=timezone.utc)
    end = datetime(y, 10, _last_sunday(y, 10), 1, tzinfo=timezone.utc)
    return 120 if start <= utc < end else 60


def iso_local_hhmm(text: str) -> str:
    """HH:MM in Amsterdam local time from an ISO datetime; '' if none. Zone-less = already local."""
    m = _ISO_RE.search(text or "")
    if not m:
        return ""
    y, mo, d, hh, mi, zone = m.groups()
    if not zone:
        return f"{hh}:{mi}"
    if zone.upper() == "Z":
        off = 0
    else:
        zm = re.match(r"([+-])(\d{2}):?(\d{2})", zone)
        off = (-1 if zm.group(1) == "-" else 1) * (int(zm.group(2)) * 60 + int(zm.group(3)))
    try:
        utc = datetime(int(y), int(mo), int(d), int(hh), int(mi), tzinfo=timezone.utc) - timedelta(minutes=off)
    except ValueError:
        return ""
    local = utc + timedelta(minutes=amsterdam_offset_minutes(utc))
    return local.strftime("%H:%M")


def probe_time_text(text: str) -> str:
    """Extract a time-of-day token; colon/h-suffix forms only (dots look like dates).
    ISO datetimes are converted to Amsterdam local time (a naive '\\d:\\d' search on
    '2026-09-12T23:00:00+02:00' used to return the offset's '00:00')."""
    if not text:
        return ""
    iso = iso_local_hhmm(text)
    if iso:
        return iso
    # 12-hour clock ("10pm", "9:30 pm", "9.30 p.m."): normalised to 24 h, checked before the plain HH:MM form so "9:30 pm" is 21:30.
    m = re.search(r"\b(\d{1,2})(?:[:.](\d{2}))?\s*([ap])\.?m\b", text, re.I)
    if m:
        hh, mi = int(m.group(1)), int(m.group(2) or 0)
        if 1 <= hh <= 12 and mi <= 59:
            hh = hh % 12 + (12 if m.group(3).lower() == "p" else 0)
            return f"{hh:02d}:{mi:02d}"
    m = re.search(r"\b(\d{1,2}:\d{2})\b", text)
    if m:
        return m.group(1)
    # Dutch "22u", "22 uur", "22u30": hours, optional minutes.
    m = re.search(r"\b(\d{1,2})\s*(?:u|uur)\s?(\d{2})?(?![a-z\d])", text, re.I)
    if m and int(m.group(1)) <= 23 and int(m.group(2) or 0) <= 59:
        return f"{int(m.group(1)):02d}:{m.group(2) or '00'}"
    m = re.search(r"\b(\d{1,2})\s*h\b", text, re.I)
    if m:
        return f"{m.group(1)}h"
    return ""


def probe_time_attr(container: str) -> str:
    """Time embedded in a <time datetime=...> attribute of the container."""
    m = re.search(r'<time\b[^>]*\bdatetime="([^"]*)"', container, re.I)
    if m:
        return probe_time_text(unescape(m.group(1)))
    return ""


# ─── F1-07 evidence fields (optional, backward compatible) ───────────────────
# The webhook's deterministic extractor (supabase/functions/_shared/extract) does the
# date/time/venue logic; the crawler only forwards raw evidence so its validators can
# check that every extracted value literally appears in the source:
#   source_text    visible text of the event card (bounded)
#   raw_time_text  the full time line ("open 22:00 — start 23:00"), so the webhook can
#                  tell doors from start; raw_time_string stays the legacy HH:MM token
#   jsonld         the JSON-LD Event node itself (small ones only)
SOURCE_TEXT_MAX = 1500
DETAIL_MAX = 120   # detail pages fetched per source and run (politeness + run time)
JSONLD_MAX = 8000


def evidence_text(*parts: object) -> str:
    seen: list[str] = []
    for part in parts:
        t = collapse_text(as_string(part))
        if t and t not in seen:
            seen.append(t)
    return " | ".join(seen)[:SOURCE_TEXT_MAX]


# ─── JSON-LD (preferred path, port of parseJsonLdEvents) ─────────────────────

def parse_jsonld_events(page_html: str, base_url: str, source_label: str) -> list[dict]:
    events: list[dict] = []
    script_re = re.compile(
        r"<script\b[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        re.I | re.S,
    )
    for raw in script_re.findall(page_html):
        try:
            parsed = json.loads(raw.strip().replace("<!--", "").replace("-->", ""))
        except json.JSONDecodeError:
            continue
        queue = list(parsed) if isinstance(parsed, list) else [parsed]
        while queue:
            item = queue.pop(0)
            if isinstance(item, list):
                queue.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                queue.extend(graph)
            types = as_string(item.get("@type")).lower().split()
            if not any(t in ("event", "musicevent", "musicfestival", "festival", "businessevent")
                       for t in types):
                continue
            title = clean_title(as_string(item.get("name")))  # D-T04-2
            start = as_string(item.get("startDate"))
            if not title or not start:
                continue
            location = item.get("location") if isinstance(item.get("location"), dict) else {}
            address = location.get("address") if isinstance(location, dict) else {}
            if isinstance(address, dict):
                address_text = ", ".join(as_string(address.get(k)) for k in
                                         ("streetAddress", "postalCode", "addressLocality",
                                          "addressCountry"))
            else:
                address_text = as_string(address)
            raw_time = probe_time_text(start)
            # ISO startDate with 'T' already carries the exact time inside
            # raw_date_string (the webhook honors it and skips setHours); the
            # probed token is informational and covers date-only startDates
            # whose page text still shows a clock time.
            events.append({
                "source": source_label,
                "title": title[:255],
                "raw_date_string": start,
                "raw_time_string": raw_time,
                "end_date_string": as_string(item.get("endDate")),
                "venue_name": as_string(location.get("name")) or as_string(location.get("address")) or venue_fallback(source_label),
                "address": collapse_text(address_text),
                "image_url": pick_image_candidate([(jsonld_image_url(item.get("image")), 0)], base_url),
                "external_url": absolutize(base_url, as_string(item.get("url")) or as_string(item.get("@id"))),
                "description": as_string(item.get("description"))[:4000],
                **({"jsonld": item} if len(json.dumps(item, ensure_ascii=False)) <= JSONLD_MAX else {}),
            })
    return events


# ─── heuristic fallback (port of discoverEventContainers, 9 candidates) ──────

def discover_event_containers(page_html: str) -> list[tuple[int, int, int, str]]:
    for selector in HEURISTIC_CANDIDATES:
        spans = find_elements_spans(page_html, selector)
        if 1 <= len(spans) <= 200:
            return spans
    return []


# ─── default schema packs ────────────────────────────────────────────────────

def load_packs() -> list[dict]:
    try:
        with open(SCHEMAS_PATH, encoding="utf-8") as fh:
            return json.load(fh).get("packs", [])
    except (OSError, json.JSONDecodeError) as exc:
        print(f"::warning::source_schemas.json unavailable: {exc}", file=sys.stderr)
        return []


def find_pack(packs: list[dict], name: str, site_name: str, url: str) -> dict | None:
    n = (name or "").lower()
    sn = (site_name or "").lower()
    u = (url or "").lower()
    for pack in packs:
        match = pack.get("match", {})
        if any(s and (s in n or s in sn) for s in match.get("name", [])):
            return pack
        if any(s and s in u for s in match.get("url", [])):
            return pack
    return None


def crawl_schema_from(selectors: dict) -> dict | None:
    """Map DB-style selectors to a Crawl4AI JsonCssExtractionStrategy schema."""
    base = (selectors or {}).get("event_container")
    if not base:
        return None
    fields: list[dict] = []
    text_fields = {
        "title": "title",
        "date": "raw_date_string",
        "location": "venue_name",
        "description": "description",
    }
    for key, field_name in text_fields.items():
        sel = (selectors or {}).get(key)
        if sel:
            fields.append({"name": field_name, "selector": sel, "type": "text"})
    img_sel = (selectors or {}).get("image") or "img"
    fields.append({"name": "image", "selector": img_sel, "type": "attribute", "attribute": "src"})
    fields.append({"name": "image_srcset", "selector": img_sel, "type": "attribute",
                   "attribute": "srcset"})
    # F1-09: lazy-load attributes + the whole <picture>/<img> markup, so the real
    # file (data-src / data-srcset / <source srcset>) beats the placeholder src.
    for attr in ("data-src", "data-srcset", "data-lazy-src", "data-lazy-srcset"):
        fields.append({"name": "image_" + attr.replace("-", "_"), "selector": img_sel,
                       "type": "attribute", "attribute": attr})
    fields.append({"name": "image_html", "selector": "picture", "type": "html"})
    fields.append({"name": "image_img_html", "selector": img_sel, "type": "html"})
    link_sel = (selectors or {}).get("link") or "a"
    fields.append({"name": "link", "selector": link_sel, "type": "attribute", "attribute": "href"})
    # Round-2 hard requirement: every source must yield a real start time, not the
    # webhook's 21:00 default. time_text prefers an explicit 'time' selector, then
    # the 'date' selector's text (many venues embed HH:MM in the date line), then
    # the generic <time> element.
    time_sel = (selectors or {}).get("time") or (selectors or {}).get("date") or "time"
    fields.append({"name": "time_text", "selector": time_sel, "type": "text"})
    fields.append({"name": "time_attr", "selector": "time", "type": "attribute",
                   "attribute": "datetime"})
    # Round-2 E2E fallbacks: melkweg renders <time datetime>/<itemprop>, nightlife
    # ticket sites put HH:MM in class*="time" divs. probe_time_text validates the
    # result, so these can only help, never corrupt.
    fields.append({"name": "time_attr_iso", "selector": "[datetime]", "type": "attribute",
                   "attribute": "datetime"})
    fields.append({"name": "time_itemprop", "selector": "[itemprop='startDate'], [itemprop='endDate']",
                   "type": "attribute", "attribute": "content"})
    fields.append({"name": "time_class", "selector": "[class*='time' i]", "type": "text"})
    return {"baseSelector": base, "fields": fields}


# ─── candidate assembly ──────────────────────────────────────────────────────

# Page-level title → ISO-startTime pairs mined from embedded JSON blobs
# (Next.js __NEXT_DATA__, Remix serializers, …). Sites like melkweg.nl render
# times ONLY inside these JSON islands, invisible to CSS selectors and ld+json.
_page_time_lookup: dict[str, str] = {}


def build_page_time_lookup(page_html: str) -> None:
    """Mine (title, startDate) pairs embedded in the page's JSON islands."""
    global _page_time_lookup
    _page_time_lookup = {}
    if not page_html:
        return
    pairs: dict[str, str] = {}

    def add(title: str, iso: str) -> None:
        t = unescape(title).strip().lower()
        if t and len(t) >= 3:
            pairs.setdefault(t, iso)

    title_re = r'"(?:title|name)"\s*:\s*"((?:[^"\\]|\\.)*)"'
    # keep seconds/fraction/zone: melkweg serializes UTC ("...T17:00:00.000000Z")
    start_re = r'"(?:startDate|start_date)"\s*:\s*"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)'
    for m in re.finditer(title_re + r"[\s\S]{0,600}?" + start_re, page_html):
        add(m.group(1), m.group(2))
    for m in re.finditer(start_re + r'"[\s\S]{0,600}?' + title_re, page_html):
        add(m.group(2), m.group(1))
    _page_time_lookup = pairs


def lookup_page_time(title: str) -> str:
    if not _page_time_lookup:
        return ""
    # Exact match only: loose substring matching caused cross-event collisions
    # (a midnight-start JSON entry poisoning unrelated candidates).
    return _page_time_lookup.get((title or "").strip().lower(), "")


def dedupe(events: list[dict]) -> list[dict]:
    seen_urls: set[str] = set()
    seen_titles: set[tuple[str, str]] = set()
    out: list[dict] = []
    for ev in events:
        url = ev.get("external_url") or ""
        key = (ev.get("title") or "", ev.get("raw_date_string") or "")
        if url and url in seen_urls:
            continue
        if key in seen_titles:
            continue
        if url:
            seen_urls.add(url)
        seen_titles.add(key)
        out.append(ev)
    return out


def candidate_from_row(row: dict, base_url: str, source_label: str) -> dict | None:
    title = clean_title(as_string(row.get("title")))  # D-T04-2: bad_title -> rejected
    if not title:
        return None
    raw_date = collapse_text(as_string(row.get("raw_date_string")))
    image = best_image_from_html(as_string(row.get("image_html")) + as_string(row.get("image_img_html")),
                                 base_url)
    if not image:
        cands: list[tuple[str, int]] = []
        for key in ("image_data_lazy_srcset", "image_data_srcset", "image_srcset"):
            cands.extend(srcset_candidates(as_string(row.get(key))))
        for key in ("image_data_lazy_src", "image_data_src", "image"):
            url = as_string(row.get(key))
            if url:
                cands.append((url, url_width_hint(url)))
        image = pick_image_candidate(cands, base_url)
    link = absolutize(base_url, as_string(row.get("link")))
    if not link.startswith("http"):
        link = ""
    # Normalize to a bare time-of-day token: "start 22:30" → "22:30",
    # ISO datetime attr "2026-10-02T22:00:00+02:00" → "22:00". The webhook's
    # parseTimeString consumes HH:MM / HH.MM / HHh; anything else → its 21:00 default.
    raw_time = (probe_time_text(as_string(row.get("time_text")))
                or probe_time_text(as_string(row.get("time_attr")))
                or probe_time_text(raw_date)
                or probe_time_text(lookup_page_time(title))
                or probe_time_text(as_string(row.get("time_attr_iso")))
                or probe_time_text(as_string(row.get("time_itemprop")))
                or probe_time_text(as_string(row.get("time_class"))))
    raw_time_text = collapse_text(as_string(row.get("time_text")) or as_string(row.get("time_class")))[:300]
    return {
        "source": source_label,
        "title": title[:255],
        "raw_date_string": raw_date,
        "raw_time_string": raw_time,
        "raw_time_text": raw_time_text,
        "venue_name": collapse_text(as_string(row.get("venue_name"))) or venue_fallback(source_label),
        "image_url": image,
        "external_url": link,
        "description": as_string(row.get("description"))[:4000],
        "source_text": evidence_text(title, raw_date, row.get("time_text"), row.get("time_attr"),
                                     row.get("time_attr_iso"), row.get("time_itemprop"),
                                     row.get("time_class"), lookup_page_time(title),
                                     row.get("venue_name"), as_string(row.get("description"))[:500]),
    }


def extract_via_heuristic(page_html: str, base_url: str, source_label: str,
                          selectors: dict) -> list[dict]:
    """Last-resort path: same discovery + probes; never worse than the edge fn."""
    events: list[dict] = []
    if selectors.get("event_container"):
        spans = find_elements_spans(page_html, selectors["event_container"])
    else:
        spans = discover_event_containers(page_html)
    for span in spans[:200]:
        container = slice_html(page_html, span)
        title_el = (extract_first(container, selectors["title"], base_url)
                    if selectors.get("title") else {"text": "", "href": "", "src": ""})
        title = clean_title(title_el["text"] or html_text(container[:300]))  # D-T04-2
        if not title:
            continue
        date_text = (extract_first(container, selectors["date"], base_url)["text"]
                     if selectors.get("date") else "")
        loc_text = (extract_first(container, selectors["location"], base_url)["text"]
                    if selectors.get("location") else "")
        desc_text = (extract_first(container, selectors["description"], base_url)["text"]
                     if selectors.get("description") else "")
        image = (extract_first(container, selectors["image"], base_url)["src"]
                 if selectors.get("image") else "") or probe_image(container, base_url)
        link = (extract_first(container, selectors["link"], base_url)["href"]
                if selectors.get("link") else "") or probe_link(container, base_url)
        time_text = (probe_time_text(date_text) or probe_time_attr(container)
                     or probe_time_text(html_text(container)))
        events.append({
            "source": source_label,
            "title": title[:255],
            "raw_date_string": collapse_text(date_text),
            "raw_time_string": time_text,
            "raw_time_text": collapse_text(date_text)[:300],
            "venue_name": collapse_text(loc_text) or venue_fallback(source_label),
            "image_url": image,
            "external_url": link,
            "description": desc_text[:4000],
            "source_text": html_text(container)[:SOURCE_TEXT_MAX],
        })
    return events


# ─── Supabase REST + webhook IO ──────────────────────────────────────────────

def rest_request(url: str, method: str, service_role: str, body: object | None = None) -> tuple[int, object]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "apikey": service_role,
        "Authorization": f"Bearer {service_role}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8", errors="replace"))
        except Exception:
            return exc.code, None
    except urllib.error.URLError as exc:
        print(f"::warning::REST {method} {url} failed: {exc.reason}", file=sys.stderr)
        return 0, None


def check_automation_enabled(supabase_url: str, service_role: str) -> bool:
    status, body = rest_request(
        f"{supabase_url}/rest/v1/rpc/is_automation_enabled", "POST", service_role, {})
    if status == 0:
        return True  # unreachable → fail open, same as edge fn
    return body is True


def fetch_sources(supabase_url: str, service_role: str) -> list[dict]:
    path = ("scraping_sources?is_active=eq.true&method=eq.html_scrape"
            "&select=id,site_name,name,url,selectors,consecutive_zero_count,auto_approve")
    status, body = rest_request(f"{supabase_url}/rest/v1/{path}", "GET", service_role)
    if status != 200 or not isinstance(body, list):
        raise RuntimeError(f"scraping_sources REST query returned HTTP {status}")
    return body


class Backend:
    """Where the crawler reads its source list and writes outcomes.
    Direct mode: Supabase REST with the service role key (private repo). Gateway mode: the scoped scraper-gateway edge function with the
    webhook secret only (public repo: the service role key never leaves Supabase)."""

    def __init__(self, supabase_url: str, service_role: str, gateway_url: str, secret: str):
        self.supabase_url, self.service_role, self.gateway_url, self.secret = supabase_url, service_role, gateway_url, secret

    @property
    def gateway(self) -> bool:
        return not self.service_role and bool(self.gateway_url)

    def _gw(self, payload: dict) -> tuple[int, object]:
        req = urllib.request.Request(self.gateway_url, data=json.dumps(payload).encode("utf-8"), method="POST", headers={
            "Content-Type": "application/json", "X-Webhook-Secret": self.secret, "User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            return exc.code, None
        except urllib.error.URLError as exc:
            print(f"::warning::gateway {payload.get('action')} failed: {exc.reason}", file=sys.stderr)
            return 0, None

    def automation_enabled(self) -> bool:
        if self.gateway:
            status, body = self._gw({"action": "automation"})
            return True if status == 0 else bool(isinstance(body, dict) and body.get("enabled") is True)
        return check_automation_enabled(self.supabase_url, self.service_role)

    def sources(self) -> list[dict]:
        if self.gateway:
            status, body = self._gw({"action": "sources"})
            if status != 200 or not isinstance(body, dict) or not isinstance(body.get("sources"), list):
                raise RuntimeError(f"scraper-gateway sources returned HTTP {status}")
            return body["sources"]
        return fetch_sources(self.supabase_url, self.service_role)

    def source_status(self, source_id: str, fields: dict) -> None:
        if self.gateway:
            self._gw({"action": "source_status", "id": source_id, **fields})
        else:
            rest_request(f"{self.supabase_url}/rest/v1/scraping_sources?id=eq.{source_id}", "PATCH", self.service_role,
                         {"last_scraped_at": datetime.now(timezone.utc).isoformat(), **fields})

    def run_log(self, row: dict) -> None:
        if self.gateway:
            self._gw({"action": "run_log", **row})
        else:
            rest_request(f"{self.supabase_url}/rest/v1/scraping_layer2_runs", "POST", self.service_role, row)


ZERO_ALERT_AFTER = int(os.environ.get("OUTLIST_ZERO_ALERT_AFTER", "3"))  # S4: sources with 0 events this many runs in a row are reported
WEBHOOK_BATCH = int(os.environ.get("OUTLIST_WEBHOOK_BATCH", "25"))      # events per POST (S4: melkweg sent 270 in one call and timed out)
WEBHOOK_TIMEOUT_S = int(os.environ.get("OUTLIST_WEBHOOK_TIMEOUT", "120"))  # seconds per POST


def next_zero_count(events_found: int, previous: int | None) -> int:
    """Consecutive runs with 0 events: reset by any event, +1 otherwise (S4; tested in tests/s9/zero_alert.test.ts)."""
    return 0 if events_found else (previous or 0) + 1


def write_step_summary(ran: int, failed_sources: list[str], silent: list[str]) -> None:
    """Markdown report on the GitHub run page (GITHUB_STEP_SUMMARY). No-op outside Actions."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = [f"### Layer-2 run: {ran} source(s), {len(failed_sources)} failed, {len(silent)} silent", ""]
    if failed_sources:
        lines += ["**Failed**", *[f"- {x}" for x in failed_sources], ""]
    if silent:
        lines += [f"**Silent (0 events for {ZERO_ALERT_AFTER}+ runs in a row, probably a changed page)**", *[f"- {x}" for x in silent], ""]
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except OSError:
        pass


def post_webhook(webhook_url: str, secret: str, events: list[dict],
                 auto_approve: bool, timeout: int = 60) -> tuple[int, str]:
    req = urllib.request.Request(webhook_url, data=json.dumps(events).encode("utf-8"),
                                 method="POST", headers={
                                     "Content-Type": "application/json",
                                     "X-Webhook-Secret": secret,
                                     "X-Auto-Approve": "true" if auto_approve else "false",
                                     "User-Agent": UA,
                                 })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def post_webhook_batched(webhook_url: str, secret: str, events: list[dict], auto_approve: bool,
                         batch: int = WEBHOOK_BATCH, timeout: int = WEBHOOK_TIMEOUT_S,
                         poster=post_webhook) -> tuple[bool, str]:
    """Send events in batches so one slow webhook call cannot sink a whole source.

    Each batch is retried once (the webhook dedupes, so a batch that actually finished on the server is answered
    with skipped_duplicates on the retry). Returns (all_batches_ok, one-line summary with summed counters).
    """
    totals = {"inserted": 0, "skipped_duplicates": 0, "filtered": 0, "merged": 0}
    failed: list[str] = []
    size = max(1, batch)
    n_batches = (len(events) + size - 1) // size
    for b in range(n_batches):
        chunk = events[b * size:(b + 1) * size]
        last_err = ""
        for attempt in (1, 2):
            try:
                status, body = poster(webhook_url, secret, chunk, auto_approve, timeout=timeout)
            except Exception as exc:  # timeout, connection reset
                last_err = f"batch {b + 1}/{n_batches} attempt {attempt}: {exc}"
                continue
            if 200 <= status < 300:
                try:
                    data = json.loads(body)
                    for k in totals:
                        totals[k] += int(data.get(k) or 0)
                except (ValueError, TypeError, AttributeError):
                    pass
                last_err = ""
                break
            last_err = f"batch {b + 1}/{n_batches} attempt {attempt}: HTTP {status} {body[:120]}"
        if last_err:
            failed.append(last_err)
    summary = (f"{len(events)} events in {n_batches} batch(es): inserted={totals['inserted']} "
               f"skipped_duplicates={totals['skipped_duplicates']} filtered={totals['filtered']} merged={totals['merged']}")
    if failed:
        summary += f" FAILED {len(failed)}/{n_batches}: " + " | ".join(failed[:3])
    return not failed, summary


# ─── Crawl4AI (lazy import so --validate runs without it) ────────────────────

BLOCK_RETRY_WAIT_S = 8


def is_blocked_page(status: int, html: str) -> bool:
    """True when the crawl got a block / challenge / error page instead of the agenda (BUG-F20261118-4).
    Such a page used to be parsed as an empty agenda and logged as a success with 0 events (Partyflock 403, Bitterzoet Cloudflare)."""
    if status in (403, 429) or status >= 500 or status == 0:
        return True
    if not html or len(html) < 600:
        return True
    head = html[:4000].lower()
    return "just a moment" in head and ("cloudflare" in html.lower() or "challenge" in head or "turnstile" in html.lower() or len(html) < 20000)


async def crawl_many(urls: list[str], retry_blocked: bool = True) -> dict[str, tuple[int, str]]:
    """Render all URLs with one shared headless Chromium. Returns url → (status, html).
    A URL that came back blocked is tried once more after a pause (a 403 from Partyflock is often transient)."""
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

    browser_cfg = BrowserConfig(headless=True)
    run_cfg = CrawlerRunConfig(
        wait_for="css:body",
        page_timeout=CRAWL_TIMEOUT_MS,
        magic=True,
        verbose=False,
    )
    results: dict[str, tuple[int, str]] = {}
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        for url in urls:
            try:
                res = await crawler.arun(url=url, config=run_cfg)
                html = getattr(res, "html", "") or getattr(res, "cleaned_html", "") or ""
                status = int(getattr(res, "status_code", 0) or 0)
                if not html and getattr(res, "success", False):
                    status = status or 200
                results[url] = (status, html)
            except Exception as exc:  # per-source isolation
                print(f"::warning::crawl failed for {url}: {exc}", file=sys.stderr)
                results[url] = (0, "")
        for url in [u for u, (st, h) in results.items() if retry_blocked and is_blocked_page(st, h)]:
            await asyncio.sleep(BLOCK_RETRY_WAIT_S)
            try:
                res = await crawler.arun(url=url, config=run_cfg)
                html = getattr(res, "html", "") or getattr(res, "cleaned_html", "") or ""
                status = int(getattr(res, "status_code", 0) or 0)
                if not html and getattr(res, "success", False):
                    status = status or 200
                if not is_blocked_page(status, html):
                    print(f"[crawl] {url}: blocked on the first try (HTTP {results[url][0]}), fine on the retry")
                    results[url] = (status, html)
            except Exception as exc:
                print(f"::warning::crawl retry failed for {url}: {exc}", file=sys.stderr)
    return results


def css_extract(page_html: str, schema: dict) -> list[dict]:
    try:
        from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
        rows = JsonCssExtractionStrategy(schema=schema, verbose=False).run(page_html)
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        print(f"::warning::JsonCssExtractionStrategy failed, falling back to heuristic: {exc}",
              file=sys.stderr)
        return []


# ─── agenda lists with the day in a heading + detail pages for the time (Melkweg) ──
# Melkweg's /agenda shows the DAY once (<h2><time datetime="2026-10-21">) and the events below it, with no
# clock time; the times ("18:00 Doors / 20:30 <act>") only exist on each event page. Without them every event
# got a placeholder 21:00 (60 public events with 2 distinct start instants, F8). The list parser yields the
# event with its date; enrich_with_details() then adds the program lines from the detail page as evidence.

def parse_day_grouped_list(page_html: str, base_url: str, source_label: str) -> list[dict]:
    events: list[dict] = []
    for part in re.split(r"(?=<h2\b[^>]*event-list-day__title)", page_html)[1:]:
        head = re.match(r"<h2\b[^>]*>(.*?)</h2>(.*)", part, re.S)
        if not head:
            continue
        days = re.findall(r'<time\b[^>]*datetime="(\d{4}-\d{2}-\d{2})', head.group(1))
        if len(days) != 1:        # multi-day group (exhibitions): no single date to state
            continue
        for a in re.finditer(r"<a\b([^>]*)>(.*?)</a>", head.group(2), re.S):
            attrs, inner = a.group(1), a.group(2)
            href = re.search(r'\bhref="([^"]+)"', attrs)
            title = re.search(r"<h3\b[^>]*>(.*?)</h3>", inner, re.S)
            if not href or not title or "event-compact" not in attrs:
                continue
            events.append({
                "source": source_label,
                "title": clean_title(title.group(1))[:255],
                "raw_date_string": days[0],
                "raw_time_string": "",
                "venue_name": source_label,
                "image_url": best_image_from_html(inner, base_url),
                "external_url": absolutize(base_url, unescape(href.group(1))),
                "description": "",
                "source_text": evidence_text(html_text(title.group(1)), days[0]),
            })
    return events


# ─── club list parsers (S7, BUG-F20261110-43) ───────────────────────────────
# Pack key "list_parser": day_grouped (Melkweg), microdata_events (Partyflock), paradiso_by_date (Paradiso), dotted_date_list (Warehouse
# Elementenstraat). The last three are exclusive: the pack selectors are not used on top of them (they only matched menu items).

def _md_meta(block: str, prop: str) -> str:
    m = re.search(r'<meta\b[^>]*\bitemprop="' + prop + r'"[^>]*\bcontent="([^"]*)"', block)
    return collapse_text(m.group(1)) if m else ""


_MD_EVENT_START = re.compile(r'(?=<[a-z]+\b[^>]*itemtype="https?://schema\.org/Event")')


def parse_microdata_events(page_html: str, base_url: str, source_label: str, city: str = "") -> list[dict]:
    """schema.org Event microdata (itemscope itemtype=.../Event), e.g. Partyflock's agenda. `city` keeps only events whose venue
    addressLocality matches (Partyflock's agenda is national)."""
    events: list[dict] = []
    for part in _MD_EVENT_START.split(page_html)[1:]:
        start = _md_meta(part, "startDate")
        name = re.search(r'<[a-z]+\b[^>]*\bitemprop="name"[^>]*>(.*?)</', part, re.S)
        if not start or not name:
            continue
        venue_block = re.search(r'itemtype="https?://schema\.org/EventVenue"(.*)', part, re.S)
        venue = _md_meta(venue_block.group(1), "name") if venue_block else ""
        locality = _md_meta(part, "addressLocality")
        if city and locality.lower() != city.lower():
            continue
        title = clean_title(name.group(1))[:255]
        if not title:
            continue
        url = _md_meta(part, "url")
        if not url:
            m = re.search(r'<a\b[^>]*\bhref="([^"]+)"', part)
            url = m.group(1) if m else ""
        events.append({
            "source": source_label,
            "title": title,
            "raw_date_string": start,
            "raw_time_string": "",
            "venue_name": venue or venue_fallback(source_label),
            "image_url": "",
            "external_url": absolutize(base_url, unescape(url)),
            "description": "",
            "source_text": evidence_text(html_text(name.group(1)), start, venue),
        })
    return events


_MONTHS_EN = {m: i + 1 for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_PARADISO_DAY = re.compile(r'>\s*(?:Mo|Tu|We|Th|Fr|Sa|Su)\s+(\d{1,2})\s+([A-Z][a-z]{2})\s*<')
_PARADISO_ITEM = re.compile(r'<a\b[^>]*\bhref="(/(?:en|nl)/(?:program|programma)/[^"]+/\d+)"[^>]*>(.*?)</a>', re.S)


def parse_paradiso_by_date(page_html: str, base_url: str, source_label: str, now: datetime | None = None) -> list[dict]:
    """Paradiso programme ("By date" list): day headings like 'Tu 10 Nov', then one link per event (h2 title, p description, time span).
    The page has hashed class names, so the parser keys on the heading text and the /program/<slug>/<id> link shape."""
    now = now or datetime.now(timezone.utc)
    events: list[dict] = []
    seen: set[str] = set()
    tokens = sorted([(m.start(), "day", m) for m in _PARADISO_DAY.finditer(page_html)] +
                    [(m.start(), "item", m) for m in _PARADISO_ITEM.finditer(page_html)], key=lambda t: t[0])
    day_iso = ""
    for _, kind, m in tokens:
        if kind == "day":
            month = _MONTHS_EN.get(m.group(2).lower())
            day = int(m.group(1))
            if not month or not 1 <= day <= 31:
                day_iso = ""
                continue
            year = now.year
            try:
                if datetime(year, month, day, tzinfo=timezone.utc) < now - timedelta(days=60):
                    year += 1
                day_iso = f"{year:04d}-{month:02d}-{day:02d}"
                datetime(year, month, day)
            except ValueError:
                day_iso = ""
            continue
        inner = m.group(2)
        title = re.search(r"<h2\b[^>]*>(.*?)</h2>", inner, re.S)
        if not day_iso or not title:
            continue
        href = absolutize(base_url, unescape(m.group(1)))
        if href in seen:
            continue
        seen.add(href)
        desc = re.search(r"<p\b[^>]*>(.*?)</p>", inner, re.S)
        desc_text = html_text(desc.group(1)) if desc else ""
        time_m = re.search(r"<span\b[^>]*>\s*(\d{1,2}:\d{2})\s*</span>", inner)
        venue = source_label
        loc = re.search(r"\s/\s*([^/]{2,40})$", desc_text)   # "Brand-new post-punk band / Tolhuistuin"
        if loc:
            venue = loc.group(1).strip()
        t = clean_title(title.group(1))[:255]
        if not t:
            continue
        events.append({
            "source": source_label,
            "title": t,
            "raw_date_string": day_iso,
            "raw_time_string": time_m.group(1) if time_m else "",
            "venue_name": venue,
            "image_url": best_image_from_html(inner, base_url),
            "external_url": href,
            "description": clean_description(desc_text) if desc_text else "",
            "source_text": evidence_text(html_text(title.group(1)), day_iso, time_m.group(1) if time_m else ""),
        })
    return events


def parse_dotted_date_list(page_html: str, base_url: str, source_label: str) -> list[dict]:
    """Webflow list used by Warehouse Elementenstraat: .cl_event-list_item with the title in .is-event, the date as separate
    'DD . MM . YYYY' cells, 'HH:MM - HH:MM', the line-up and a ticket link."""
    events: list[dict] = []
    for part in re.split(r'(?=<div\b[^>]*class="[^"]*\bcl_event-list_item\b)', page_html)[1:]:
        part = part.split("</a></div>", 1)[0]
        title = re.search(r'<div\b[^>]*class="[^"]*\bis-event\b[^"]*"[^>]*>(.*?)</div>', part, re.S)
        date_block = re.search(r'class="event-list_date"[^>]*>(.*?)</div></div>', part, re.S)
        if not title or not date_block:
            continue
        nums = re.findall(r">\s*(\d{1,4})\s*<", date_block.group(0))
        if len(nums) < 3:
            continue
        d, m, y = int(nums[0]), int(nums[1]), int(nums[2])
        if not (1 <= d <= 31 and 1 <= m <= 12 and 2000 <= y <= 2100):
            continue
        after = part[part.find(date_block.group(0)) + len(date_block.group(0)):]
        times = re.findall(r"(?<![\d:])(\d{1,2}:\d{2})(?![\d:])", html_text(re.sub(r"<svg[\s\S]*?</svg>", " ", after)))
        href = re.search(r'<a\b[^>]*\bhref="([^"]+)"', part)
        t = clean_title(title.group(1))[:255]
        if not t:
            continue
        day_iso = f"{y:04d}-{m:02d}-{d:02d}"
        events.append({
            "source": source_label,
            "title": t,
            "raw_date_string": day_iso,
            "raw_time_string": times[0] if times else "",
            "venue_name": source_label,
            "image_url": "",
            "external_url": absolutize(base_url, unescape(href.group(1))) if href else "",
            "description": "",
            "source_text": evidence_text(html_text(title.group(1)), day_iso, times[0] if times else ""),
        })
    return events


EXCLUSIVE_LIST_PARSERS = {"microdata_events", "paradiso_by_date", "dotted_date_list"}


_PROGRAM_LINE = re.compile(r"^(\d{1,2}[:.]\d{2})\s+(.{2,80})$")
_DOORS_WORDS = re.compile(r"\b(doors?|deuren|open(ing)?)\b", re.I)


def program_from_detail(detail_html: str, title: str) -> list[tuple[str, str]]:
    """[(HH:MM, label)] lines of an event page's program block ('18:00 Doors', '20:30 Mind Enterprises')."""
    body = re.sub(r"<(script|style)\b.*?</\1>", " ", detail_html, flags=re.S | re.I)
    out: list[tuple[str, str]] = []
    for node in re.findall(r">([^<>]{3,400})<", body):
        for line in unescape(node).splitlines():
            m = _PROGRAM_LINE.match(collapse_text(line))
            if m:
                out.append((m.group(1).replace(".", ":"), m.group(2).strip()))
    return out


# ─── detail-page description + og:image (module 2D; mirrors _shared/extract/describe.ts) ──────────────
MAX_DESCRIPTION = 600
_BOILERPLATE = re.compile(r"(cookie|consent|privacy|accept all|javascript is|enable javascript|subscribe to|newsletter|all rights reserved|copyright|log ?in|sign ?up|terms (?:and|&) conditions|skip to)", re.I)


def clean_description(raw: object, limit: int = MAX_DESCRIPTION) -> str:
    text = html_text(re.sub(r"<(script|style|noscript)\b.*?</\1>", " ", str(raw or ""), flags=re.S | re.I))
    if len(text) <= limit:
        return text
    cut = text[:limit]
    sentence = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if sentence >= limit * 0.5:
        return cut[:sentence + 1].strip()
    word = cut.rfind(" ")
    return re.sub(r"[,;:\-\s]+$", "", cut[:word] if word > 0 else cut) + "…"


def _usable_description(text: str) -> bool:
    return len(text) >= 25 and not _BOILERPLATE.search(text[:120])


def _meta_content(page_html: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for tag in re.findall(r"<meta\b[^>]*>", page_html, re.I):
        key = re.search(r"\b(?:property|name|itemprop)\s*=\s*[\"']?([^\"'\s>]+)", tag, re.I)
        content = re.search(r"\bcontent\s*=\s*(?:\"([^\"]*)\"|'([^']*)')", tag, re.I)
        if key and content:
            out.setdefault(key.group(1).lower(), unescape(content.group(1) or content.group(2) or "").strip())
    return out


def _jsonld_descriptions(page_html: str) -> list[str]:
    found: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            types = node.get("@type")
            types = types if isinstance(types, list) else [types]
            if any(isinstance(t, str) and t.endswith("Event") for t in types) and isinstance(node.get("description"), str):
                found.append(node["description"])
            for key in ("@graph", "subEvent", "itemListElement", "item"):
                if key in node:
                    walk(node[key])

    for m in re.finditer(r"<script\b[^>]*type\s*=\s*[\"']?application/ld\+json[\"']?[^>]*>(.*?)</script>", page_html, re.S | re.I):
        try:
            walk(json.loads(m.group(1)))
        except ValueError:
            continue
    return found


def _first_paragraph(page_html: str) -> str:
    main = re.search(r"<(main|article)\b[^>]*>(.*?)</\1>", page_html, re.S | re.I)
    scope = main.group(2) if main else page_html
    scope = re.sub(r"<(nav|header|footer|aside|form|script|style|noscript)\b.*?</\1>", " ", scope, flags=re.S | re.I)
    for p in re.findall(r"<p\b[^>]*>(.*?)</p>", scope, re.S | re.I):
        text = clean_description(p)
        if len(text) >= 60 and _usable_description(text):
            return text
    return ""


def detail_info(page_html: str, base_url: str) -> dict:
    """{'description', 'source', 'image_url'} from an event detail page. Order: JSON-LD -> og -> meta -> first paragraph."""
    meta = _meta_content(page_html)
    description, source = "", ""
    for raw in _jsonld_descriptions(page_html):
        d = clean_description(raw)
        if _usable_description(d):
            description, source = d, "jsonld"
            break
    if not description:
        d = clean_description(meta.get("og:description"))
        if _usable_description(d):
            description, source = d, "og"
    if not description:
        d = clean_description(meta.get("description") or meta.get("twitter:description"))
        if _usable_description(d):
            description, source = d, "meta"
    if not description:
        d = _first_paragraph(page_html)
        if d:
            description, source = d, "paragraph"
    image = meta.get("og:image") or meta.get("og:image:url") or meta.get("twitter:image") or ""
    return {"description": description, "source": source, "image_url": absolutize(base_url, image) if image else ""}


def enrich_with_descriptions(events: list[dict], details: dict[str, str]) -> int:
    """Fill empty descriptions (and a missing/placeholder image) from detail pages. Returns events changed."""
    done = 0
    for ev in events:
        page = details.get(ev.get("external_url") or "")
        if not page:
            continue
        info = detail_info(page, ev["external_url"])
        changed = False
        if info["description"] and len(str(ev.get("description") or "").strip()) < 25:
            ev["description"] = info["description"]
            changed = True
        if info["image_url"] and (not ev.get("image_url") or is_placeholder_image(ev["image_url"])):
            ev["image_url"] = info["image_url"]
            changed = True
        done += 1 if changed else 0
    return done


def enrich_with_details(events: list[dict], details: dict[str, str]) -> int:
    """Add clock times from detail pages. Returns how many events got a time."""
    done = 0
    for ev in events:
        html = details.get(ev.get("external_url") or "")
        if not html or ev.get("raw_time_string"):
            continue
        prog = program_from_detail(html, ev.get("title", ""))
        if not prog:
            continue
        title_key = re.sub(r"\W+", "", (ev.get("title") or "").lower())
        doors = next((t for t, label in prog if _DOORS_WORDS.search(label)), "")
        show = next((t for t, label in prog if title_key and title_key[:12] in re.sub(r"\W+", "", label.lower())), "")
        if not show:
            others = [t for t, label in prog if not _DOORS_WORDS.search(label)]
            show = others[-1] if others else ""
        parts = ([f"Doors {doors}"] if doors else []) + ([f"Show {show}"] if show else [])
        if not parts:
            continue
        ev["raw_time_text"] = " | ".join(parts)
        ev["raw_time_string"] = show or doors
        ev["source_text"] = evidence_text(ev.get("title"), ev.get("raw_date_string"), *parts, *(f"{t} {l}" for t, l in prog))
        done += 1
    return done


# ─── per-source orchestration ────────────────────────────────────────────────

def process_source(source: dict, page_html: str, crawl_status: int,
                   packs: list[dict]) -> list[dict]:
    label = source.get("site_name") or source.get("name") or source["url"]
    base_url = source["url"]
    selectors = source.get("selectors") or {}

    events = parse_jsonld_events(page_html, base_url, label)
    build_page_time_lookup(page_html)

    effective = dict(selectors)
    if not effective.get("event_container"):
        pack = find_pack(packs, source.get("name", ""), source.get("site_name", ""), base_url)
        if pack:
            effective = dict(pack.get("selectors", {}))
            effective.update({k: v for k, v in selectors.items() if v})

    pack_for_source = find_pack(packs, source.get("name", ""), source.get("site_name", ""), base_url)
    list_parser = (pack_for_source or {}).get("list_parser")
    if list_parser == "day_grouped":
        events.extend(parse_day_grouped_list(page_html, base_url, label))
    elif list_parser in EXCLUSIVE_LIST_PARSERS:
        # the pack selectors only matched menu items on these pages: the dedicated parser is the only producer
        if list_parser == "microdata_events":
            found = parse_microdata_events(page_html, base_url, label, (pack_for_source or {}).get("city", ""))
        elif list_parser == "paradiso_by_date":
            found = parse_paradiso_by_date(page_html, base_url, label)
        else:
            found = parse_dotted_date_list(page_html, base_url, label)
        return dedupe(events + found)

    schema = crawl_schema_from(effective)
    if schema:
        for row in css_extract(page_html, schema):
            cand = candidate_from_row(row if isinstance(row, dict) else {}, base_url, label)
            if cand:
                events.append(cand)

    if not events:
        events = extract_via_heuristic(page_html, base_url, label, effective)

    return dedupe(events)


def count_flags(events: list[dict]) -> tuple[int, int, int]:
    with_image = sum(1 for e in events if str(e.get("image_url") or "").startswith("http"))
    with_link = sum(1 for e in events if e.get("external_url"))
    with_time = sum(1 for e in events if e.get("raw_time_string"))
    return with_image, with_link, with_time


async def main() -> int:
    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    service_role = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    webhook_url = (os.environ.get("SCRAPING_WEBHOOK_URL", "").strip()
                   or (f"{supabase_url}/functions/v1/scraping-webhook" if supabase_url else ""))
    secret = os.environ.get("SCRAPER_WEBHOOK_SECRET", "")
    gateway_url = (os.environ.get("SCRAPER_GATEWAY_URL", "").strip()
                   or (f"{supabase_url}/functions/v1/scraper-gateway" if supabase_url else ""))
    only_source = os.environ.get("OUTLIST_ONLY_SOURCE", "").strip().lower()
    dry_run = os.environ.get("OUTLIST_DRY_RUN", "").lower() in ("1", "true", "yes")
    force = os.environ.get("OUTLIST_FORCE", "").lower() in ("1", "true", "yes")

    # Gateway mode (no service role key): SUPABASE_URL (or SCRAPER_GATEWAY_URL + SCRAPING_WEBHOOK_URL) and the webhook secret are enough.
    missing = [n for n, v in (("SUPABASE_URL", supabase_url or (gateway_url if not service_role else "")),
                              ("SCRAPER_WEBHOOK_SECRET", secret)) if not v]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        return 1

    backend = Backend(supabase_url, service_role, gateway_url, secret)
    if not force and not backend.automation_enabled():
        print("SYSTEM_AUTOMATION_ENABLED=false — Layer-2 scraping intentionally disabled")
        return 0

    sources = backend.sources()
    if only_source:
        sources = [s for s in sources if only_source in (s.get("name") or "").lower()
                   or only_source in (s.get("site_name") or "").lower()
                   or only_source in (s.get("url") or "").lower()]
    if not sources:
        print("No matching active html_scrape sources — nothing to do")
        return 0
    print(f"Layer-2 run: {len(sources)} source(s){' [DRY RUN]' if dry_run else ''}")

    packs = load_packs()
    rendered = await crawl_many([s["url"] for s in sources])

    ran = errors = 0
    silent: list[str] = []
    failed_sources: list[str] = []
    for source in sources:
        label = source.get("site_name") or source.get("name") or source["url"]
        ran += 1
        try:
            crawl_status, page_html = rendered.get(source["url"], (0, ""))
            if not page_html:
                raise RuntimeError(f"crawl returned no HTML (status {crawl_status})")
            if is_blocked_page(crawl_status, page_html):
                # reported as an error on the source (admin Scraping tab) instead of "success, 0 events"
                raise RuntimeError(f"blocked by the site: HTTP {crawl_status}, {len(page_html)} bytes (retried once)")
            events = process_source(source, page_html, crawl_status, packs)
            pk = find_pack(packs, source.get("name", ""), source.get("site_name", ""), source["url"])
            if pk and pk.get("detail_times"):
                need = [e["external_url"] for e in events if e.get("external_url") and not e.get("raw_time_string")][:DETAIL_MAX]
                if need:
                    fetched = await crawl_many(need, retry_blocked=False)
                    got = enrich_with_details(events, {u: h for u, (_st, h) in fetched.items()})
                    print(f"[{label}] detail pages: {len(need)} fetched, {got} events got a clock time")
            # 2D: detail pages for events without a description (OUTLIST_DETAIL_DESCRIPTIONS=0 turns this off)
            if os.environ.get("OUTLIST_DETAIL_DESCRIPTIONS", "1") != "0":
                need_d = [e["external_url"] for e in events
                          if e.get("external_url") and len(str(e.get("description") or "").strip()) < 25][:DETAIL_MAX]
                if need_d:
                    fetched_d = await crawl_many(need_d, retry_blocked=False)
                    got_d = enrich_with_descriptions(events, {u: h for u, (_st, h) in fetched_d.items()})
                    print(f"[{label}] description pages: {len(need_d)} fetched, {got_d} events enriched")
            with_image, with_link, with_time = count_flags(events)
            print(f"[{label}] events={len(events)} image={with_image} "
                  f"link={with_link} time={with_time}")

            if dry_run:
                print(f"[{label}] DRY RUN payload: "
                      f"{json.dumps(events[:3], ensure_ascii=False)[:1200]}")
                continue

            if events and webhook_url:
                auto_approve = bool(source.get("auto_approve"))
                all_ok, summary = post_webhook_batched(webhook_url, secret, events, auto_approve)
                if not all_ok:
                    raise RuntimeError(f"webhook batches failed: {summary[:400]}")
                print(f"[{label}] webhook OK: {summary}")

            new_zero = next_zero_count(len(events), source.get("consecutive_zero_count"))
            if new_zero >= ZERO_ALERT_AFTER:
                silent.append(f"{label} ({new_zero} runs in a row with 0 events)")
                print(f"::warning::[{label}] found 0 events for {new_zero} runs in a row - the page layout may have changed")
            backend.source_status(source["id"], {
                "last_status": "success", "last_error": None,
                "events_found_last_run": len(events), "consecutive_zero_count": new_zero})

            backend.run_log({"source_id": source["id"], "http_status": crawl_status,
                             "events_found": len(events), "with_image": with_image,
                             "with_link": with_link, "with_time": with_time, "error": None})
        except Exception as exc:
            errors += 1
            failed_sources.append(f"{label}: {str(exc)[:120]}")
            print(f"::error::[{label}] {exc}", file=sys.stderr)
            if not dry_run:
                backend.source_status(source["id"], {
                    "last_status": "error", "last_error": str(exc)[:500],
                    "events_found_last_run": 0})
                backend.run_log({"source_id": source["id"], "http_status": 0, "events_found": 0,
                                 "with_image": 0, "with_link": 0, "with_time": 0,
                                 "error": str(exc)[:500]})

    print(f"Layer-2 run complete: {ran} source(s), {errors} error(s)")
    write_step_summary(ran, failed_sources, silent)
    return 1 if ran > 0 and errors == ran else 0


# ─── offline validation (REGOLA ZERO: no network, no crawl4ai) ───────────────

def validate() -> int:
    if not os.path.exists(FIXTURE_PATH):
        print(f"FAIL: fixture missing: {FIXTURE_PATH}", file=sys.stderr)
        return 1
    with open(FIXTURE_PATH, encoding="utf-8") as fh:
        page_html = fh.read()
    base = "https://occii.org/events"
    failures: list[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"{'PASS' if cond else 'FAIL'}: {name}{(' — ' + detail) if detail else ''}")
        if not cond:
            failures.append(name)

    # 2D detail-page description + og:image (parity with _shared/extract/describe.ts, same fixtures)
    fx = os.path.join(BASE_DIR, "..", "..", "test_fixtures")

    def _read(rel: str) -> str:
        with open(os.path.join(fx, rel), encoding="utf-8") as fh:
            return fh.read()

    d1 = detail_info(_read("describe/jsonld_and_og.html"), "https://venue.example/events/jazz")
    check("detail: JSON-LD description wins, tags stripped", d1["source"] == "jsonld" and d1["description"].startswith("An intimate jazz evening & jam session") and "<p>" not in d1["description"], d1["description"][:60])
    check("detail: og:image absolutized", d1["image_url"] == "https://venue.example/img/poster-1600x900.jpg", d1["image_url"])
    d2 = detail_info(_read("extraction/melkweg_detail_mind_enterprises.html"), "https://melkweg.nl/en/events/x")
    check("detail: melkweg og/meta description", d2["source"] in ("og", "meta") and "Mind Enterprises" in d2["description"] and len(d2["description"]) <= 601, d2["source"])
    check("detail: melkweg og:image", d2["image_url"].endswith("MindEnterprises_Banner.png"), d2["image_url"])
    d3 = detail_info(_read("describe/paragraph_only.html"), "https://x.example/e")
    check("detail: first-paragraph fallback skips nav/cookie/footer", d3["source"] == "paragraph" and d3["description"].startswith("Join us for a long night of raw techno"), d3["description"][:50])
    check("detail: nothing usable -> empty", detail_info("<html><body><p>Hi</p></body></html>", "https://x.example")["description"] == "")
    evs = [{"external_url": "https://venue.example/events/jazz", "description": "", "image_url": ""}]
    check("enrich_with_descriptions fills description and image", enrich_with_descriptions(evs, {"https://venue.example/events/jazz": _read("describe/jsonld_and_og.html")}) == 1 and evs[0]["description"] != "" and evs[0]["image_url"] != "")

    jld = parse_jsonld_events(page_html, base, "OCCII")
    check("jsonld finds 2 events", len(jld) == 2, f"got {len(jld)}")
    check("jsonld event has ISO date+time",
          bool(jld) and "T" in jld[0]["raw_date_string"])
    check("jsonld event raw_time_string probed (23:00)",
          bool(jld) and jld[0]["raw_time_string"] == "23:00", str(bool(jld) and jld[0]["raw_time_string"]))
    check("jsonld image absolutized",
          bool(jld) and jld[0]["image_url"].startswith("https://occii.org/"))
    check("jsonld external_url",
          bool(jld) and jld[0]["external_url"].startswith("https://occii.org/"))

    with open(SCHEMAS_PATH, encoding="utf-8") as fh:
        packs = json.load(fh)["packs"]
    # S4: webhook batching (25 per POST, one retry per batch, summed counters, a dead batch is reported)
    sent: list[int] = []

    def fake_ok(_u, _s, chunk, _a, timeout=0):
        sent.append(len(chunk))
        return 200, json.dumps({"inserted": len(chunk), "skipped_duplicates": 1, "filtered": 0, "merged": 0})

    ok_all, summ = post_webhook_batched("http://x", "s", [{"title": "e"}] * 60, False, batch=25, poster=fake_ok)
    check("webhook batching: 60 events -> batches of 25, 25, 10", ok_all and sent == [25, 25, 10], str(sent))
    check("webhook batching: counters are summed", "inserted=60" in summ and "skipped_duplicates=3" in summ, summ)
    calls = {"n": 0}

    def flaky(_u, _s, chunk, _a, timeout=0):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("The read operation timed out")
        return 200, json.dumps({"inserted": 1})

    ok_flaky, _ = post_webhook_batched("http://x", "s", [{"title": "e"}] * 5, False, batch=25, poster=flaky)
    check("webhook batching: a timed-out batch is retried once", ok_flaky and calls["n"] == 2)

    def dead(_u, _s, chunk, _a, timeout=0):
        raise TimeoutError("The read operation timed out")

    ok_dead, summ_dead = post_webhook_batched("http://x", "s", [{"title": "e"}] * 30, False, batch=25, poster=dead)
    check("webhook batching: a batch that keeps failing is reported", (not ok_dead) and "FAILED 2/2" in summ_dead, summ_dead)
    check("19 default packs present (every registered source but the webhook pseudo-source)",
          len(packs) == 19, f"got {len(packs)}")
    for required in ("occii", "melkweg", "paradiso", "partyflock", "resident advisor",
                     "amsterdamfun", "asva", "esn amsterdam", "esn vu",
                     "uva student", "vu libcal"):
        check(f"pack match: {required}",
              find_pack(packs, required, required, f"https://www.{required.replace(' ', '')}.nl") is not None)
    check("pack match does not false-positive on 'ra'",
          (find_pack(packs, "paradiso", "paradiso", "https://www.paradiso.nl") or {}).get(
              "match", {}).get("url") == ["paradiso.nl"])

    # ── round-2 hard requirement: per-source link + time coverage ───────────
    for pack in packs:
        sel = pack.get("selectors", {})
        pname = (pack.get("match", {}).get("name") or ["?"])[0]
        ok = all(sel.get(k) for k in ("event_container", "title", "link", "time"))
        check(f"source [{pname}] pack has container+title+LINK+TIME selectors", ok)

    schema = crawl_schema_from({"event_container": ".event", "title": "h2", "date": "time"})
    check("schema builder emits baseSelector + fields",
          bool(schema) and schema["baseSelector"] == ".event" and len(schema["fields"]) >= 7)
    schema_time = [f for f in (schema or {}).get("fields", []) if f["name"] == "time_text"]
    check("schema builder emits time_text field (time → date → 'time' fallback)",
          len(schema_time) == 1 and schema_time[0]["selector"] == "time")
    schema_time2 = crawl_schema_from({
        "event_container": ".ev", "title": "h3", "date": ".date-line", "time": ".start-time"})
    st2 = [f for f in schema_time2["fields"] if f["name"] == "time_text"]
    check("schema time_text prefers explicit 'time' selector over 'date'",
          len(st2) == 1 and st2[0]["selector"] == ".start-time")
    check("schema builder returns None without event_container",
          crawl_schema_from({"title": "h2"}) is None)

    cand = candidate_from_row({
        "title": " Test Event ",
        "raw_date_string": "5 September 2026",
        "image": "/img/relative.jpg",
        "image_srcset": "/img/small.jpg 320w, /img/large.jpg 1200w",
        "link": "/events/123",
        "time_attr": "23:00",
    }, "https://occii.org", "OCCII")
    check("candidate absolutizes image/link",
          cand["image_url"] == "https://occii.org/img/large.jpg"  # F1-09: srcset largest wins
          and cand["external_url"] == "https://occii.org/events/123")
    check("candidate keeps time_attr as raw_time_string", cand["raw_time_string"] == "23:00")
    cand_iso = candidate_from_row({
        "title": "ISO Attr Event",
        "raw_date_string": "vr 2 oktober 2026",
        "link": "/events/x",
        "time_attr": "2026-10-02T22:00:00+02:00",
    }, "https://occii.org", "OCCII")
    check("candidate normalizes ISO datetime attr to HH:MM (not the full ISO blob)",
          cand_iso["raw_time_string"] == "22:00", cand_iso["raw_time_string"])
    cand_txt = candidate_from_row({
        "title": "Text Time Event",
        "raw_date_string": "5 november 2026",
        "link": "/events/y",
        "time_text": "deur open 22:30",
    }, "https://occii.org", "OCCII")
    check("candidate probes time_text selector text", cand_txt["raw_time_string"] == "22:30",
          cand_txt["raw_time_string"])

    check("F1-07: candidate forwards source_text evidence",
          "deur open 22:30" in cand_txt.get("source_text", ""), cand_txt.get("source_text", ""))
    check("F1-07: candidate forwards full raw_time_text (doors vs start)",
          cand_txt.get("raw_time_text") == "deur open 22:30", cand_txt.get("raw_time_text", ""))
    check("F1-07: jsonld events carry the JSON-LD node", bool(jld) and jld[0].get("jsonld", {}).get("name") == "Noiselab Night")

    check("F1-07: ISO UTC 'Z' converted to Amsterdam (CEST) — melkweg island format",
          probe_time_text("2026-10-18T17:00:00.000000Z") == "19:00", probe_time_text("2026-10-18T17:00:00.000000Z"))
    check("F1-07: ISO UTC converted to Amsterdam (CET, after last Sunday of October)",
          probe_time_text("2026-10-25T01:30:00Z") == "02:30" and probe_time_text("2026-11-07T19:30:00Z") == "20:30")
    check("F1-07: zone-less ISO kept as local", probe_time_text("2026-11-14T23:00") == "23:00")
    check("F1-07: last Sundays (2026: Mar 29 / Oct 25)", (_last_sunday(2026, 3), _last_sunday(2026, 10)) == (29, 25))
    # 3.5 item 15: 12-hour and Dutch time spellings
    check("15: 10pm -> 22:00", probe_time_text("Doors 10pm") == "22:00", probe_time_text("Doors 10pm"))
    check("15: 9:30 p.m. -> 21:30", probe_time_text("from 9:30 p.m.") == "21:30", probe_time_text("from 9:30 p.m."))
    check("15: 12am -> 00:00, 12 pm -> 12:00", probe_time_text("12am") == "00:00" and probe_time_text("12 pm") == "12:00")
    check("15: 22 uur -> 22:00, 22u30 -> 22:30", probe_time_text("Aanvang 22 uur") == "22:00" and probe_time_text("22u30") == "22:30")
    check("15: no false positive on plain text", probe_time_text("Sam's pub, Utrecht") == "" and probe_time_text("5 June 2026") == "")

    check("srcset picks largest", pick_srcset_largest("a.jpg 320w, b.jpg 1200w, c.jpg 640w") == "b.jpg")

    ev_a = {"title": "X", "raw_date_string": "1 jan", "external_url": "https://e/1"}
    ev_b = {"title": "X", "raw_date_string": "1 jan", "external_url": ""}
    check("dedupe drops same title+date", len(dedupe([ev_a, dict(ev_b)])) == 1)
    check("dedupe drops same url",
          len(dedupe([ev_a, {"title": "Y", "raw_date_string": "2 jan",
                             "external_url": "https://e/1"}])) == 1)

    heur = extract_via_heuristic(page_html, base, "OCCII", {})
    check("heuristic finds 3 fixture cards", len(heur) == 3, f"got {len(heur)}")
    if heur:
        h0 = heur[0]
        check("heuristic image probe (srcset largest, absolutized)",
              h0["image_url"] == "https://occii.org/images/cover1-large.jpg", h0["image_url"])
        check("heuristic link probe", h0["external_url"].startswith("https://occii.org/"),
              h0["external_url"])
        check("heuristic time probe", h0["raw_time_string"] == "23:00", h0["raw_time_string"])
        check("heuristic venue text", bool(h0["venue_name"]))
        check("F1-07: heuristic source_text has the card text",
              "open 22:00" in h0.get("source_text", "") and "start 23:00" in h0.get("source_text", ""))

    pack = find_pack(packs, "occii", "occii", "https://occii.org/events") or {}
    via_pack = extract_via_heuristic(page_html, base, "OCCII", pack.get("selectors", {}))
    check("pack comma-selector container '.event, article' finds 3", len(via_pack) == 3,
          f"got {len(via_pack)}")
    if via_pack:
        check("comma-list title selector resolves exactly",
              via_pack[0]["title"] == "Punk Fest Amsterdam", via_pack[0]["title"])
        check("comma-list date selector resolves",
              "25 september" in via_pack[0]["raw_date_string"], via_pack[0]["raw_date_string"])

    lazy = extract_first('<div class="card"><a href="/e/9"><img data-src="/i.jpg"></a></div>',
                         "a", "https://x.org")
    check("extract_first port (href)", lazy["href"] == "https://x.org/e/9")

    # ── OCCII pilot end-to-end with the REAL prod scraping_sources selectors ──
    # (mirrors the live DB row: container .occii-event-display, title/link
    # h3.occii-event-link a, date p.occii-event-times, image .occii-event-display-image img)
    occii_db = {
        "event_container": ".occii-event-display",
        "title": "h3.occii-event-link a",
        "date": "p.occii-event-times",
        "link": "h3.occii-event-link a",
        "image": ".occii-event-display-image img",
    }
    via_db = extract_via_heuristic(page_html, base, "OCCII", occii_db)
    check("OCCII prod selectors find 3 fixture events", len(via_db) == 3, f"got {len(via_db)}")
    for i, ev in enumerate(via_db):
        label = f"OCCII event {i + 1} [{ev['title'][:30]}]"
        check(f"{label}: external LINK present",
              ev["external_url"].startswith("https://occii.org/"), ev["external_url"])
        check(f"{label}: raw_time_string present (no 21:00 fallback)",
              bool(ev["raw_time_string"]), ev["raw_time_string"])
        check(f"{label}: image present", ev["image_url"].startswith("https://occii.org/"),
              ev["image_url"])

    # ── F1-09 / T-COV-05: real cards from 3 sources (test_fixtures/covers/) ──
    cov_dir = os.path.join(BASE_DIR, "..", "..", "test_fixtures", "covers")
    real_path = os.path.join(cov_dir, "real_sources.json")
    if os.path.exists(real_path):
        with open(real_path, encoding="utf-8") as fh:
            real = json.load(fh)
        for src in real["sources"]:
            with open(os.path.join(cov_dir, src["card_fixture"]), encoding="utf-8") as fh:
                card = fh.read()
            got = extract_via_heuristic(card, src["page_url"], src["source"], src["selectors"])
            legacy_srcs = [m.group(1) for m in re.finditer(r'<img\b[^>]*?(?<![\w-])src="([^"]*)"', card)]
            for exp in src["events"]:
                key = exp.get("title") or exp["title_prefix"]
                ev = next((e for e in got if e["title"].startswith(key)), None)
                check(f"F1-09 {src['source']} [{key[:24]}]: card extracted", ev is not None)
                if ev:
                    check(f"F1-09 {src['source']} [{key[:24]}]: real image, not placeholder/thumbnail",
                          ev["image_url"] == exp["crawler_image_url"], ev["image_url"])
                check(f"F1-09 {src['source']} [{key[:24]}]: fixture legacy src is what the old crawler stored",
                      exp["legacy_image_url"] in legacy_srcs)

    # D-T04-2 / D-T04-3: title sanitizer (mirror of title.ts)
    check("title: raw <a href> selector junk is rejected", clean_title('koop tickets <a href="https://www.bi') == "")
    check("title: unclosed tag + CTA-only rejected", clean_title("koop tickets </") == "")
    check("title: CTA suffix/prefix stripped", clean_title("Natanya – THE ATTITUDE TOUR - koop tickets") == "Natanya – THE ATTITUDE TOUR"
          and clean_title("Meer info | Foo &amp; Bar") == "Foo & Bar")
    check("title: short alphanumeric acts kept", all(clean_title(t) == t for t in ("NAPA", "MUNA", "LEAP", "EAST", "TX2")))
    check("title: numbers, single char, junk rejected", all(clean_title(t) == "" for t in ("12", "A", "Home", "menu", "Agenda", "Tickets", "2024")))
    # shared cases with the TypeScript original (BUG-F20261110-41 / 46): the same JSON file is read by tests/extraction/title.test.ts
    with open(os.path.join(fx, "extraction", "title_cases.json"), encoding="utf-8") as fh:
        shared_cases = json.load(fh)["cases"]
    bad_shared = [(c["input"], clean_title(c["input"]), c["expected"]) for c in shared_cases if clean_title(c["input"]) != c["expected"]]
    check(f"title: all {len(shared_cases)} shared waitlist cases match the TypeScript rule", not bad_shared, str(bad_shared[:3]))
    check("title: 'Tickets to Ride' untouched", clean_title("Tickets to Ride") == "Tickets to Ride")

    # club list parsers (S7, BUG-F20261110-43): fixtures are trimmed copies of the public pages saved 2026-11-10
    fxd = os.path.join(BASE_DIR, "fixtures")

    def _fx(name: str) -> str:
        with open(os.path.join(fxd, name), encoding="utf-8") as fh:
            return fh.read()

    packs = load_packs()
    junk_titles = {"home", "menu", "agenda", "tickets", "cookies", "meer info", "inloggen"}

    pf_html = _fx("partyflock_agenda.html")
    pf = process_source({"name": "Partyflock", "site_name": "Partyflock", "url": "https://partyflock.nl/agenda"}, pf_html, 200, packs)
    check("partyflock: Amsterdam events from schema.org microdata", len(pf) >= 10, f"got {len(pf)}")
    check("partyflock: every event has a title, an ISO date with offset and an absolute link",
          all(e["title"] and re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", e["raw_date_string"]) and e["external_url"].startswith("https://partyflock.nl/party/") for e in pf))
    check("partyflock: no navigation junk and no other city", not {e["title"].lower() for e in pf} & junk_titles
          and not any(e["venue_name"] == "Ampere" for e in pf), ", ".join(sorted({e["venue_name"] for e in pf}))[:80])
    check("partyflock: city filter off keeps the other cities", len(parse_microdata_events(pf_html, "https://partyflock.nl/agenda", "Partyflock")) > len(pf))

    pa_html = _fx("paradiso_program.html")
    pa = parse_paradiso_by_date(pa_html, "https://www.paradiso.nl/en", "Paradiso", now=datetime(2026, 11, 10, tzinfo=timezone.utc))
    check("paradiso: events from the by-date list", len(pa) >= 8, f"got {len(pa)}")
    check("paradiso: first day is 2026-11-10 with title, time and /program/ link",
          bool(pa) and pa[0]["title"] == "Kiwi Jr." and pa[0]["raw_date_string"] == "2026-11-10" and pa[0]["raw_time_string"] == "19:30"
          and pa[0]["external_url"] == "https://www.paradiso.nl/en/program/kiwi-jr/2884046", str(pa[:1]))
    check("paradiso: other venues of the programme keep their own name", any(e["venue_name"] == "Tolhuistuin" for e in pa))
    check("paradiso: a December date after the year turn belongs to the next year",
          parse_paradiso_by_date(pa_html.replace("Tu 10 Nov", "Tu 5 Jan"), "https://www.paradiso.nl/en", "Paradiso", now=datetime(2026, 12, 20, tzinfo=timezone.utc))[0]["raw_date_string"] == "2027-01-05")
    check("paradiso: wired through the pack (process_source)",
          len(process_source({"name": "Paradiso", "site_name": "Paradiso", "url": "https://www.paradiso.nl/en"}, pa_html, 200, packs)) == len(pa))
    check("paradiso: no junk titles", not {e["title"].lower() for e in pa} & junk_titles)

    we_html = _fx("warehouse_elementenstraat_events.html")
    we = process_source({"name": "Warehouse Elementenstraat", "site_name": "Warehouse Elementenstraat", "url": "https://elementenstraat.nl/events"}, we_html, 200, packs)
    check("warehouse: all 10 list items parsed", len(we) == 10, f"got {len(we)}")
    check("warehouse: DD . MM . YYYY cells become an ISO date, time and ticket link kept",
          any(e["title"] == "ELEKTRA X MIXMAG ADE" and e["raw_date_string"] == "2026-10-21" and e["raw_time_string"] == "23:00"
              and e["external_url"].startswith("https://xceed.me/") for e in we))
    check("warehouse: venue is the club, no junk", all(e["venue_name"] == "Warehouse Elementenstraat" for e in we) and not {e["title"].lower() for e in we} & junk_titles)
    check("club parsers: an unrelated page yields nothing (no false positives)",
          not parse_microdata_events("<html><body><h1>Home</h1></body></html>", "https://x.example", "X")
          and not parse_paradiso_by_date("<html><body><h1>Home</h1></body></html>", "https://x.example", "X")
          and not parse_dotted_date_list("<html><body><h1>Home</h1></body></html>", "https://x.example", "X"))

    # BUG-F20261118-4: a 403 / Cloudflare challenge page used to be parsed as an empty agenda and logged as "success, 0 events"
    check("blocked pages: a 403 with a tiny body, a 429, a challenge page and an empty page are blocked",
          is_blocked_page(403, "403 Forbidden") and is_blocked_page(429, "x" * 5000)
          and is_blocked_page(200, "<title>Just a moment...</title>" + "x" * 3000) and is_blocked_page(0, ""))
    check("blocked pages: a normal agenda page is not blocked",
          not is_blocked_page(200, "<html><body>" + "<li>event</li>" * 400 + "</body></html>") and not is_blocked_page(200, "x" * 5000 + " 403 "))

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{len(failures)} CHECK(S) FAILED'}")
    return 0 if not failures else 1


if __name__ == "__main__":
    if "--validate" in sys.argv:
        sys.exit(validate())
    sys.exit(asyncio.run(main()))
