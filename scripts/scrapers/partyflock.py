#!/usr/bin/env python3
"""DEPRECATED (release 3.6): standalone Partyflock stub, not used by Layer 2 and left unchanged on purpose.

Its hard-coded URL (/agenda/amsterdam) answers 404 since 2026 and the page has no JSON-LD. Partyflock is now crawled by
scripts/scrapers/outlist_crawler.py with the "microdata_events" parser (pack in source_schemas.json). Do not run this file.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
import urllib.parse
from html import unescape

WEBHOOK_URL = os.environ.get('SCRAPING_WEBHOOK_URL', '')
WEBHOOK_SECRET = os.environ.get('SCRAPER_WEBHOOK_SECRET', '')

if not WEBHOOK_URL or not WEBHOOK_SECRET:
    print('Missing SCRAPING_WEBHOOK_URL or SCRAPER_WEBHOOK_SECRET', file=sys.stderr)
    sys.exit(1)

AGENDA_URL = 'https://partyflock.nl/agenda/amsterdam'
request = urllib.request.Request(
    AGENDA_URL,
    headers={
        'User-Agent': 'Mozilla/5.0 (compatible; OutlistBot/1.0; +https://app.outlist.org)',
        'Accept': 'text/html,application/xhtml+xml',
    },
)
with urllib.request.urlopen(request, timeout=60) as response:
    html = response.read().decode('utf-8', errors='replace')

def text(value: object) -> str:
    return re.sub(r'\s+', ' ', unescape(str(value or ''))).strip()

def absolute(url: str) -> str:
    return urllib.parse.urljoin(AGENDA_URL, url)

payload: list[dict[str, str]] = []
scripts = re.findall(
    r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    html, flags=re.I | re.S,
)
for raw in scripts:
    try:
        data = json.loads(raw.strip().replace('<!--', '').replace('-->', ''))
    except json.JSONDecodeError:
        continue
    queue = list(data) if isinstance(data, list) else [data]
    while queue:
        item = queue.pop(0)
        if isinstance(item, list):
            queue.extend(item)
            continue
        if not isinstance(item, dict):
            continue
        queue.extend(item.get('@graph', []) if isinstance(item.get('@graph'), list) else [])
        kind = item.get('@type', '')
        kinds = kind if isinstance(kind, list) else [kind]
        if not any(str(k).lower() in ('event', 'music event', 'festival') for k in kinds):
            continue
        title = text(item.get('name'))
        start = text(item.get('startDate'))
        if not title or not start:
            continue
        location = item.get('location') if isinstance(item.get('location'), dict) else {}
        address = location.get('address') if isinstance(location, dict) else {}
        address_text = address if isinstance(address, str) else ', '.join(
            text(address.get(k)) for k in ('streetAddress', 'postalCode', 'addressLocality')
            if isinstance(address, dict) and text(address.get(k))
        )
        payload.append({
            'source': 'partyflock',
            'title': title[:255],
            'raw_date_string': start,
            'end_date_string': text(item.get('endDate')),
            'venue_name': text(location.get('name')) if isinstance(location, dict) else 'Partyflock',
            'address': text(address_text),
            'description': text(item.get('description'))[:4000],
            'external_url': absolute(text(item.get('url') or item.get('@id'))),
            'image_url': absolute(text(item.get('image'))),
            'category_hint': 'club_nightclub',
        })

seen: set[tuple[str, str]] = set()
payload = [event for event in payload
           if not ((event['title'], event['raw_date_string']) in seen
                   or seen.add((event['title'], event['raw_date_string'])))]

req = urllib.request.Request(
    WEBHOOK_URL,
    data=json.dumps(payload).encode('utf-8'),
    headers={
        'Content-Type': 'application/json',
        'X-Webhook-Secret': WEBHOOK_SECRET,
    },
    method='POST',
)

with urllib.request.urlopen(req, timeout=60) as response:
    print(response.read().decode('utf-8'))
