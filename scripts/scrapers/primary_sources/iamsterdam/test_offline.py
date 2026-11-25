"""Offline test for the iamsterdam primary source: real fixtures (listing page + detail JSON-LD) -> client parsers -> mapper -> events."""

from __future__ import annotations

import glob
import json
import os
import unittest

from .. import base
from . import client, mapper

HERE = os.path.dirname(os.path.abspath(__file__))
FX = os.path.join(HERE, "fixtures")
META = json.loads(base.read_text(os.path.join(HERE, "meta.json")))


def read(name: str) -> str:
    return base.read_text(os.path.join(FX, name))


class IamsterdamOffline(unittest.TestCase):
    def test_listing_parser_reads_the_embedded_json(self):
        items = client.parse_listing(read("listing_nightlife.html"))
        self.assertGreaterEqual(len(items), 8)
        self.assertTrue(all(i["slug"].startswith("whats-on/calendar/") for i in items))

    def test_listing_parser_returns_empty_when_layout_changes(self):
        self.assertEqual(client.parse_listing("<html><body>nothing</body></html>"), [])
        self.assertEqual(client.parse_listing('<script>self.__next_f.push([1,"{bad"])</script>'), [])

    def test_series_are_detected_from_the_listing_range(self):
        self.assertTrue(client.is_series(["2026-06-05T23:00:00.000Z", "2026-11-27T23:00:00.000Z"]))
        self.assertFalse(client.is_series(["2026-11-24T23:30:00.000Z", "2026-11-24T23:30:00.000Z"]))
        self.assertFalse(client.is_series(None))

    def test_mapper_keeps_single_occurrences_and_drops_series(self):
        cands = []
        for f in sorted(glob.glob(os.path.join(FX, "detail_*.html"))):
            cands.append({"listing": {"slug": "whats-on/calendar/nightlife/clubbing/" + os.path.basename(f)[7:-5], "id": "x"}, "ld": client.parse_detail(base.read_text(f))})
        self.assertGreaterEqual(len(cands), 3)
        events = mapper.map_events(cands, META)
        titles = [e["title"] for e in events]
        self.assertEqual(titles, ["Techno Tuesday; Jayzo"], titles)  # the four others are weeks/months long
        e = events[0]
        self.assertEqual((e["raw_date_string"], e["raw_time_string"]), ("2026-11-24", "23:30"))  # +00:00 stamp ignored, clock is Amsterdam
        self.assertEqual(e["source_kind"], "primary")
        self.assertNotIn("end_date_string", e)  # end == start means unknown
        self.assertEqual(base.validate_events(events, source_prefix=META["source_prefix"]), [])

    def test_client_fetch_uses_only_robots_allowed_urls_and_skips_series(self):
        pages = {"nightlife": read("listing_nightlife.html")}
        urls = []

        def transport(method, url, headers, body, timeout):
            urls.append(url)
            if "/calendar/" in url and "/clubbing/" not in url:
                return 200, {}, (pages["nightlife"] if url.endswith("/nightlife") else b"<html></html>".decode()).encode()
            return 200, {}, read("detail_techno-tuesday-jayzo.html").encode()

        http = base.Http(min_interval_s=0, transport=transport, sleep=lambda s: None)
        out = client.fetch(http, {"params": {"categories": ["nightlife"]}})
        for u in urls:
            self.assertTrue("?" not in u or "?page=" in u, u)  # robots: query strings are disallowed except ?page=
        self.assertTrue(all(c["ld"]["name"] for c in out))
        self.assertLess(len(urls), 1 + 16)  # series were not fetched


if __name__ == "__main__":
    unittest.main()
