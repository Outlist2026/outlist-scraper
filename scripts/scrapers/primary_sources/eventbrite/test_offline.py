"""Offline test for the eventbrite primary source: trimmed real pages -> client parsers -> mapper -> events."""

from __future__ import annotations

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


class EventbriteOffline(unittest.TestCase):
    def test_page_parser_reads_cards_and_schema_items(self):
        cards, items = client.parse_page(read("landing.html"))
        self.assertGreaterEqual(len(cards), 8)
        self.assertEqual(len(cards), len(items))
        self.assertTrue(all(c["url"].startswith("https://") for c in cards))

    def test_page_parser_returns_empty_when_layout_changes(self):
        self.assertEqual(client.parse_page("<html>nothing</html>"), ([], []))
        self.assertEqual(client.parse_page("window.__SERVER_DATA__ = {bad"), ([], []))

    def test_detail_parser(self):
        d = client.parse_detail(read("detail.html"))
        self.assertEqual(d["startDate"], "2026-12-03T19:30:00+01:00")

    def test_mapper_keeps_music_and_drops_the_rest(self):
        cards, items = client.parse_page(read("landing.html"))
        by = {client.event_id(it["url"]): it for it in items}
        cands = [{"id": client.event_id(c["url"]), "ld": by.get(client.event_id(c["url"])), "card": c, "nightlife": False, "detail": None} for c in cards]
        events = mapper.map_events(cands, META)
        titles = [e["title"] for e in events]
        self.assertIn("AMSTERDAM x Old School Vibes", titles)
        self.assertNotIn("BARTS Sample Sale 2026", titles)  # not music
        self.assertFalse(any("PSNY" in t for t in titles))  # online event, other timezone
        e = next(x for x in events if x["title"] == "AMSTERDAM x Old School Vibes")
        self.assertEqual((e["raw_date_string"], e["raw_time_string"]), ("2026-11-27", "23:00"))
        self.assertEqual(e["category_hint"], "club_nightclub")
        self.assertEqual(e["venue_name"], "Club Panama")
        self.assertTrue(e["external_url"].startswith("https://www.eventbrite.nl/e/"))
        self.assertEqual(e["source_kind"], "primary")
        self.assertEqual(base.validate_events(events, source_prefix=META["source_prefix"]), [])

    def test_date_only_item_is_dropped_until_the_detail_gives_the_time(self):
        _cards, items = client.parse_page(read("nightlife.html"))
        self.assertEqual(len(items), 6)
        cand = {"id": client.event_id(items[0]["url"]), "ld": items[0], "card": None, "nightlife": True, "detail": None}
        self.assertEqual(mapper.map_events([cand], META), [])
        cand["detail"] = client.parse_detail(read("detail.html"))
        out = mapper.map_events([cand], META)
        self.assertEqual((out[0]["raw_date_string"], out[0]["raw_time_string"]), ("2026-12-03", "19:30"))
        self.assertEqual(out[0]["category_hint"], "club_nightclub")

    def test_cancelled_and_multiday_are_dropped(self):
        card = {"id": "123456", "name": "X", "url": "https://www.eventbrite.com/e/x-tickets-123456", "start_date": "2026-12-01", "start_time": "20:00",
                "timezone": "Europe/Amsterdam", "tags": [{"tag": mapper.MUSIC}]}
        ok = {"id": "123456", "ld": None, "card": card, "nightlife": False, "detail": None}
        self.assertEqual(len(mapper.map_events([ok], META)), 1)
        self.assertEqual(mapper.map_events([{**ok, "card": {**card, "is_cancelled": True}}], META), [])
        long_card = {**card, "end_date": "2026-12-05", "end_time": "20:00", "hide_end_date": False}
        self.assertEqual(mapper.map_events([{**ok, "card": long_card}], META), [])

    def test_client_fetch_uses_no_query_strings_and_asks_details_only_for_missing_times(self):
        urls = []

        def transport(method, url, headers, body, timeout):
            urls.append(url)
            if "/e/" in url:
                return 200, {}, read("detail.html").encode()
            return 200, {}, read("nightlife.html" if "nightlife" in url else "landing.html").encode()

        http = base.Http(min_interval_s=0, transport=transport, sleep=lambda s: None)
        out = client.fetch(http, META)
        for u in urls:
            self.assertNotIn("?", u)
        self.assertTrue(any(r["detail"] for r in out))
        self.assertLessEqual(len(urls), 2 + client.MAX_DETAILS)


if __name__ == "__main__":
    unittest.main()
