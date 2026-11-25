"""Offline test for the ra.co primary source: fixture -> mapper -> webhook events, and the client's paging with a fake transport."""

from __future__ import annotations

import json
import os
import unittest

from .. import base
from . import client, mapper

HERE = os.path.dirname(os.path.abspath(__file__))
META = json.loads(base.read_text(os.path.join(HERE, "meta.json")))
ROWS = json.loads(base.read_text(os.path.join(HERE, "fixtures", "listing_page1.json")))["data"]["eventListings"]["data"]


class RaCoOffline(unittest.TestCase):
    def test_maps_every_row_to_a_valid_primary_event(self):
        events = mapper.map_events(ROWS, META)
        self.assertEqual(len(events), len(ROWS))
        self.assertEqual(base.validate_events(events, source_prefix=META["source_prefix"]), [])
        first = events[0]
        self.assertEqual((first["raw_date_string"], first["raw_time_string"]), ("2026-11-23", "23:00"))  # RA startTime is Amsterdam wall-clock
        self.assertTrue(first["external_url"].startswith("https://ra.co/events/"))
        self.assertEqual(first["source_kind"], "primary")
        self.assertAlmostEqual(first["lat"], 52.366478, places=5)

    def test_skips_rows_without_title_time_or_event_path(self):
        bad = [{"event": {"id": "1", "title": "", "startTime": "2026-12-01T22:00:00.000", "contentUrl": "/events/1"}},
               {"event": {"id": "2", "title": "x", "startTime": None, "date": None, "contentUrl": "/events/2"}},
               {"event": {"id": "3", "title": "x", "startTime": "2026-12-01T22:00:00.000", "contentUrl": "http://evil/x"}}, None, {}]
        self.assertEqual(mapper.map_events(bad, META), [])

    def test_client_pages_until_short_page(self):
        calls = []

        def transport(method, url, headers, body, timeout):
            page = json.loads(body)["variables"]["p"]
            calls.append(page)
            data = ROWS[:1] * (client.PAGE_SIZE if page == 1 else 3)
            return 200, {}, json.dumps({"data": {"eventListings": {"totalResults": client.PAGE_SIZE + 3, "data": data}}}).encode()

        rows = client.fetch(base.Http(min_interval_s=0, transport=transport, sleep=lambda s: None), META)
        self.assertEqual(calls, [1, 2])
        self.assertEqual(len(rows), client.PAGE_SIZE + 3)

    def test_client_raises_on_graphql_errors(self):
        http = base.Http(min_interval_s=0, retries=0, sleep=lambda s: None, transport=lambda *a: (200, {}, b'{"errors":[{"message":"x"}]}'))
        with self.assertRaises(RuntimeError):
            client.fetch(http, META)


if __name__ == "__main__":
    unittest.main()
