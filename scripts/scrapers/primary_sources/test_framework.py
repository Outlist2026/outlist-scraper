"""Offline tests of the shared plumbing (base.py), the source selection and the posting of the runner (run.py). No network."""

from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from . import base, registry, run


def src(slug="x", enabled=True, **meta):
    return SimpleNamespace(slug=slug, meta={"enabled": enabled, "source_prefix": f"primary:{slug}", **meta})


class Time(unittest.TestCase):
    def test_amsterdam_dst_boundaries(self):
        # 2026: DST starts Sun 29 Mar 01:00 UTC, ends Sun 25 Oct 01:00 UTC
        self.assertEqual(base.amsterdam_parts(datetime(2026, 3, 29, 0, 59, tzinfo=timezone.utc)), ("2026-03-29", "01:59"))
        self.assertEqual(base.amsterdam_parts(datetime(2026, 3, 29, 1, 0, tzinfo=timezone.utc)), ("2026-03-29", "03:00"))
        self.assertEqual(base.amsterdam_parts(datetime(2026, 10, 25, 0, 59, tzinfo=timezone.utc)), ("2026-10-25", "02:59"))
        self.assertEqual(base.amsterdam_parts(datetime(2026, 10, 25, 1, 0, tzinfo=timezone.utc)), ("2026-10-25", "02:00"))

    def test_naive_iso_is_amsterdam_wall_clock(self):
        d = base.parse_iso("2026-11-23T23:00:00.000")
        self.assertEqual(base.amsterdam_parts(d), ("2026-11-23", "23:00"))
        d = base.parse_iso("2026-07-10T23:00:00")
        self.assertEqual(base.amsterdam_parts(d), ("2026-07-10", "23:00"))

    def test_bad_values_give_none(self):
        for v in (None, "", "soon", 5):
            self.assertIsNone(base.parse_iso(v))


class Events(unittest.TestCase):
    def mk(self, **o):
        kw = dict(title="Night", start=base.parse_iso("2026-12-05T22:00:00"), url="https://x.example/e/1", venue="Paradiso")
        kw.update(o)
        return base.make_event("primary:x/1", **kw)

    def test_event_has_the_primary_contract_fields(self):
        e = self.mk(end=base.parse_iso("2026-12-06T04:00:00"), lat=52.36, lng=4.88, price="10", image="https://img.example/a.jpg")
        self.assertEqual((e["source_kind"], e["raw_date_string"], e["raw_time_string"]), ("primary", "2026-12-05", "22:00"))
        self.assertEqual(e["jsonld"]["startDate"][:19], "2026-12-05T22:00:00")
        self.assertEqual(e["lat"], 52.36)
        self.assertEqual(base.validate_events([e], source_prefix="primary:x"), [])

    def test_non_https_url_and_image_and_zero_coordinates_are_dropped(self):
        e = self.mk(url="http://x.example/e", image="http://i/a.jpg", lat=0.0, lng=0.0)
        self.assertEqual(e["external_url"], "")
        self.assertNotIn("image_url", e)
        self.assertNotIn("lat", e)
        self.assertTrue(base.validate_events([e], source_prefix="primary:x"))

    def test_validate_flags_wrong_kind_prefix_and_duplicates(self):
        a = self.mk()
        b = dict(a, source_kind="web", source="other:1")
        probs = base.validate_events([a, b, dict(a)], source_prefix="primary:x")
        self.assertTrue(any("source_kind" in p for p in probs))
        self.assertTrue(any("source must start" in p for p in probs))
        self.assertTrue(any("duplicate" in p for p in probs))

    def test_lone_surrogates_do_not_break_json(self):
        e = self.mk(title="Bad \udc9d title")
        json.dumps([e]).encode("utf-8")

    def test_future_only_and_dedupe(self):
        now = datetime(2026, 12, 1, tzinfo=timezone.utc)
        old = self.mk(start=base.parse_iso("2026-11-01T22:00:00"), url="https://x.example/old")
        new = self.mk(url="https://x.example/new")
        self.assertEqual([e["external_url"] for e in base.future_only([old, new], now)], ["https://x.example/new"])
        self.assertEqual(len(base.dedupe([new, dict(new)])), 1)


class HttpBehaviour(unittest.TestCase):
    def test_retries_on_503_with_retry_after_then_succeeds(self):
        seq = [(503, {"Retry-After": "3"}, b""), (200, {}, b'{"ok":1}')]
        sleeps = []
        h = base.Http(min_interval_s=0, transport=lambda *a: seq.pop(0), sleep=sleeps.append)
        self.assertEqual(h.get_json("https://x.example"), (200, {"ok": 1}))
        self.assertIn(3.0, sleeps)

    def test_request_budget_is_enforced(self):
        h = base.Http(min_interval_s=0, max_requests=2, transport=lambda *a: (200, {}, b"{}"), sleep=lambda s: None)
        h.get_json("https://x.example")
        h.get_json("https://x.example")
        with self.assertRaises(RuntimeError):
            h.get_json("https://x.example")

    def test_min_interval_between_calls(self):
        sleeps = []
        h = base.Http(min_interval_s=5, transport=lambda *a: (200, {}, b"{}"), sleep=sleeps.append)
        h.get_json("https://x.example")
        h.get_json("https://x.example")
        self.assertTrue(sleeps and sleeps[0] > 4)

    def test_oversize_response_and_gave_up_retries(self):
        h = base.Http(min_interval_s=0, transport=lambda *a: (200, {}, b"x" * (base.MAX_BODY_BYTES + 1)), sleep=lambda s: None)
        with self.assertRaises(RuntimeError):
            h.get_json("https://x.example")
        h = base.Http(min_interval_s=0, retries=1, transport=lambda *a: (500, {}, b""), sleep=lambda s: None)
        self.assertEqual(h.get_json("https://x.example"), (500, None))  # retried once, then the status is handed to the caller
        self.assertEqual(h.calls, 2)
        h = base.Http(min_interval_s=0, retries=1, transport=lambda *a: (_ for _ in ()).throw(OSError("down")), sleep=lambda s: None)
        with self.assertRaises(RuntimeError):
            h.get_json("https://x.example")

    def test_robots_parser(self):
        txt = "User-agent: *\nDisallow: /api/\n"
        self.assertTrue(base.robots_allows(txt, "https://ra.co/graphql"))
        self.assertFalse(base.robots_allows(txt, "https://ra.co/api/x"))


class RobotsEnforcement(unittest.TestCase):
    def mk(self, robots=(200, b"User-agent: *\nDisallow: /x"), **kw):
        seen = []

        def transport(method, url, hdrs, data, timeout):
            seen.append(url)
            return (robots[0], {}, robots[1]) if url.endswith("/robots.txt") else (200, {}, b"{}")

        return base.Http(min_interval_s=0, transport=transport, sleep=lambda s: None, **kw), seen

    def test_disallowed_path_raises_and_robots_is_cached(self):
        h, seen = self.mk(enforce_robots=True)
        with self.assertRaises(base.RobotsDisallowed):
            h.get_json("https://h.example/x/1")
        self.assertEqual(h.get_json("https://h.example/ok"), (200, {}))
        self.assertEqual(seen.count("https://h.example/robots.txt"), 1)

    def test_robots_404_or_error_fails_open(self):
        h, _ = self.mk(robots=(404, b""), enforce_robots=True)
        self.assertEqual(h.get_json("https://h.example/x/1"), (200, {}))
        h = base.Http(min_interval_s=0, retries=0, enforce_robots=True, sleep=lambda s: None,
                      transport=lambda m, u, *a: (_ for _ in ()).throw(OSError("down")) if u.endswith("/robots.txt") else (200, {}, b"{}"))
        self.assertEqual(h.get_json("https://h.example/x/1"), (200, {}))

    def test_off_by_default_and_for_not_applicable_sources(self):
        h, seen = self.mk()
        self.assertEqual(h.get_json("https://h.example/x/1"), (200, {}))
        self.assertEqual(seen, ["https://h.example/x/1"])
        self.assertFalse(run.http_for({"robots": {"verdict": "not_applicable"}}).enforce_robots)
        self.assertTrue(run.http_for({"robots": {"verdict": "allowed"}}).enforce_robots)


class Selection(unittest.TestCase):
    ROW = {"id": "00000000-0000-4000-8000-000000000001", "primary_slug": "a", "is_primary": True}

    def test_enabled_with_active_row_runs(self):
        (s, row, why), = run.select_sources([src("a")], [self.ROW])
        self.assertIsNone(why)
        self.assertEqual(row["id"], self.ROW["id"])

    def test_meta_off_switch(self):
        (_, _, why), = run.select_sources([src("a", enabled=False)], [self.ROW])
        self.assertIn("enabled=false", why)

    def test_env_off_switch(self):
        os.environ["OUTLIST_PRIMARY_A"] = "off"
        try:
            (_, _, why), = run.select_sources([src("a", env_switch="OUTLIST_PRIMARY_A")], [self.ROW])
        finally:
            del os.environ["OUTLIST_PRIMARY_A"]
        self.assertIn("switched off", why)

    def test_db_off_switch_means_no_active_row(self):
        (_, row, why), = run.select_sources([src("a")], [])
        self.assertIsNone(row)
        self.assertIn("no active", why)

    def test_only_filter(self):
        plan = run.select_sources([src("a"), src("b")], [self.ROW, dict(self.ROW, primary_slug="b")], only="b")
        self.assertEqual([p[0].slug for p in plan], ["b"])


class Posting(unittest.TestCase):
    def test_batches_sum_counters_and_retry_once(self):
        calls = []

        def post(url, headers, body, timeout):
            calls.append((headers["X-Auto-Approve"], len(json.loads(body))))
            if len(calls) == 1:
                raise TimeoutError("slow")
            return 200, json.dumps({"inserted": 2, "skipped_duplicates": 1})

        ok, summary = run.post_events("https://w.example", "s", [{"t": i} for i in range(3)], batch=2, post=post)
        self.assertTrue(ok, summary)
        self.assertEqual(calls[0], ("false", 2))  # primary events are never auto-approved by the runner
        self.assertIn("inserted=4", summary)

    def test_failed_batch_is_reported(self):
        ok, summary = run.post_events("https://w.example", "s", [{"t": 1}], post=lambda *a: (500, "boom"))
        self.assertFalse(ok)
        self.assertIn("FAILED", summary)

    def test_gateway_call_shape(self):
        seen = {}

        def post(url, headers, body, timeout):
            seen.update(url=url, secret=headers["X-Webhook-Secret"], body=json.loads(body))
            return 200, '{"sources":[]}'

        self.assertEqual(run.Gateway("https://g.example", "sec", post).call("primary_sources"), (200, {"sources": []}))
        self.assertEqual(seen["body"], {"action": "primary_sources"})
        self.assertEqual(seen["secret"], "sec")


class Registry(unittest.TestCase):
    GOOD = {"slug": "a", "name": "A", "region_slug": "amsterdam", "site_url": "https://a.example", "source_url": "https://a.example/x", "enabled": True,
            "robots": {"verdict": "allowed"}, "tos": {"verdict": "unclear"}, "risk": {"level": "low"}, "rate": {}}

    def test_good_meta_has_no_problems(self):
        self.assertEqual(registry.meta_problems(self.GOOD, "a"), [])

    def test_robots_disallow_blocks_the_source(self):
        bad = dict(self.GOOD, robots={"verdict": "disallowed"})
        self.assertTrue(any("disallows" in p for p in registry.meta_problems(bad, "a")))

    def test_missing_fields_and_bad_values(self):
        probs = registry.meta_problems({"slug": "b"}, "a")
        self.assertTrue(len(probs) >= 8)
        self.assertTrue(registry.meta_problems(dict(self.GOOD, source_url="http://a.example"), "a"))
        self.assertTrue(registry.meta_problems(dict(self.GOOD, risk={"level": "meh"}), "a"))

    def test_bad_slug_is_refused(self):
        for s in ("../x", "A", "a b", "1a"):
            with self.assertRaises(ValueError):
                registry.load(s)


if __name__ == "__main__":
    unittest.main()
