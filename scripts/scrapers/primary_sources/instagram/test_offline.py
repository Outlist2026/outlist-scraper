"""Offline test for the instagram wrapper: handle allow-list, environment handed to S5's runner, fail-closed behaviour. No network, S5's module is stubbed."""

from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace

from .. import base
from . import client, mapper

HERE = os.path.dirname(os.path.abspath(__file__))
META = json.loads(base.read_text(os.path.join(HERE, "meta.json")))
FX = json.loads(base.read_text(os.path.join(HERE, "fixtures", "delegate_env.json")))


class InstagramWrapper(unittest.TestCase):
    def test_handles_are_normalised_deduped_and_validated(self):
        self.assertEqual(client.handles_from(FX), FX["expected_env"]["OUTLIST_HANDLES"].split(","))

    def test_run_passes_only_the_allow_list_and_direct_mode(self):
        env, seen = {}, {}
        mod = SimpleNamespace(main=lambda argv: seen.update(env=dict(env), argv=argv) or 0)
        rc, summary = client.run(FX, dry_run=True, environ=env, module=mod)
        self.assertEqual(rc, 0, summary)
        for k, v in FX["expected_env"].items():
            self.assertEqual(seen["env"][k], v)

    def test_no_handles_fails_closed_and_never_calls_the_scraper(self):
        called = []
        mod = SimpleNamespace(main=lambda argv: called.append(1) or 0)
        rc, _ = client.run({"params": {"handles": ["not a handle"]}}, environ={}, module=mod)
        self.assertEqual(rc, 1)
        self.assertEqual(called, [])

    def test_nonzero_exit_is_returned(self):
        mod = SimpleNamespace(main=lambda argv: 1)
        self.assertEqual(client.run(FX, environ={}, module=mod)[0], 1)

    def test_ships_off_with_a_high_risk_note_and_no_mapper_events(self):
        self.assertFalse(META["enabled"])
        self.assertEqual(META["risk"]["level"], "high")
        self.assertEqual(mapper.map_events([{"x": 1}], META), [])
        with self.assertRaises(RuntimeError):
            client.fetch(None, META)

    def test_real_instagram_scraper_understands_what_the_wrapper_hands_over(self):
        import importlib

        mod = importlib.import_module("instagram_scraper")  # S5's runner, same folder level as primary_sources/
        self.assertTrue(callable(mod.main))
        self.assertEqual(mod.parse_handles(",".join(client.handles_from(FX))), FX["expected_env"]["OUTLIST_HANDLES"].split(","))


if __name__ == "__main__":
    unittest.main()
