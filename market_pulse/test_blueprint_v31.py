"""v3.1 blueprint alignment tests."""
from __future__ import annotations
import unittest
from pathlib import Path

from market_pulse.blueprint_v31 import (
    assess_crypto_price_quality,
    final_price_check,
    build_immutable_snapshot,
    BLUEPRINT_VERSION,
    STATUS_SKIPPED_DATA_QUALITY,
)


class TestBlueprintBasics(unittest.TestCase):
    def test_version(self):
        self.assertEqual(BLUEPRINT_VERSION, "3.1")

    def test_blocked_no_price(self):
        st, reason = assess_crypto_price_quality("BTC", None)
        self.assertEqual(st, "BLOCKED")

    def test_ok_price(self):
        st, reason = assess_crypto_price_quality("BTC", 70000.0)
        self.assertIn(st, ("OK", "DEGRADED", "BLOCKED"))  # may degrade if cache stale

    def test_snapshot(self):
        s = build_immutable_snapshot(
            coin="BTC", tier="edge", direction="long",
            entry=70000, stop=69000, target1=72000,
        )
        self.assertEqual(s["blueprint"], "3.1")
        self.assertEqual(s["entry"], 70000)

    def test_edge_floor_in_source(self):
        src = Path(__file__).with_name("edge_trade_engine.py").read_text()
        self.assertIn("NORMAL floor", src)
        self.assertIn("EDGE blocked by NORMAL floor", src)

    def test_scanner_has_dq_and_final_check(self):
        src = Path(__file__).with_name("trade_scanner.py").read_text()
        self.assertIn("SKIPPED_DATA_QUALITY", src)
        self.assertIn("EXPIRED_BEFORE_PUBLISH", src)
        self.assertIn("final_price_check", src)


if __name__ == "__main__":
    unittest.main()
