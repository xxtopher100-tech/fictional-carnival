"""Live output integrity tests — no network."""
from __future__ import annotations
import unittest
from pathlib import Path

# Load edge disclaimer helpers without full package DB dependency
_edge_src = Path(__file__).with_name("edge_trade_engine.py").read_text()
_ns = {}
_start = _edge_src.find("EDGE_DISCLAIMER")
_end = _edge_src.find("def _gather_trade_analytics")
exec(_edge_src[_start:_end], _ns)

from market_pulse.message_integrity import (
    strip_spurious_naira_from_crypto_text,
    count_standard_footers,
)


class TestFooter(unittest.TestCase):
    def test_exactly_one_footer_after_finalize(self):
        body = "Setup body\n\n" + _ns["STANDARD_DISCLAIMER"] + "\n\n" + _ns["STANDARD_DISCLAIMER"]
        out = _ns["_finalize_trade_message"](body, "steady")
        self.assertEqual(out.count("Illustrative only"), 1)
        self.assertEqual(out.count("Market Pulse Pro"), 1)

    def test_count_helper(self):
        self.assertEqual(count_standard_footers("Illustrative only ... Illustrative only"), 2)


class TestCurrency(unittest.TestCase):
    def test_btc_level_not_naira(self):
        bad = "Entry: ₦77265.79 Stop: ₦75894.41 Target: ₦78180.04"
        fixed = strip_spurious_naira_from_crypto_text(bad)
        self.assertNotIn("₦77265", fixed)
        self.assertIn("77265.79", fixed)

    def test_p2p_rate_kept(self):
        ok = "USDT/NGN Buy ₦1500 / Sell ₦1480"
        fixed = strip_spurious_naira_from_crypto_text(ok)
        self.assertIn("₦1500", fixed)


class TestPatternConflict(unittest.TestCase):
    def test_signal_engine_conflict_logic_in_source(self):
        src = Path(__file__).with_name("signal_engine.py").read_text()
        self.assertIn("Conflicting chart patterns", src)
        self.assertIn("aligned_charts", src)


class TestSimilarSetupSource(unittest.TestCase):
    def test_scanner_has_similar_gate(self):
        src = Path(__file__).with_name("trade_scanner.py").read_text()
        self.assertIn("SIMILAR_ACTIVE_SETUP", src)


if __name__ == "__main__":
    unittest.main()
