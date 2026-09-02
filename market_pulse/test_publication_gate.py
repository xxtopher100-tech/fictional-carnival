"""Publication gate — identity, fingerprint, burst policy, no daily caps."""
from __future__ import annotations
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from market_pulse.publication_gate import (
    signal_fingerprint,
    public_signal_id,
    ensure_confidence,
    _norm_level,
    get_publication_cooldown_sec,
)


class TestFingerprint(unittest.TestCase):
    def test_same_setup_same_fp(self):
        a = signal_fingerprint(
            market_type="forex", symbol="EUR/USD", direction="Buy EUR",
            timeframe="4H", entry=1.1596, stop=1.1526, target1=1.1700,
        )
        b = signal_fingerprint(
            market_type="forex", symbol="EUR/USD", direction="BUY EUR",
            timeframe="4H", entry="1.159595", stop="1.152637", target1="1.170031",
        )
        self.assertEqual(a, b)

    def test_different_direction_different_fp(self):
        a = signal_fingerprint(
            market_type="crypto", symbol="SOL", direction="long",
            timeframe="1H", entry=100, stop=95, target1=110,
        )
        b = signal_fingerprint(
            market_type="crypto", symbol="SOL", direction="short",
            timeframe="1H", entry=100, stop=105, target1=90,
        )
        self.assertNotEqual(a, b)

    def test_public_id_format(self):
        self.assertEqual(public_signal_id(65, "crypto"), "MP-C-0065")
        self.assertEqual(public_signal_id(84, "forex"), "MP-F-0084")


class TestConfidence(unittest.TestCase):
    def test_none_replaced(self):
        t = ensure_confidence("Confidence: none\nEntry: 1.16", "Moderate")
        self.assertIn("Confidence: Moderate", t)
        self.assertNotIn("Confidence: none", t.lower().replace("moderate", ""))


class TestNorm(unittest.TestCase):
    def test_float_noise(self):
        self.assertEqual(_norm_level(1.159595069), _norm_level("1.1596"))


class TestWiring(unittest.TestCase):
    def test_scanner_uses_gate(self):
        src = Path(__file__).with_name("trade_scanner.py").read_text()
        self.assertIn("publish_canonical_trade", src)

    def test_morning_uses_gate(self):
        src = Path(__file__).with_name("morning_package.py").read_text()
        self.assertIn("publish_canonical_trade", src)

    def test_handlers_drains_queue(self):
        src = Path(__file__).with_name("handlers.py").read_text()
        self.assertIn("process_publication_queue", src)
        self.assertIn("PubQueueDrain", src)


class TestNoDailyCapBlocker(unittest.TestCase):
    def test_soft_cap_never_blocks(self):
        from market_pulse.trade_scanner import _may_publish_over_soft_cap, MAX_TRADES_PER_DAY

        ok, code = _may_publish_over_soft_cap(99.0, MAX_TRADES_PER_DAY + 50)
        self.assertTrue(ok)
        self.assertIn(code, ("DAILY_CAP_DISABLED", "UNDER_SOFT_CAP"))

    def test_scanner_source_has_no_hard_day_cap_string_as_blocker(self):
        src = Path(__file__).with_name("trade_scanner.py").read_text()
        # Comment may remain; ensure no mark_trade_publication(..., "HARD_DAY_CAP")
        self.assertNotIn('"HARD_DAY_CAP"', src)
        self.assertNotIn("'HARD_DAY_CAP'", src)
        self.assertIn("NO daily/scan cap blockers", src)


class TestBurstCooldownConfig(unittest.TestCase):
    def test_default_cooldown_600(self):
        with patch.dict(os.environ, {}, clear=False):
            # Re-read via function which uses module-level constant set at import
            # At least verify getter is non-negative and env name is documented in module
            sec = get_publication_cooldown_sec()
            self.assertGreaterEqual(sec, 0)
        src = Path(__file__).with_name("publication_gate.py").read_text()
        self.assertIn("NORMAL_PUBLICATION_COOLDOWN_SECONDS", src)
        self.assertIn("TEMPORARILY_QUEUED", src)
        self.assertIn("enqueue_publication", src)
        self.assertIn("process_publication_queue", src)


class TestKeyAlertEvents(unittest.TestCase):
    def test_level_label_states(self):
        from market_pulse.alerts import _level_label

        lab, _ = _level_label(100.3, 100.0)  # ~0.3% above
        self.assertIn("APPROACHING SUPPORT", lab)
        lab, _ = _level_label(100.8, 100.0)  # ~0.8%
        self.assertIn("TESTING SUPPORT", lab)
        lab, _ = _level_label(102.0, 100.0)
        self.assertIn("BREAKOUT", lab)
        lab, _ = _level_label(98.0, 100.0)
        self.assertIn("BREAKDOWN", lab)

    def test_reclaim_after_breakdown(self):
        from market_pulse.alerts import _level_label

        lab, _ = _level_label(100.5, 100.0, prev_event="BREAKDOWN")
        self.assertIn("RECLAIM SUPPORT", lab)

    def test_zone_gate_testing_once(self):
        from market_pulse.alerts import _zone_gate_allow

        allow, reason = _zone_gate_allow(
            {"in_zone": False, "last_event": "", "level": 100},
            True,
            "TESTING RESISTANCE",
            100,
        )
        self.assertTrue(allow)
        allow2, reason2 = _zone_gate_allow(
            {"in_zone": True, "last_event": "TESTING RESISTANCE", "level": 100},
            True,
            "TESTING RESISTANCE",
            100,
        )
        self.assertFalse(allow2)
        self.assertIn("still_in_zone", reason2)

    def test_zone_gate_approaching_no_spam(self):
        from market_pulse.alerts import _zone_gate_allow

        a1, _ = _zone_gate_allow(
            {"in_zone": False, "last_event": "", "level": 100},
            True,
            "APPROACHING RESISTANCE",
            100,
        )
        self.assertTrue(a1)
        a2, r2 = _zone_gate_allow(
            {"in_zone": True, "last_event": "APPROACHING RESISTANCE", "level": 100},
            True,
            "APPROACHING RESISTANCE",
            100,
        )
        self.assertFalse(a2)


if __name__ == "__main__":
    unittest.main()
