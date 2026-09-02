"""Publication gate — identity, fingerprint, confidence consistency."""
from __future__ import annotations
import unittest

from market_pulse.publication_gate import (
    signal_fingerprint,
    public_signal_id,
    ensure_confidence,
    _norm_level,
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
        from pathlib import Path
        src = Path(__file__).with_name("trade_scanner.py").read_text()
        self.assertIn("publish_canonical_trade", src)

    def test_morning_uses_gate(self):
        from pathlib import Path
        src = Path(__file__).with_name("morning_package.py").read_text()
        self.assertIn("publish_canonical_trade", src)


if __name__ == "__main__":
    unittest.main()
