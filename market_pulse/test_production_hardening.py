"""Production hardening tests — no live network required."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

# Ensure package importable
sys.path.insert(0, str(__file__).rsplit("/market_pulse", 1)[0] if "/market_pulse" in __file__ else ".")


class TestConfigValidation(unittest.TestCase):
    def test_missing_token_detected(self):
        with mock.patch.dict(os.environ, {"BOT_TOKEN": "YOUR_BOT_TOKEN_HERE", "DATABASE_URL": "postgresql://u:p@h/db"}, clear=False):
            # re-import pattern: call function with patched module attrs
            from market_pulse import config_runtime as cr
            old_tok, old_db = cr.BOT_TOKEN, cr.DATABASE_URL
            try:
                cr.BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
                cr.DATABASE_URL = "postgresql://u:p@h/db"
                m = cr.validate_critical_config()
                self.assertIn("BOT_TOKEN", m)
                self.assertNotIn("DATABASE_URL", m)
            finally:
                cr.BOT_TOKEN, cr.DATABASE_URL = old_tok, old_db

    def test_missing_db_detected(self):
        from market_pulse import config_runtime as cr
        old_tok, old_db = cr.BOT_TOKEN, cr.DATABASE_URL
        try:
            cr.BOT_TOKEN = "123456:ABC-real-looking-token-value"
            cr.DATABASE_URL = ""
            m = cr.validate_critical_config()
            self.assertIn("DATABASE_URL", m)
        finally:
            cr.BOT_TOKEN, cr.DATABASE_URL = old_tok, old_db

    def test_ok_when_both_set(self):
        from market_pulse import config_runtime as cr
        old_tok, old_db = cr.BOT_TOKEN, cr.DATABASE_URL
        try:
            cr.BOT_TOKEN = "123456:ABC-real-looking-token-value"
            cr.DATABASE_URL = "postgresql://u:p@localhost/mp"
            self.assertEqual(cr.validate_critical_config(), [])
        finally:
            cr.BOT_TOKEN, cr.DATABASE_URL = old_tok, old_db


class TestAINarrativeLock(unittest.TestCase):
    def test_sanitize_strips_naira_pressure(self):
        from market_pulse.ai_narrative_guard import sanitize_ai_narrative
        out = sanitize_ai_narrative("EUR strength could ease naira pressure if sustained.")
        self.assertNotIn("naira pressure", out.lower())

    def test_lock_confidence(self):
        from market_pulse.ai_narrative_guard import lock_levels_and_confidence_in_text
        text = "Entry: $1.159595069401765\nConfidence: High"
        locked = lock_levels_and_confidence_in_text(
            text, entry=1.1596, stop=1.15, target=1.17, confidence="Moderate"
        )
        self.assertIn("Moderate", locked)
        self.assertNotIn("1.159595069401765", locked)

    def test_format_level(self):
        from market_pulse.ai_narrative_guard import format_level_for_prompt
        self.assertNotIn("159595069", format_level_for_prompt(1.159595069401765))


class TestScanSchemaText(unittest.TestCase):
    def test_indexes_in_schema(self):
        from pathlib import Path
        src = Path(__file__).with_name("trade_engine_report.py").read_text()
        self.assertIn("idx_scan_cand_run", src)
        self.assertIn("build_ops_diagnostic_report", src)


class TestStartupImport(unittest.TestCase):
    def test_handlers_imports_validate(self):
        src = open(__file__.replace("test_production_hardening.py", "handlers.py")).read()
        self.assertIn("validate_critical_config", src)
        self.assertIn("opsreport", src)


if __name__ == "__main__":
    unittest.main()
