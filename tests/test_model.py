from __future__ import annotations
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl
from src.account_catalog import load_account_catalog
from src.event_long_format import to_long_event_frame
from src import google_ads, metads, tiktok, kwai
from src.main import iterar_blocos_datas
from scripts.deploy_job import build_plan, main as deploy_main, validate_live_config
from scripts.render_sql import render_tree

ROOT = Path(__file__).resolve().parents[1]


class ModelTests(unittest.TestCase):
    def test_catalog_rejects_ambiguous_product_join(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "accounts.yaml"
            path.write_text("accounts:\n- platform: meta\n  account_id: act_demo\n  product: A\n- platform: meta\n  account_id: demo\n  product: B\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_account_catalog(path)

    def test_catalog_preserves_products_and_account_option(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "accounts.yaml"
            path.write_text("accounts:\n- platform: google\n  account_id: demo\n  product: Produto A\n  aggregate_conversions: true\n", encoding="utf-8")
            rows = load_account_catalog(path)
            self.assertEqual(rows[0]["product"], "Produto A")
            self.assertTrue(rows[0]["aggregate_conversions"])

    def test_events_do_not_include_performance_metrics(self):
        frame = pl.DataFrame({"id": ["demo"], "spend": [30.0], "lead": [2.0], "purchase": [1.0], "zero": [0.0]})
        result = to_long_event_frame(frame, ["id", "spend"])
        self.assertEqual(result.height, 2)
        self.assertEqual(set(result["evento"]), {"lead", "purchase"})
        self.assertEqual(result["valor"].sum(), 3.0)

    def test_dates_cover_boundary_once(self):
        self.assertEqual(iterar_blocos_datas("2025-01-01", "2025-01-08", "%Y-%m-%d", 7),
                         [("2025-01-01", "2025-01-07"), ("2025-01-08", "2025-01-08")])

    def test_reprocessing_replaces_metrics_for_all_providers(self):
        # Local Parquet writes only. No API method is called.
        for module, backup_setting in ((google_ads, "BACKUP_GADS_PATH"), (metads, "BACKUP_META_PATH"),
                                       (tiktok, "BACKUP_TIKTOK_PATH"), (kwai, "BACKUP_KWAI_PATH")):
            with self.subTest(provider=module.__name__), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "facts.parquet"
                backup = Path(folder) / "backup"
                backup.mkdir()
                base = {"data": "2025-01-01", "nivel": "campanha", "id_cliente": "demo-account",
                        "id_campanha": "demo-campaign", "id_grupo_anuncio": "demo-group", "id_anuncio": "demo-ad",
                        "spend": 10.0, "impressoes": 100, "cliques": 10}
                changed = {**base, "spend": 20.0}
                other_account = {**base, "id_cliente": "another-demo-account"}
                with patch.object(module, backup_setting, backup), contextlib.redirect_stdout(io.StringIO()):
                    module.inserir_substituir(pl.DataFrame([base, other_account]), str(path))
                    module.inserir_substituir(pl.DataFrame([changed]), str(path))
                    module.inserir_substituir(pl.DataFrame([changed]), str(path))
                result = pl.read_parquet(path)
                self.assertEqual(result.height, 2)
                self.assertEqual(result["spend"].sum(), 30.0)

    def test_empty_extract_preserves_existing_meta_tiktok_kwai_file(self):
        for module in (metads, tiktok, kwai):
            with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
                path = Path(folder) / "existing.parquet"
                pl.DataFrame({"marker": ["preserve"]}).write_parquet(path)
                before = path.read_bytes()
                module.inserir_substituir(pl.DataFrame(), str(path))
                self.assertEqual(path.read_bytes(), before)

    def test_default_deploy_never_invokes_cloud_cli(self):
        with patch("sys.argv", ["deploy_job.py"]), patch("subprocess.run") as command, contextlib.redirect_stdout(io.StringIO()):
            deploy_main()
        command.assert_not_called()

    def test_modes_and_config_secrets(self):
        config = json.loads((ROOT / "config/cloud.example.json").read_text())
        daily = build_plan(config, "daily", None, None, "google")
        self.assertIn("scheduler_create", daily)
        self.assertTrue(any(arg.startswith("--set-secrets=/app/config/") for arg in daily["job_create"]))
        reprocess = build_plan(config, "reprocess", "2025-01-01", "2025-01-07", "google")
        self.assertNotIn("scheduler_create", reprocess)
        self.assertTrue(any("src.main_reprocess" in arg for arg in reprocess["job_create"]))
        with self.assertRaises(ValueError):
            validate_live_config(config)

    def test_sql_templates_resolve_all_parameters(self):
        config = json.loads((ROOT / "config/cloud.example.json").read_text())
        with tempfile.TemporaryDirectory() as folder:
            count = render_tree(config, Path(folder))
            self.assertGreater(count, 50)
            for path in Path(folder).rglob("*.sql"):
                self.assertNotIn("${", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
