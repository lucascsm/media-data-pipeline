import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from scripts.configure import ROOT, initialize, check_config
from scripts.deploy_job import validate_live_config


class ConfigurationTests(unittest.TestCase):
    def test_initialize_preserves_existing_user_configuration(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder)
            initialize(config, ["google"])
            path = config / "google.yaml"
            path.write_text("user_settings: preserve\n", encoding="utf-8")
            before = path.read_bytes()
            initialize(config, ["google"])
            self.assertEqual(before, path.read_bytes())
            self.assertFalse((config / "meta.yaml").exists())

    def test_missing_parameters_report_fields_without_values(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder)
            initialize(config, ["google"])
            with patch.dict(os.environ, {"ACCOUNT_CATALOG_PATH": str(config / "accounts.yaml")}):
                findings = check_config(config, ["google"])
            self.assertTrue(any("refresh_token" in message for message in findings))
            self.assertTrue(any("pelo menos uma conta" in message for message in findings))
            self.assertNotIn("REPLACE_WITH", "\n".join(findings))

    def test_check_selected_channel_does_not_require_other_providers_or_network(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder)
            initialize(config, ["google"])
            settings = {key: "example-value" for key in ("developer_token", "client_id", "client_secret", "refresh_token")}
            settings["use_proto_plus"] = True
            (config / "google.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")
            (config / "accounts.yaml").write_text(yaml.safe_dump({"accounts": [{"platform": "google", "account_id": "demo-account", "product": "Cursos"}]}), encoding="utf-8")
            with patch.dict(os.environ, {"ACCOUNT_CATALOG_PATH": str(config / "accounts.yaml")}), patch("subprocess.run") as command:
                self.assertEqual(check_config(config, ["google"]), [])
            command.assert_not_called()

    def test_invalid_yaml_does_not_reveal_credential_contents(self):
        with tempfile.TemporaryDirectory() as folder:
            config = Path(folder)
            initialize(config, ["google"])
            marker = "example-value-do-not-print"
            (config / "google.yaml").write_text(yaml.safe_dump({"client_secret": marker}) + "bad: [\n", encoding="utf-8")
            with patch.dict(os.environ, {"ACCOUNT_CATALOG_PATH": str(config / "accounts.yaml")}):
                findings = check_config(config, ["google"])
            self.assertNotIn(marker, "\n".join(findings))
            self.assertTrue(any("YAML invalido" in value for value in findings))

    def test_cloud_validation_only_requires_secrets_of_selected_channels(self):
        config = json.loads((ROOT / "config/cloud.example.json").read_text(encoding="utf-8"))
        config.update(project_id="example-media-project", data_bucket="example-media-data")
        config["config_secrets"].update({"google.yaml": "example-google-config:latest", "accounts.yaml": "example-accounts:latest"})
        validate_live_config(config, channels="google")
        with self.assertRaises(ValueError):
            validate_live_config(config, channels="google,meta")


if __name__ == "__main__":
    unittest.main()
