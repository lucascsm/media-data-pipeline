"""Initialize and check reusable configuration locally, without API calls."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.deploy_job import validate_live_config

PLATFORMS = {"google", "meta", "tiktok", "kwai"}
REQUIRED_FIELDS = {
    "google": ("developer_token", "client_id", "client_secret", "refresh_token"),
    "meta": ("app_id", "app_secret", "access_token"),
    "tiktok": ("access_token",),
}


def initialize(config_dir: Path, channels: list[str]) -> list[str]:
    """Create local copies of templates without overwriting existing values."""
    config_dir.mkdir(parents=True, exist_ok=True)
    names = [(f"{name}.example.yaml", f"{name}.yaml") for name in channels]
    names += [("accounts.example.yaml", "accounts.yaml"),
              ("cloud.example.json", "cloud.local.json"),
              ("depara_visao_eventos.example.csv", "depara_visao_eventos.local.csv")]
    messages = []
    for example, local in names:
        content = (ROOT / "config" / example).read_text(encoding="utf-8")
        try:
            with (config_dir / local).open("x", encoding="utf-8", newline="") as stream:
                stream.write(content)
            messages.append(f"Criado: {local}")
        except FileExistsError:
            messages.append(f"Preservado: {local}")
    return messages


def _read_yaml(path: Path, findings: list[str]) -> dict | None:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8-sig")) or {}
    except FileNotFoundError:
        findings.append(f"{path.name}: arquivo ausente; execute --init.")
        return None
    except (yaml.YAMLError, UnicodeError, OSError):
        # Parser messages may include credential values; report only the file.
        findings.append(f"{path.name}: YAML invalido ou arquivo ilegivel.")
        return None
    if not isinstance(value, dict):
        findings.append(f"{path.name}: a configuracao deve ser um mapa de campos.")
        return None
    return value


def _filled(value) -> bool:
    text = "" if value is None else str(value).strip()
    return bool(text) and "REPLACE_" not in text and not text.startswith("YOUR_")


def check_config(config_dir: Path, channels: list[str], cloud: bool = False) -> list[str]:
    """Check selected channels only; never authenticate or print config values."""
    findings = []
    catalog_path = Path(os.getenv("ACCOUNT_CATALOG_PATH", str(config_dir / "accounts.yaml")))
    catalog = _read_yaml(catalog_path, findings)
    enabled, seen = set(), set()
    if catalog is not None:
        rows = catalog.get("accounts", [])
        if not isinstance(rows, list):
            findings.append("accounts.yaml: accounts deve ser uma lista.")
            rows = []
        for index, row in enumerate(rows, 1):
            if not isinstance(row, dict):
                findings.append(f"accounts.yaml: entrada {index} deve conter platform, account_id e product.")
                continue
            platform = str(row.get("platform", "")).strip().lower()
            identifier = str(row.get("account_id", "")).strip()
            if platform not in PLATFORMS or not _filled(row.get("account_id")) or not _filled(row.get("product")):
                findings.append(f"accounts.yaml: preencha platform, account_id e product na entrada {index}.")
                continue
            normalized = identifier[4:] if identifier.lower().startswith("act_") else identifier
            key = platform, normalized
            if key in seen:
                findings.append(f"accounts.yaml: mapeamento plataforma/conta repetido na entrada {index}.")
            seen.add(key)
            enabled.add(platform)
            if not isinstance(row.get("aggregate_conversions", False), bool):
                findings.append(f"accounts.yaml: aggregate_conversions deve ser true/false na entrada {index}.")
        for platform in channels:
            if platform not in enabled:
                findings.append(f"accounts.yaml: configure pelo menos uma conta para {platform}.")

    for platform in channels:
        path = config_dir / f"{platform}.yaml"
        settings = _read_yaml(path, findings)
        if settings is None:
            continue
        for field in REQUIRED_FIELDS.get(platform, ()):
            if not _filled(settings.get(field)):
                findings.append(f"{path.name}: preencha {field}.")
        if platform == "google":
            if settings.get("use_proto_plus") is not True:
                findings.append("google.yaml: configure use_proto_plus: true, como no modelo.")
            manager = settings.get("login_customer_id")
            if manager and not _filled(manager):
                findings.append("google.yaml: preencha login_customer_id ou remova o campo se nao usar conta gerente.")
        if platform == "kwai":
            for field in ("access_token", "refresh_token", "client_id", "secret_key", "corp_id", "report_account_id"):
                settings[field] = os.getenv("KWAI_" + field.upper()) or settings.get(field)
            refresh = all(_filled(settings.get(field)) for field in ("refresh_token", "client_id", "secret_key"))
            if not _filled(settings.get("access_token")) and not refresh:
                findings.append("kwai.yaml: preencha access_token ou refresh_token + client_id + secret_key.")
            for field in ("refresh_token", "client_id", "secret_key", "corp_id", "report_account_id"):
                if settings.get(field) and not _filled(settings[field]):
                    findings.append(f"kwai.yaml: preencha {field} ou deixe vazio se nao utilizar.")

    if cloud:
        path = config_dir / "cloud.local.json"
        try:
            settings = json.loads(path.read_text(encoding="utf-8-sig"))
            validate_live_config(settings, channels=",".join(channels))
            product = str(settings.get("special_product", "Produto C")).strip().lower()
            normalized = str(settings.get("special_product_normalized", product)).strip()
            if product != normalized:
                findings.append("cloud.local.json: special_product_normalized deve ser special_product em minusculas.")
        except (OSError, UnicodeError, json.JSONDecodeError):
            findings.append("cloud.local.json: arquivo ausente, invalido ou ilegivel.")
        except (ValueError, KeyError, TypeError):
            findings.append("cloud.local.json: revise projeto, bucket, nomes de recursos e secrets dos canais selecionados.")
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--init", action="store_true")
    action.add_argument("--check", action="store_true")
    parser.add_argument("--channels", default="google,meta,tiktok,kwai")
    parser.add_argument("--config-dir", type=Path, default=Path(os.getenv("CONFIG_DIR", str(ROOT / "config"))))
    parser.add_argument("--cloud", action="store_true", help="Also check cloud.local.json and selected Secret Manager references")
    args = parser.parse_args()
    channels = list(dict.fromkeys(value.strip().lower() for value in args.channels.split(",") if value.strip()))
    if not channels or any(value not in PLATFORMS for value in channels):
        parser.error("Choose channels from google,meta,tiktok,kwai")
    if args.init:
        print("\n".join(initialize(args.config_dir, channels)))
        print("Preencha seus valores nos arquivos locais e execute --check.")
        return
    findings = check_config(args.config_dir, channels, args.cloud)
    if findings:
        print("\n".join(findings))
        raise SystemExit(1)
    print("Configuracao local validada. Autenticacao e disponibilidade das APIs nao foram testadas.")


if __name__ == "__main__":
    main()
