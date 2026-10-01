from __future__ import annotations
import os
import unicodedata
from pathlib import Path
from src.pipeline_utils import load_yaml_config
from src.utils_do_pai import CONFIG_DIR


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value).strip())
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower()


def normalize_platform(platform: str | None) -> str:
    return _normalize_text(platform)


def normalize_account_id(account_id: str | None) -> str:
    value = str(account_id or "").strip()
    return value[4:] if value.lower().startswith("act_") else value


def load_account_catalog(path: Path) -> tuple[dict, ...]:
    """Require unambiguous platform/account/product configuration."""
    if not path.exists():
        return ()
    config = load_yaml_config(path)
    result, seen = [], set()
    for row in config.get("accounts", []):
        platform = normalize_platform(row.get("platform"))
        account_id = str(row.get("account_id", "")).strip()
        product = str(row.get("product", "")).strip()
        if platform not in {"google", "meta", "tiktok", "kwai"} or not account_id or not product:
            raise ValueError("Each account needs a valid platform, account_id and product")
        if "REPLACE_" in account_id:
            raise ValueError("Configure real account IDs before using the account catalog")
        if not isinstance(row.get("aggregate_conversions", False), bool):
            raise ValueError("aggregate_conversions must be a YAML boolean")
        key = platform, normalize_account_id(account_id)
        if key in seen:
            raise ValueError("Duplicate platform/account mapping")
        seen.add(key)
        result.append({**row, "platform": platform, "account_id": account_id, "product": product})
    return tuple(result)


CATALOG_PATH = Path(os.getenv("ACCOUNT_CATALOG_PATH", str(CONFIG_DIR / "accounts.yaml")))
ACCOUNT_CATALOG = load_account_catalog(CATALOG_PATH)
_config = load_yaml_config(CATALOG_PATH) if CATALOG_PATH.exists() else {}
PRODUCT_ALIASES = {_normalize_text(k): v for k, v in _config.get("product_aliases", {}).items()}
CONVERSION_AGGREGATION_ACCOUNT_IDS = {
    normalize_account_id(row["account_id"]) for row in ACCOUNT_CATALOG
    if row["platform"] == "google" and row.get("aggregate_conversions", False)
}


def normalize_product(product: str | None) -> str | None:
    if product is None:
        return None
    return PRODUCT_ALIASES.get(_normalize_text(product)) or str(product).strip()


def list_accounts(platform: str) -> list[str]:
    return [item["account_id"] for item in ACCOUNT_CATALOG if item["platform"] == normalize_platform(platform)]


def get_product_for_account(account_id: str | None, platform: str | None = None) -> str | None:
    target = normalize_account_id(account_id)
    for item in ACCOUNT_CATALOG:
        if platform and item["platform"] != normalize_platform(platform):
            continue
        if normalize_account_id(item["account_id"]) == target:
            return item["product"]
    return None
