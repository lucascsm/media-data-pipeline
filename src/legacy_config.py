"""Configuration for archived workflows; no production defaults."""
import os
from pathlib import Path
from src.pipeline_utils import load_yaml_config
from src.utils_do_pai import CONFIG_DIR


def load_legacy_settings() -> dict:
    path = Path(os.getenv("LEGACY_CONFIG_PATH", str(CONFIG_DIR / "legacy.yaml")))
    if not path.exists():
        raise FileNotFoundError("Copy config/legacy.example.yaml to legacy.yaml and configure your own values")
    return load_yaml_config(path)


def legacy_rule(name: str) -> str:
    """Resolve an archived label or custom event from the local rule model."""
    return str(load_legacy_settings()["rules"][name])
