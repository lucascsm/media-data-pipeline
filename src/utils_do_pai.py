"""Runtime paths shared by the local runner and the Cloud Run job."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", str(PROJECT_ROOT / "config")))
GCS_MOUNT = Path(os.getenv("GCS_MOUNT", str(PROJECT_ROOT / "data")))
OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", str(GCS_MOUNT / "output")))
PIPELINE_TIMEZONE = os.getenv("PIPELINE_TIMEZONE", "America/Sao_Paulo")
CONFIG_GOOGLE_PATH = CONFIG_DIR / "google.yaml"
CONFIG_META_PATH = CONFIG_DIR
CONFIG_TIKTOK_PATH = CONFIG_DIR / "tiktok.yaml"
CONFIG_KWAI_PATH = CONFIG_DIR / "kwai.yaml"
BACKUP_GADS_PATH = OUTPUT_PATH / "backup"
BACKUP_META_PATH = OUTPUT_PATH / "backup"
BACKUP_TIKTOK_PATH = OUTPUT_PATH / "backup"
BACKUP_KWAI_PATH = OUTPUT_PATH / "backup"
AD_ASSETS_PATH_GOOGLE = GCS_MOUNT / "ad_media" / "google"
AD_ASSETS_PATH_META = GCS_MOUNT / "ad_media" / "meta"
AD_ASSETS_PATH_TIKTOK = GCS_MOUNT / "ad_media" / "tiktok"
# Provider endpoints are retained from the collectors.
TIKTOK_API_BASE = os.getenv("TIKTOK_API_BASE", "https://business-api.tiktok.com/open_api/v1.3")
KWAI_API_BASE = os.getenv("KWAI_API_BASE", "https://developers.kwai.com")
KWAI_OAUTH_BASE = os.getenv("KWAI_OAUTH_BASE", "https://business.kwai.com")
