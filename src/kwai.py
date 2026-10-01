from __future__ import annotations

import os
import shutil
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.account_catalog import get_product_for_account, list_accounts
from src.pipeline_utils import align_and_concat_frames, load_yaml_config, resolve_selected_ids
from src.utils_do_pai import BACKUP_KWAI_PATH, CONFIG_KWAI_PATH, KWAI_API_BASE, KWAI_OAUTH_BASE, OUTPUT_PATH


KWAI_DEFAULT_TZ = timezone.utc
KWAI_OUTPUT_FILE = os.path.join(str(OUTPUT_PATH), "dados_kwai_all_new.parquet")
KWAI_ACCOUNTS = list_accounts("kwai")

KWAI_REPORT_LEVELS = {
    "campanha": {
        "endpoint": "/rest/n/mapi/report/dspCampaignEffectQuery",
        "id_field": "campaignId",
        "name_field": "campaignName",
    },
    "anuncio": {
        "endpoint": "/rest/n/mapi/report/dspCreativeEffectQuery",
        "id_field": "creativeId",
        "name_field": "creativeName",
    },
}

KWAI_BASE_COLUMNS = [
    "data",
    "nivel",
    "produto",
    "id_cliente",
    "account_id",
    "account_name",
    "campaign_id",
    "id_campanha",
    "nome_campanha",
    "unit_id",
    "id_grupo_anuncio",
    "grupo_anuncio",
    "creative_id",
    "id_anuncio",
    "anuncio",
    "spend",
    "impressoes",
    "cliques",
    "ctr",
    "cpc",
    "cpm",
    "cpa",
    "alcance",
    "result",
    "action",
    "evento",
    "valor",
    "currency",
]

KWAI_EVENT_EXCLUDED_FIELDS = {
    "accountId",
    "accountName",
    "adId",
    "adName",
    "adgroupId",
    "adgroupName",
    "biddingType",
    "buyingType",
    "campaignId",
    "campaignName",
    "clientId",
    "click",
    "clicks",
    "conversion",
    "conversions",
    "convertedTaxId",
    "cost",
    "countryCode",
    "cpa",
    "cpc",
    "cpm",
    "creativeId",
    "creativeName",
    "ctr",
    "currency",
    "cvr",
    "date",
    "exposure",
    "frequency",
    "frequencia",
    "id",
    "impression",
    "impressions",
    "photoAvgPlayTime",
    "reach",
    "reachCount",
    "reach_count",
    "reportDate",
    "report_date",
    "show",
    "spend",
    "statDate",
    "stat_date",
    "time",
    "totalCost",
    "total_cost",
    "unitId",
    "unitName",
    "unit_id",
    "unit_name",
}
KWAI_EVENT_EXCLUDED_LOWER = {field.lower() for field in KWAI_EVENT_EXCLUDED_FIELDS}
KWAI_EVENT_EXCLUDED_PREFIXES = ("cost", "stimecost")
KWAI_EVENT_EXCLUDED_SUFFIXES = ("rate",)


def _candidate_config_paths(path: str | Path) -> list[Path]:
    requested = Path(path)
    repo_root = Path(__file__).resolve().parents[1]
    candidates = [requested]
    candidates.extend([
        repo_root / "config" / "kwai.yaml",
        repo_root / "config" / "local_kwai.yaml",
        Path("config") / "kwai.yaml",
        Path("config") / "local_kwai.yaml",
    ])
    unique: list[Path] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def carregar_config_kwai(caminho_config: str | Path = CONFIG_KWAI_PATH) -> dict[str, Any]:
    """Load Kwai config from YAML and environment variables without requiring secrets in Git."""
    config: dict[str, Any] = {}
    for candidate in _candidate_config_paths(caminho_config):
        if candidate.exists():
            config.update(load_yaml_config(candidate))
            break

    env_map = {
        "client_id": "KWAI_CLIENT_ID",
        "app_id": "KWAI_APP_ID",
        "secret_key": "KWAI_SECRET_KEY",
        "corp_id": "KWAI_CORP_ID",
        "access_token": "KWAI_ACCESS_TOKEN",
        "refresh_token": "KWAI_REFRESH_TOKEN",
        "api_base": "KWAI_API_BASE",
        "oauth_base": "KWAI_OAUTH_BASE",
        "oauth_refresh_url": "KWAI_OAUTH_REFRESH_URL",
        "report_account_id": "KWAI_REPORT_ACCOUNT_ID",
        "time_zone_iana": "KWAI_TIME_ZONE_IANA",
    }
    for key, env_name in env_map.items():
        value = os.getenv(env_name)
        if value:
            config[key] = value

    if "clientId" in config and "client_id" not in config:
        config["client_id"] = config["clientId"]
    if "appId" in config and "app_id" not in config:
        config["app_id"] = config["appId"]
    if "secretKey" in config and "secret_key" not in config:
        config["secret_key"] = config["secretKey"]
    if "corpId" in config and "corp_id" not in config:
        config["corp_id"] = config["corpId"]

    return config


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return default


def _pick(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _normalize_event_name(field_name: str) -> str:
    return str(field_name).strip().replace(".", "_").replace("-", "_")


def _is_kwai_event_field(field_name: str) -> bool:
    field = _normalize_event_name(field_name)
    lower_field = field.lower()
    if not field:
        return False
    if lower_field in KWAI_EVENT_EXCLUDED_LOWER:
        return False
    if lower_field in {"action", "result"}:
        return False
    if lower_field.startswith(KWAI_EVENT_EXCLUDED_PREFIXES):
        return False
    if "rate" in lower_field:
        return False
    if lower_field.endswith(KWAI_EVENT_EXCLUDED_SUFFIXES):
        return False
    return True


def _money_from_micro(value: Any) -> float:
    return _as_float(value) / 1_000_000


def _safe_log_text(value: Any) -> str:
    return str(value).encode("ascii", errors="backslashreplace").decode("ascii")


def _timezone_from_config(value: Any) -> timezone | ZoneInfo:
    text = str(value or "UTC+00:00").strip()
    if text.upper() in {"UTC", "UTC+00:00", "GMT", "GMT+00:00"}:
        return KWAI_DEFAULT_TZ
    if text.upper().startswith(("UTC+", "UTC-", "GMT+", "GMT-")):
        offset_text = text[3:] if text.upper().startswith("UTC") else text[3:]
        sign = -1 if offset_text.startswith("-") else 1
        hours_text, _, minutes_text = offset_text[1:].partition(":")
        try:
            hours = int(hours_text or 0)
            minutes = int(minutes_text or 0)
            return timezone(sign * timedelta(hours=hours, minutes=minutes))
        except ValueError:
            return KWAI_DEFAULT_TZ
    try:
        return ZoneInfo(text)
    except Exception:
        return KWAI_DEFAULT_TZ


def _date_to_millis(
    value: str,
    *,
    end_of_day: bool = False,
    tzinfo: timezone | ZoneInfo = KWAI_DEFAULT_TZ,
) -> int:
    base_date = datetime.strptime(value, "%Y-%m-%d").date()
    base_time = time(23, 59, 59, 999000) if end_of_day else time.min
    dt = datetime.combine(base_date, base_time, tzinfo=tzinfo)
    return int(dt.timestamp() * 1000)


def _date_from_report_value(value: Any, fallback: str) -> str:
    if value in (None, ""):
        return fallback

    if isinstance(value, (int, float)) or str(value).isdigit():
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric = numeric / 1000
        return datetime.fromtimestamp(numeric, KWAI_DEFAULT_TZ).strftime("%Y-%m-%d")

    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return fallback


def _extract_payload(body: dict[str, Any]) -> dict[str, Any]:
    data = body.get("data")
    return data if isinstance(data, dict) else body


def _response_ok(body: dict[str, Any]) -> bool:
    code = body.get("code")
    status = body.get("status")
    result = body.get("result")
    if result not in (None, 0, "0", 1, "1", 200, "200"):
        return False
    if status not in (None, 0, "0", 200, "200"):
        return False
    if code in (None, 0, "0", 200, "200"):
        return True
    return False


def _extract_records(body: dict[str, Any]) -> tuple[list[dict[str, Any]], int | None]:
    payload = _extract_payload(body)
    records = payload.get("data") or payload.get("list") or payload.get("records") or []
    if isinstance(records, dict):
        records = records.get("list") or records.get("data") or []
    if not isinstance(records, list):
        records = []

    total = payload.get("total") or payload.get("totalCount") or payload.get("total_count")
    try:
        total_int = int(total) if total is not None else None
    except (TypeError, ValueError):
        total_int = None
    return records, total_int


def _token_payload(body: dict[str, Any]) -> dict[str, Any]:
    data = body.get("data")
    return data if isinstance(data, dict) else body


def _refresh_access_token(config: dict[str, Any]) -> str | None:
    refresh_token = config.get("refresh_token")
    client_id = config.get("client_id")
    secret_key = config.get("secret_key")
    if not all([refresh_token, client_id, secret_key]):
        return None

    oauth_base = str(config.get("oauth_base") or KWAI_OAUTH_BASE).rstrip("/")
    api_base = str(config.get("api_base") or KWAI_API_BASE).rstrip("/")
    refresh_url = str(config.get("oauth_refresh_url") or f"{oauth_base}/oauth/token").rstrip("/")

    request_specs: list[tuple[str, str, dict[str, Any]]] = [
        (
            "kwai_oauth_query",
            "get",
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": secret_key,
            },
        ),
        (
            "kwai_oauth_form",
            "post_form",
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": secret_key,
            },
        ),
        (
            "kwai_oauth_json",
            "post_json",
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": secret_key,
            },
        ),
    ]

    for payload_label, method, payload in request_specs:
        response = None
        try:
            if method == "get":
                response = requests.get(refresh_url, params=payload, timeout=30)
            elif method == "post_form":
                response = requests.post(refresh_url, data=payload, timeout=30)
            else:
                response = requests.post(refresh_url, json=payload, timeout=30)
            body = response.json()
        except Exception as exc:
            status_code = response.status_code if response is not None else "sem_status"
            text = response.text[:200] if response is not None else ""
            last_error = _safe_log_text(f"{payload_label}: status={status_code}, erro={exc}, body={text}")
            continue

        token_data = _token_payload(body)
        access_token = token_data.get("access_token")
        if _response_ok(body) and access_token:
            config["access_token"] = access_token
            if token_data.get("refresh_token"):
                config["refresh_token"] = token_data["refresh_token"]
            return access_token

        last_error = _safe_log_text(
            f"{payload_label}: status_http={response.status_code}, "
            f"code={body.get('code')}, status={body.get('status')}, "
            f"message={body.get('message') or body.get('error_description') or body.get('error')}"
        )

    print(f"[Kwai] Nao foi possivel renovar access token: {last_error}")
    return None


def _resolve_access_token(config: dict[str, Any]) -> str | None:
    refreshed = _refresh_access_token(config)
    if refreshed:
        return refreshed
    return config.get("access_token")


def _kwai_headers(access_token: str) -> dict[str, str]:
    return {"Access-Token": access_token, "Content-Type": "application/json"}


def _fetch_kwai_paginated(
    endpoint: str,
    access_token: str,
    payload: dict[str, Any],
    *,
    page_size: int,
    context_label: str,
) -> dict[str, Any]:
    page = 1
    records_all: list[dict[str, Any]] = []
    max_pages = 2000

    while page <= max_pages:
        page_payload = dict(payload)
        page_payload["pageNo"] = page
        page_payload["pageSize"] = page_size

        try:
            response = requests.post(
                endpoint,
                headers=_kwai_headers(access_token),
                json=page_payload,
                timeout=45,
            )
        except Exception as exc:
            return {
                "code": -1,
                "message": f"Erro ao consultar Kwai ({context_label}) pagina {page}: {exc}",
                "data": {"data": records_all},
            }

        try:
            body = response.json()
        except Exception as exc:
            return {
                "code": response.status_code,
                "message": (
                    f"Resposta nao-JSON ao consultar Kwai ({context_label}) pagina {page}: "
                    f"{exc}; body={_safe_log_text(response.text[:300])}"
                ),
                "data": {"data": records_all},
            }

        if response.status_code >= 400:
            return {
                "code": response.status_code,
                "message": body.get("message") or response.text[:300],
                "data": {"data": records_all},
            }

        if not _response_ok(body):
            return body

        records, total = _extract_records(body)
        records_all.extend(records)
        if not records:
            break
        if total is not None and page * page_size >= total:
            break
        if total is None and len(records) < page_size:
            break
        page += 1

    if page > 1:
        print(f"[Kwai] {context_label}: {len(records_all)} registros coletados em {page} paginas.")
    return {"code": 0, "message": "OK", "data": {"data": records_all}}


def fetch_report_data(
    account_id: str,
    report_level: str,
    start_date: str,
    end_date: str,
    access_token: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Fetch one Kwai reporting level for the requested account/date window."""
    level_config = KWAI_REPORT_LEVELS[report_level]
    api_base = str(config.get("api_base") or KWAI_API_BASE).rstrip("/")
    endpoint = f"{api_base}{level_config['endpoint']}"
    page_size = int(config.get("page_size") or 500)
    time_zone_iana = config.get("reach_time_zone_iana") or "UTC+0"
    report_tz = _timezone_from_config(time_zone_iana)

    payload: dict[str, Any] = {
        "accountId": int(account_id) if str(account_id).isdigit() else account_id,
        "dataBeginTime": _date_to_millis(start_date, tzinfo=report_tz),
        "dataEndTime": _date_to_millis(end_date, end_of_day=True, tzinfo=report_tz),
        "granularity": config.get("granularity") or "DAY",
        "timeZoneIana": time_zone_iana,
        "withReach": True,
    }
    if config.get("report_account_id"):
        report_account_id = config["report_account_id"]
        payload["accountId"] = int(report_account_id) if str(report_account_id).isdigit() else report_account_id
    if config.get("corp_id"):
        payload["corpId"] = int(config["corp_id"]) if str(config["corp_id"]).isdigit() else config["corp_id"]

    return _fetch_kwai_paginated(
        endpoint,
        access_token,
        payload,
        page_size=page_size,
        context_label=f"{account_id} {report_level}",
    )


def transformar_registro(registro: dict[str, Any], account_id: str, nivel: str, fallback_date: str) -> dict[str, Any]:
    """Normalize a Kwai API report row into the project schema."""
    produto = get_product_for_account(account_id, "kwai")
    campaign_id = _pick(registro, "campaignId", "campaign_id")
    unit_id = _pick(registro, "unitId", "unit_id", "adgroupId", "adgroup_id")
    creative_id = _pick(registro, "creativeId", "creative_id", "adId", "ad_id")

    if nivel == "campanha" and not campaign_id:
        campaign_id = _pick(registro, "id")
    if nivel == "grupo_anuncio" and not unit_id:
        unit_id = _pick(registro, "id")
    if nivel == "anuncio" and not creative_id:
        creative_id = _pick(registro, "id")

    impressoes = _as_int(_pick(registro, "impression", "impressions", "exposure", "show"))
    cliques = _as_int(_pick(registro, "click", "clicks"))
    action = _as_float(_pick(registro, "action", "conversion", "conversions"))
    result = _as_float(_pick(registro, "result", "action", "conversion", "conversions"))
    frequency = _as_float(_pick(registro, "frequency", "frequencia"), default=None)
    alcance = _as_float(_pick(
        registro,
        "reach",
        "reachCount",
        "reach_count",
        "alcance",
        "uniqueUsers",
        "unique_users",
        "userReach",
        "user_reach",
    ), default=None)
    if alcance is None and frequency and impressoes:
        alcance = impressoes / frequency
    cost_value = _pick(registro, "cost")
    spend = _money_from_micro(cost_value) if cost_value is not None else _as_float(
        _pick(registro, "spend", "totalCost", "total_cost")
    )

    return {
        "data": _date_from_report_value(
            _pick(registro, "time", "date", "statDate", "stat_date", "reportDate", "report_date"),
            fallback_date,
        ),
        "nivel": nivel,
        "produto": produto,
        "id_cliente": str(account_id),
        "account_id": str(account_id),
        "account_name": _pick(registro, "accountName", "account_name") or "",
        "campaign_id": str(campaign_id or ""),
        "id_campanha": str(campaign_id or ""),
        "nome_campanha": _pick(registro, "campaignName", "campaign_name") or "",
        "unit_id": str(unit_id or ""),
        "id_grupo_anuncio": str(unit_id or ""),
        "grupo_anuncio": _pick(registro, "unitName", "unit_name", "adgroupName", "adgroup_name") or "",
        "creative_id": str(creative_id or ""),
        "id_anuncio": str(creative_id or ""),
        "anuncio": _pick(registro, "creativeName", "creative_name", "adName", "ad_name") or "",
        "spend": spend,
        "impressoes": impressoes,
        "cliques": cliques,
        "ctr": _as_float(_pick(registro, "ctr")) or (cliques / impressoes if impressoes else 0.0),
        "cpc": _as_float(_pick(registro, "cpc")),
        "cpm": _as_float(_pick(registro, "cpm")),
        "cpa": _as_float(_pick(registro, "cpa")),
        "alcance": alcance,
        "result": result,
        "action": action,
        "evento": "result",
        "valor": result,
        "currency": _pick(registro, "currency") or "BRL",
    }


def transformar_registro_eventos(
    registro: dict[str, Any],
    account_id: str,
    nivel: str,
    fallback_date: str,
) -> list[dict[str, Any]]:
    base = transformar_registro(registro, account_id, nivel, fallback_date)
    rows = [base]
    seen_events = {"result"}

    for field_name, raw_value in registro.items():
        if not _is_kwai_event_field(field_name):
            continue

        event_name = _normalize_event_name(field_name)
        event_key = event_name.lower()
        if event_key in seen_events:
            continue

        value = _as_float(raw_value)
        if value == 0:
            continue

        event_row = dict(base)
        event_row["evento"] = event_name
        event_row["valor"] = value
        _clear_performance_metrics_for_event(event_row)
        rows.append(event_row)
        seen_events.add(event_key)

    return rows


def _clear_performance_metrics_for_event(row: dict[str, Any]) -> None:
    for column in ("spend", "ctr", "cpc", "cpm", "cpa", "result", "action"):
        row[column] = 0.0
    for column in ("impressoes", "cliques"):
        row[column] = 0
    row["alcance"] = None


def _aggregation_entity_key(row: dict[str, Any], nivel: str) -> tuple[Any, ...] | None:
    campaign_id = row.get("id_campanha") or row.get("campaign_id") or ""
    campaign_name = row.get("nome_campanha") or ""
    group_id = row.get("id_grupo_anuncio") or row.get("unit_id") or ""
    group_name = row.get("grupo_anuncio") or ""

    if nivel == "campanha":
        if not campaign_id and not campaign_name:
            return None
        return (
            row.get("data"),
            row.get("id_cliente"),
            campaign_id,
            campaign_name,
        )

    if not group_id and not group_name:
        return None
    return (
        row.get("data"),
        row.get("id_cliente"),
        campaign_id,
        campaign_name,
        group_id,
        group_name,
    )


def _empty_aggregated_base(row: dict[str, Any], nivel: str) -> dict[str, Any]:
    base = {column: row.get(column) for column in KWAI_BASE_COLUMNS}
    base["nivel"] = nivel
    base["creative_id"] = ""
    base["id_anuncio"] = ""
    base["anuncio"] = ""
    base["alcance"] = None
    if nivel == "campanha":
        base["unit_id"] = ""
        base["id_grupo_anuncio"] = ""
        base["grupo_anuncio"] = ""
    for column in ("spend", "result", "action", "valor"):
        base[column] = 0.0
    for column in ("impressoes", "cliques"):
        base[column] = 0
    return base


def _recalculate_rates(row: dict[str, Any]) -> None:
    spend = _as_float(row.get("spend"))
    impressoes = _as_int(row.get("impressoes"))
    cliques = _as_int(row.get("cliques"))
    action = _as_float(row.get("action"))
    result = _as_float(row.get("result"))
    row["ctr"] = cliques / impressoes if impressoes else 0.0
    row["cpc"] = spend / cliques if cliques else 0.0
    row["cpm"] = (spend / impressoes) * 1000 if impressoes else 0.0
    row["cpa"] = spend / (result or action) if (result or action) else 0.0


def _derive_aggregated_level(source_rows: list[dict[str, Any]], nivel: str) -> list[dict[str, Any]]:
    """Build campaign/ad-group rows from ad-level rows when Kwai only exposes creative report data."""
    if nivel not in {"campanha", "grupo_anuncio"}:
        return []

    perf_grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    first_row_by_entity: dict[tuple[Any, ...], dict[str, Any]] = {}
    event_values: dict[tuple[tuple[Any, ...], str], float] = {}

    for row in source_rows:
        entity_key = _aggregation_entity_key(row, nivel)
        if entity_key is None:
            continue

        if entity_key not in first_row_by_entity:
            first_row_by_entity[entity_key] = row

        event_name = str(row.get("evento") or "result")
        event_values[(entity_key, event_name)] = (
            event_values.get((entity_key, event_name), 0.0) + _as_float(row.get("valor"))
        )

        if event_name != "result":
            continue

        if entity_key not in perf_grouped:
            perf_grouped[entity_key] = _empty_aggregated_base(row, nivel)

        target = perf_grouped[entity_key]
        target["spend"] = _as_float(target.get("spend")) + _as_float(row.get("spend"))
        target["impressoes"] = _as_int(target.get("impressoes")) + _as_int(row.get("impressoes"))
        target["cliques"] = _as_int(target.get("cliques")) + _as_int(row.get("cliques"))
        target["result"] = _as_float(target.get("result")) + _as_float(row.get("result"))
        target["action"] = _as_float(target.get("action")) + _as_float(row.get("action"))

    output_rows: list[dict[str, Any]] = []
    for entity_key, row in perf_grouped.items():
        _recalculate_rates(row)
        row["evento"] = "result"
        row["valor"] = row["result"]
        output_rows.append(row)

        for (event_entity_key, event_name), value in event_values.items():
            if event_entity_key != entity_key or event_name == "result" or value == 0:
                continue
            event_row = dict(row)
            event_row["evento"] = event_name
            event_row["valor"] = value
            _clear_performance_metrics_for_event(event_row)
            output_rows.append(event_row)

    for entity_key, first_row in first_row_by_entity.items():
        if entity_key in perf_grouped:
            continue
        base = _empty_aggregated_base(first_row, nivel)
        _recalculate_rates(base)
        for (event_entity_key, event_name), value in event_values.items():
            if event_entity_key != entity_key or value == 0:
                continue
            event_row = dict(base)
            event_row["evento"] = event_name
            event_row["valor"] = value
            _clear_performance_metrics_for_event(event_row)
            output_rows.append(event_row)

    return output_rows


def _normalize_kwai_frame(df: pl.DataFrame) -> pl.DataFrame:
    for column in KWAI_BASE_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    numeric_casts = {
        "spend": pl.Float64,
        "impressoes": pl.Int64,
        "cliques": pl.Int64,
        "ctr": pl.Float64,
        "cpc": pl.Float64,
        "cpm": pl.Float64,
        "cpa": pl.Float64,
        "alcance": pl.Float64,
        "result": pl.Float64,
        "action": pl.Float64,
        "valor": pl.Float64,
    }
    for column, dtype in numeric_casts.items():
        if column in df.columns:
            df = df.with_columns(pl.col(column).cast(dtype, strict=False).alias(column))
    return df.select([column for column in KWAI_BASE_COLUMNS if column in df.columns])


def obter_dados_kwai(account_id: str, data_inicio: str, data_fim: str, config: dict[str, Any]) -> pl.DataFrame:
    """Collect Kwai performance data for campaign, unit and creative levels."""
    access_token = _resolve_access_token(config)
    if not access_token:
        print("[Kwai] Configuracao sem access_token/refresh_token. Coleta ignorada.")
        return pl.DataFrame()

    start_date = datetime.strptime(data_inicio, "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(data_fim, "%Y%m%d").strftime("%Y-%m-%d")
    rows_by_level: dict[str, list[dict[str, Any]]] = {}

    for level in KWAI_REPORT_LEVELS:
        print(f"[Kwai] Coletando {level} para account {account_id}: {start_date} a {end_date}")
        body = fetch_report_data(account_id, level, start_date, end_date, access_token, config)
        if not _response_ok(body):
            print(
                f"[Kwai] Erro no relatorio {level}: "
                f"code={body.get('code')}, status={body.get('status')}, "
                f"message={_safe_log_text(body.get('message'))}"
            )
            continue

        records, _ = _extract_records(body)
        if not records:
            print(f"[Kwai] Nenhum registro para {level}.")
            continue
        level_rows: list[dict[str, Any]] = []
        for item in records:
            level_rows.extend(transformar_registro_eventos(item, account_id, level, start_date))
        rows_by_level[level] = level_rows

    if rows_by_level.get("anuncio"):
        rows_by_level["grupo_anuncio"] = _derive_aggregated_level(rows_by_level["anuncio"], "grupo_anuncio")
        if not rows_by_level.get("campanha"):
            print("[Kwai] Campanha derivada a partir do nivel anuncio.")
            rows_by_level["campanha"] = _derive_aggregated_level(rows_by_level["anuncio"], "campanha")

    rows: list[dict[str, Any]] = []
    for level in ("campanha", "grupo_anuncio", "anuncio"):
        rows.extend(rows_by_level.get(level, []))

    if not rows:
        print("[Kwai] Nenhum dado coletado.")
        return pl.DataFrame()

    return _normalize_kwai_frame(pl.DataFrame(rows))


def inserir_substituir(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    """Merge the new Kwai extract into the parquet target using stable keys."""
    if df_novo.height == 0 or len(df_novo.columns) == 0:
        print(f"[Kwai] DataFrame vazio. Nada a inserir em {caminho_arquivo}.")
        return

    def adicionar_chave_unica(df: pl.DataFrame) -> pl.DataFrame:
        entity_id_expr = (
            pl.when(pl.col("nivel") == "campanha")
            .then(pl.col("id_campanha").cast(pl.Utf8))
            .when(pl.col("nivel") == "grupo_anuncio")
            .then(pl.col("id_grupo_anuncio").cast(pl.Utf8))
            .when(pl.col("nivel") == "anuncio")
            .then(pl.col("id_anuncio").cast(pl.Utf8))
            .otherwise(pl.lit(""))
        )
        evento_expr = (
            pl.when(pl.col("evento").cast(pl.Utf8).fill_null("") == "")
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit("_"), pl.col("evento").cast(pl.Utf8)]))
            if "evento" in df.columns
            else pl.lit("")
        )
        return df.with_columns([
            pl.concat_str([
                pl.col("id_cliente").cast(pl.Utf8),
                pl.lit("_"),
                pl.col("nivel").cast(pl.Utf8),
                pl.lit("_"),
                entity_id_expr,
                pl.lit("_"),
                pl.col("data").cast(pl.Utf8),
                evento_expr,
            ]).alias("chave_unica")
        ])

    df_novo = _normalize_kwai_frame(df_novo)
    df_novo = adicionar_chave_unica(df_novo)
    os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
    os.makedirs(BACKUP_KWAI_PATH, exist_ok=True)

    agora = datetime.now()
    for arquivo in os.listdir(BACKUP_KWAI_PATH):
        caminho_backup = os.path.join(BACKUP_KWAI_PATH, arquivo)
        if os.path.isfile(caminho_backup):
            mtime = datetime.fromtimestamp(os.path.getmtime(caminho_backup))
            if (agora - mtime) > timedelta(days=5):
                try:
                    os.remove(caminho_backup)
                except Exception as exc:
                    print(f"[Kwai] Erro ao remover backup {caminho_backup}: {exc}")

    if os.path.exists(caminho_arquivo):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
        caminho_backup = os.path.join(BACKUP_KWAI_PATH, f"{nome_base}_{timestamp}{extensao}")
        try:
            shutil.copy2(caminho_arquivo, caminho_backup)
            print(f"[Kwai] Backup realizado: {caminho_backup}")
        except Exception as exc:
            print(f"[Kwai] Erro ao realizar backup do arquivo {caminho_arquivo}: {exc}")

        df_antigo = pl.read_parquet(caminho_arquivo)
        df_antigo = adicionar_chave_unica(_normalize_kwai_frame(df_antigo))
        chaves_novas = df_novo.select("chave_unica").to_series().to_list()
        df_antigo_filtrado = df_antigo.filter(~pl.col("chave_unica").is_in(chaves_novas))
        df_combinado = align_and_concat_frames([df_antigo_filtrado, df_novo])
    else:
        df_combinado = df_novo

    df_combinado.drop("chave_unica").write_parquet(caminho_arquivo, compression="snappy")
    print(f"[Kwai] Dados salvos em {caminho_arquivo}")


def executar_kwai(
    data_inicio: str,
    data_fim: str,
    contas: list[str] | None = None,
) -> pl.DataFrame:
    """Run the Kwai extraction flow and persist the consolidated parquet."""
    config = carregar_config_kwai(CONFIG_KWAI_PATH)
    target_accounts = resolve_selected_ids(contas, config.get("accounts") or KWAI_ACCOUNTS)
    if not target_accounts:
        print("[Kwai] Nenhuma conta configurada para coleta.")
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for account_id in target_accounts:
        df = obter_dados_kwai(str(account_id), data_inicio, data_fim, config)
        if df.height > 0:
            frames.append(df)

    df_final = align_and_concat_frames(frames)
    if df_final.height > 0:
        inserir_substituir(df_final, KWAI_OUTPUT_FILE)
    return df_final
