from __future__ import annotations

import json
import os
import shutil
from datetime import date, datetime, timedelta

import polars as pl
from facebook_business.adobjects.adaccount import AdAccount
from google.ads.googleads.client import GoogleAdsClient

from src.account_catalog import get_product_for_account, list_accounts
from src.google_ads import _run_with_google_ads_retry
from src.kwai import (
    _extract_records as _extract_kwai_records,
    _pick as _pick_kwai,
    _resolve_access_token as _resolve_kwai_access_token,
    _response_ok as _kwai_response_ok,
    _safe_log_text as _safe_kwai_log_text,
    carregar_config_kwai,
    fetch_report_data as fetch_kwai_report_data,
)
from src.metads import (
    _normalizar_id_conta_meta,
    _obter_insights_com_fallback,
    _produto_por_conta_meta,
)
from src.pipeline_utils import align_and_concat_frames, load_yaml_config, resolve_selected_ids
from src.tiktok import (
    TIKTOK_API_BASE,
    _fetch_integrated_paginated,
    _is_tiktok_metric_error,
)
from src.utils_do_pai import (
    BACKUP_GADS_PATH,
    CONFIG_GOOGLE_PATH,
    CONFIG_KWAI_PATH,
    CONFIG_TIKTOK_PATH,
    OUTPUT_PATH,
)


ALCANCE_SEMANAL_OUTPUT_FILE = os.path.join(str(OUTPUT_PATH), "alcance_semanal_all_new.parquet")
ALCANCE_SEMANAL_GRUPO_OUTPUT_FILE = os.path.join(
    str(OUTPUT_PATH),
    "alcance_semanal_grupo_anuncio_all_new.parquet",
)
ALCANCE_SEMANAL_COLUMNS = [
    "semana_ref",
    "semana",
    "nome_semana",
    "data_inicio_semana",
    "data_fim_semana",
    "veiculo",
    "produto",
    "id_cliente",
    "id_campanha",
    "nome_campanha",
    "alcance",
]
ALCANCE_SEMANAL_GRUPO_COLUMNS = [
    "semana_ref",
    "semana",
    "nome_semana",
    "data_inicio_semana",
    "data_fim_semana",
    "veiculo",
    "produto",
    "id_cliente",
    "id_campanha",
    "nome_campanha",
    "id_grupo_anuncio",
    "nome_grupo_anuncio",
    "alcance",
]


def _parse_date(value: str) -> date:
    text = str(value).strip()
    fmt = "%Y%m%d" if "-" not in text else "%Y-%m-%d"
    return datetime.strptime(text, fmt).date()


def _format_br(value: date) -> str:
    return value.strftime("%d/%m/%Y")


def _iterar_semanas(data_inicio: str, data_fim: str) -> list[dict[str, str]]:
    """Build Monday-based periods without truncating the oldest calendar week."""
    start = _parse_date(data_inicio)
    end = _parse_date(data_fim)
    if start > end:
        raise ValueError("data_inicio nao pode ser maior que data_fim.")

    current_week = start - timedelta(days=start.weekday())
    semanas: list[dict[str, str]] = []
    while current_week <= end:
        week_end = current_week + timedelta(days=6)
        # Weekly reach is non-additive. Expanding the rolling-window boundary
        # to Monday prevents a completed week from being replaced by one day.
        query_start = current_week
        query_end = min(end, week_end)
        semanas.append(
            {
                "semana_ref": current_week.isoformat(),
                "semana": _format_br(current_week),
                "nome_semana": f"{_format_br(current_week)} - {_format_br(week_end)}",
                "data_inicio_semana": query_start.isoformat(),
                "data_fim_semana": query_end.isoformat(),
            }
        )
        current_week += timedelta(days=7)
    return semanas


def _base_record(semana: dict[str, str], veiculo: str, produto: str | None, id_cliente: str) -> dict:
    return {
        "semana_ref": semana["semana_ref"],
        "semana": semana["semana"],
        "nome_semana": semana["nome_semana"],
        "data_inicio_semana": semana["data_inicio_semana"],
        "data_fim_semana": semana["data_fim_semana"],
        "veiculo": veiculo,
        "produto": produto,
        "id_cliente": str(id_cliente),
    }


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace("-", "0").replace(",", "."))
    except (TypeError, ValueError):
        return None


def obter_alcance_semanal_google(id_cliente: str, semana: dict[str, str]) -> pl.DataFrame:
    client = GoogleAdsClient.load_from_storage(CONFIG_GOOGLE_PATH)
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          metrics.unique_users
        FROM campaign
        WHERE segments.date BETWEEN '{semana["data_inicio_semana"]}' AND '{semana["data_fim_semana"]}'
    """

    def _fetch_rows():
        response = service.search(customer_id=id_cliente, query=query)
        rows = []
        produto = get_product_for_account(id_cliente, "google")
        for row in response:
            record = _base_record(semana, "google", produto, id_cliente)
            record.update(
                {
                    "id_campanha": str(row.campaign.id),
                    "nome_campanha": row.campaign.name,
                    "alcance": _to_float(row.metrics.unique_users),
                }
            )
            rows.append(record)
        return rows

    rows = _run_with_google_ads_retry(
        _fetch_rows,
        context=(
            f"alcance semanal google customer={id_cliente} "
            f"periodo={semana['data_inicio_semana']}..{semana['data_fim_semana']}"
        ),
    )
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})


def obter_alcance_semanal_meta(conta_id: str, semana: dict[str, str]) -> pl.DataFrame:
    conta = AdAccount(conta_id)
    produto = _produto_por_conta_meta(conta_id)
    id_cliente = _normalizar_id_conta_meta(conta_id)
    insights = _obter_insights_com_fallback(
        conta,
        fields=["campaign_id", "campaign_name", "reach"],
        params={
            "time_range": {
                "since": semana["data_inicio_semana"],
                "until": semana["data_fim_semana"],
            },
            "limit": 5000,
        },
        level="campaign",
    )

    rows = []
    for insight in insights:
        record = _base_record(semana, "meta", produto, id_cliente)
        record.update(
            {
                "id_campanha": insight.get("campaign_id"),
                "nome_campanha": insight.get("campaign_name"),
                "alcance": _to_float(insight.get("reach")),
            }
        )
        rows.append(record)
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})


def obter_alcance_semanal_grupo_meta(conta_id: str, semana: dict[str, str]) -> pl.DataFrame:
    conta = AdAccount(conta_id)
    produto = _produto_por_conta_meta(conta_id)
    id_cliente = _normalizar_id_conta_meta(conta_id)
    insights = _obter_insights_com_fallback(
        conta,
        fields=[
            "campaign_id",
            "campaign_name",
            "adset_id",
            "adset_name",
            "reach",
        ],
        params={
            "time_range": {
                "since": semana["data_inicio_semana"],
                "until": semana["data_fim_semana"],
            },
            "limit": 5000,
        },
        level="adset",
    )

    rows = []
    for insight in insights:
        record = _base_record(semana, "meta", produto, id_cliente)
        record.update(
            {
                "id_campanha": insight.get("campaign_id"),
                "nome_campanha": insight.get("campaign_name"),
                "id_grupo_anuncio": insight.get("adset_id"),
                "nome_grupo_anuncio": insight.get("adset_name"),
                "alcance": _to_float(insight.get("reach")),
            }
        )
        rows.append(record)
    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_GRUPO_COLUMNS})
    )


def obter_alcance_semanal_tiktok(advertiser_id: str, semana: dict[str, str], config: dict) -> pl.DataFrame:
    access_token = config.get("access_token")
    if not access_token:
        print("Access token do TikTok nao encontrado na configuracao.")
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})

    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    endpoint = f"{TIKTOK_API_BASE}/report/integrated/get/"
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": json.dumps(["campaign_id"]),
        "metrics": json.dumps(["reach", "campaign_name"]),
        "start_date": semana["data_inicio_semana"],
        "end_date": semana["data_fim_semana"],
        "page": 1,
        "page_size": 1000,
    }
    body = _fetch_integrated_paginated(
        endpoint=endpoint,
        headers=headers,
        base_params=params,
        context_label="Alcance semanal TikTok AUCTION_CAMPAIGN",
    )
    if body.get("code") != 0 and _is_tiktok_metric_error(body, "reach"):
        print(
            "TikTok nao retornou metric reach para alcance semanal "
            f"({advertiser_id}, {semana['data_inicio_semana']} a {semana['data_fim_semana']})."
        )
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})
    if body.get("code") != 0:
        print(
            "Erro ao coletar alcance semanal TikTok "
            f"({advertiser_id}): code={body.get('code')}, message={body.get('message')}"
        )
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})

    rows = []
    produto = get_product_for_account(advertiser_id, "tiktok")
    for item in ((body.get("data") or {}).get("list") or []):
        dims = item.get("dimensions") or {}
        metrics = item.get("metrics") or {}
        record = _base_record(semana, "tiktok", produto, advertiser_id)
        record.update(
            {
                "id_campanha": dims.get("campaign_id"),
                "nome_campanha": metrics.get("campaign_name") or dims.get("campaign_name"),
                "alcance": _to_float(metrics.get("reach")),
            }
        )
        rows.append(record)
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})


def obter_alcance_semanal_grupo_tiktok(
    advertiser_id: str,
    semana: dict[str, str],
    config: dict,
) -> pl.DataFrame:
    access_token = config.get("access_token")
    if not access_token:
        print("Access token do TikTok nao encontrado na configuracao.")
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_GRUPO_COLUMNS})

    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    endpoint = f"{TIKTOK_API_BASE}/report/integrated/get/"
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_ADGROUP",
        "dimensions": json.dumps(["adgroup_id"]),
        "metrics": json.dumps(
            [
                "reach",
                "campaign_id",
                "campaign_name",
                "adgroup_name",
            ]
        ),
        "start_date": semana["data_inicio_semana"],
        "end_date": semana["data_fim_semana"],
        "page": 1,
        "page_size": 1000,
    }
    body = _fetch_integrated_paginated(
        endpoint=endpoint,
        headers=headers,
        base_params=params,
        context_label="Alcance semanal TikTok AUCTION_ADGROUP",
    )
    if body.get("code") != 0 and _is_tiktok_metric_error(body, "reach"):
        print(
            "TikTok nao retornou metric reach para alcance semanal por grupo "
            f"({advertiser_id}, {semana['data_inicio_semana']} a {semana['data_fim_semana']})."
        )
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_GRUPO_COLUMNS})
    if body.get("code") != 0:
        print(
            "Erro ao coletar alcance semanal por grupo no TikTok "
            f"({advertiser_id}): code={body.get('code')}, message={body.get('message')}"
        )
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_GRUPO_COLUMNS})

    rows = []
    produto = get_product_for_account(advertiser_id, "tiktok")
    for item in ((body.get("data") or {}).get("list") or []):
        dims = item.get("dimensions") or {}
        metrics = item.get("metrics") or {}
        record = _base_record(semana, "tiktok", produto, advertiser_id)
        record.update(
            {
                "id_campanha": metrics.get("campaign_id") or dims.get("campaign_id"),
                "nome_campanha": metrics.get("campaign_name") or dims.get("campaign_name"),
                "id_grupo_anuncio": dims.get("adgroup_id"),
                "nome_grupo_anuncio": metrics.get("adgroup_name") or dims.get("adgroup_name"),
                "alcance": _to_float(metrics.get("reach")),
            }
        )
        rows.append(record)
    return (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_GRUPO_COLUMNS})
    )


def obter_alcance_semanal_kwai(account_id: str, semana: dict[str, str], config: dict) -> pl.DataFrame:
    access_token = _resolve_kwai_access_token(config)
    if not access_token:
        print("[Alcance semanal][Kwai] Configuracao sem access_token/refresh_token. Coleta ignorada.")
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})

    body = fetch_kwai_report_data(
        account_id,
        "campanha",
        semana["data_inicio_semana"],
        semana["data_fim_semana"],
        access_token,
        config,
    )
    if not _kwai_response_ok(body):
        print(
            "[Alcance semanal][Kwai] Erro no relatorio campanha "
            f"({account_id}): code={body.get('code')}, status={body.get('status')}, "
            f"message={_safe_kwai_log_text(body.get('message'))}"
        )
        return pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})

    rows = []
    missing_reach = False
    produto = get_product_for_account(account_id, "kwai")
    records, _ = _extract_kwai_records(body)
    for item in records:
        alcance = _to_float(
            _pick_kwai(
                item,
                "reach",
                "reachCount",
                "reach_count",
                "alcance",
                "uniqueUsers",
                "unique_users",
                "userReach",
                "user_reach",
            )
        )
        if alcance is None:
            impressoes = _to_float(_pick_kwai(item, "impression", "impressions", "exposure", "show"))
            frequencia = _to_float(_pick_kwai(item, "frequency", "frequencia"))
            if impressoes and frequencia:
                alcance = impressoes / frequencia
        if alcance is None:
            missing_reach = True
            continue

        record = _base_record(semana, "kwai", produto, account_id)
        record.update(
            {
                "id_campanha": _pick_kwai(item, "campaignId", "campaign_id", "id"),
                "nome_campanha": _pick_kwai(item, "campaignName", "campaign_name"),
                "alcance": alcance,
            }
        )
        rows.append(record)

    if missing_reach and not rows:
        sample_keys = ", ".join(sorted(records[0].keys())) if records else "sem_amostra"
        print(
            "[Alcance semanal][Kwai] Relatorio de campanha nao retornou metrica de alcance "
            f"({account_id}, {semana['data_inicio_semana']} a {semana['data_fim_semana']}). "
            f"Campos da primeira linha: {sample_keys}"
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in ALCANCE_SEMANAL_COLUMNS})


def _normalizar_alcance_frame(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={column: pl.Utf8 for column in ALCANCE_SEMANAL_COLUMNS})

    for column in ALCANCE_SEMANAL_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    return df.select(
        [
            pl.col("semana_ref").cast(pl.Utf8),
            pl.col("semana").cast(pl.Utf8),
            pl.col("nome_semana").cast(pl.Utf8),
            pl.col("data_inicio_semana").cast(pl.Utf8),
            pl.col("data_fim_semana").cast(pl.Utf8),
            pl.col("veiculo").cast(pl.Utf8).str.to_lowercase(),
            pl.col("produto").cast(pl.Utf8),
            pl.col("id_cliente").cast(pl.Utf8),
            pl.col("id_campanha").cast(pl.Utf8),
            pl.col("nome_campanha").cast(pl.Utf8),
            pl.col("alcance").cast(pl.Float64, strict=False),
        ]
    )


def _normalizar_alcance_grupo_frame(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={column: pl.Utf8 for column in ALCANCE_SEMANAL_GRUPO_COLUMNS})

    for column in ALCANCE_SEMANAL_GRUPO_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    return df.select(
        [
            pl.col("semana_ref").cast(pl.Utf8),
            pl.col("semana").cast(pl.Utf8),
            pl.col("nome_semana").cast(pl.Utf8),
            pl.col("data_inicio_semana").cast(pl.Utf8),
            pl.col("data_fim_semana").cast(pl.Utf8),
            pl.col("veiculo").cast(pl.Utf8).str.to_lowercase(),
            pl.col("produto").cast(pl.Utf8),
            pl.col("id_cliente").cast(pl.Utf8),
            pl.col("id_campanha").cast(pl.Utf8),
            pl.col("nome_campanha").cast(pl.Utf8),
            pl.col("id_grupo_anuncio").cast(pl.Utf8),
            pl.col("nome_grupo_anuncio").cast(pl.Utf8),
            pl.col("alcance").cast(pl.Float64, strict=False),
        ]
    )


def inserir_substituir_alcance_semanal(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    df_novo = _normalizar_alcance_frame(df_novo)
    if df_novo.height == 0:
        print("Alcance semanal vazio; mantendo arquivo existente.")
        return

    def adicionar_chave(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.concat_str(
                [
                    pl.col("semana_ref").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("veiculo").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("id_cliente").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("id_campanha").cast(pl.Utf8),
                ]
            ).alias("chave_unica")
        )

    df_novo = adicionar_chave(df_novo).unique(subset=["chave_unica"])

    if os.path.exists(caminho_arquivo):
        if not os.path.exists(BACKUP_GADS_PATH):
            os.makedirs(BACKUP_GADS_PATH, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
        shutil.copy(caminho_arquivo, os.path.join(BACKUP_GADS_PATH, f"{nome_base}_{timestamp}{extensao}"))

        df_antigo = _normalizar_alcance_frame(pl.read_parquet(caminho_arquivo))
        df_antigo = adicionar_chave(df_antigo)
        chaves_novas = df_novo.select("chave_unica").to_series().to_list()
        df_antigo = df_antigo.filter(~pl.col("chave_unica").is_in(chaves_novas))
        df_final = align_and_concat_frames([df_antigo, df_novo])
    else:
        df_final = df_novo

    df_final = df_final.drop("chave_unica", strict=False)
    if "frequencia" in df_final.columns:
        df_final = df_final.drop("frequencia")
    df_final.select(ALCANCE_SEMANAL_COLUMNS).write_parquet(caminho_arquivo, compression="snappy")
    print(f"Alcance semanal salvo em {caminho_arquivo}. Shape: {df_final.shape}")


def inserir_substituir_alcance_semanal_grupo(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    df_novo = _normalizar_alcance_grupo_frame(df_novo)
    df_novo = df_novo.filter(
        pl.col("id_grupo_anuncio").cast(pl.Utf8).fill_null("") != ""
    )
    if df_novo.height == 0:
        print("Alcance semanal por grupo vazio; mantendo arquivo existente.")
        return

    def adicionar_chave(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.concat_str(
                [
                    pl.col("semana_ref").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("veiculo").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("id_cliente").cast(pl.Utf8),
                    pl.lit("|"),
                    pl.col("id_campanha").cast(pl.Utf8).fill_null(""),
                    pl.lit("|"),
                    pl.col("id_grupo_anuncio").cast(pl.Utf8).fill_null(""),
                ]
            ).alias("chave_unica")
        )

    df_novo = adicionar_chave(df_novo).unique(subset=["chave_unica"])

    if os.path.exists(caminho_arquivo):
        if not os.path.exists(BACKUP_GADS_PATH):
            os.makedirs(BACKUP_GADS_PATH, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
        shutil.copy(
            caminho_arquivo,
            os.path.join(BACKUP_GADS_PATH, f"{nome_base}_{timestamp}{extensao}"),
        )

        df_antigo = _normalizar_alcance_grupo_frame(pl.read_parquet(caminho_arquivo))
        df_antigo = adicionar_chave(df_antigo)
        chaves_novas = df_novo.select("chave_unica").to_series().to_list()
        df_antigo = df_antigo.filter(~pl.col("chave_unica").is_in(chaves_novas))
        df_final = align_and_concat_frames([df_antigo, df_novo])
    else:
        df_final = df_novo

    df_final = df_final.drop("chave_unica", strict=False)
    if "frequencia" in df_final.columns:
        df_final = df_final.drop("frequencia")
    df_final.select(ALCANCE_SEMANAL_GRUPO_COLUMNS).write_parquet(
        caminho_arquivo,
        compression="snappy",
    )
    print(f"Alcance semanal por grupo salvo em {caminho_arquivo}. Shape: {df_final.shape}")


def executar_alcance_semanal(
    data_inicio: str,
    data_fim: str,
    *,
    channels: list[str] | None = None,
    google_ids: list[str] | None = None,
    meta_ids: list[str] | None = None,
    tiktok_ids: list[str] | None = None,
    kwai_ids: list[str] | None = None,
) -> pl.DataFrame:
    canais = {channel.strip().lower() for channel in (channels or ["google", "meta", "tiktok", "kwai"])}
    semanas = _iterar_semanas(data_inicio, data_fim)
    resultados: list[pl.DataFrame] = []
    resultados_grupo: list[pl.DataFrame] = []

    if "google" in canais:
        for id_cliente in resolve_selected_ids(google_ids, list_accounts("google")):
            for semana in semanas:
                print(
                    f"[Alcance semanal][Google] {id_cliente}: "
                    f"{semana['data_inicio_semana']} a {semana['data_fim_semana']}"
                )
                resultados.append(obter_alcance_semanal_google(id_cliente, semana))

    if "meta" in canais:
        for conta_id in resolve_selected_ids(meta_ids, list_accounts("meta")):
            for semana in semanas:
                print(
                    f"[Alcance semanal][Meta] {conta_id}: "
                    f"{semana['data_inicio_semana']} a {semana['data_fim_semana']}"
                )
                resultados.append(obter_alcance_semanal_meta(conta_id, semana))
                resultados_grupo.append(obter_alcance_semanal_grupo_meta(conta_id, semana))

    if "tiktok" in canais:
        config = load_yaml_config(CONFIG_TIKTOK_PATH)
        for advertiser_id in resolve_selected_ids(tiktok_ids, list_accounts("tiktok")):
            for semana in semanas:
                print(
                    f"[Alcance semanal][TikTok] {advertiser_id}: "
                    f"{semana['data_inicio_semana']} a {semana['data_fim_semana']}"
                )
                resultados.append(obter_alcance_semanal_tiktok(advertiser_id, semana, config))
                resultados_grupo.append(
                    obter_alcance_semanal_grupo_tiktok(advertiser_id, semana, config)
                )

    if "kwai" in canais:
        config = carregar_config_kwai(CONFIG_KWAI_PATH)
        for account_id in resolve_selected_ids(kwai_ids, list_accounts("kwai")):
            for semana in semanas:
                print(
                    f"[Alcance semanal][Kwai] {account_id}: "
                    f"{semana['data_inicio_semana']} a {semana['data_fim_semana']}"
                )
                resultados.append(obter_alcance_semanal_kwai(account_id, semana, config))

    df_final = _normalizar_alcance_frame(align_and_concat_frames(resultados))
    inserir_substituir_alcance_semanal(df_final, ALCANCE_SEMANAL_OUTPUT_FILE)
    df_final_grupo = _normalizar_alcance_grupo_frame(
        align_and_concat_frames(resultados_grupo)
    )
    inserir_substituir_alcance_semanal_grupo(
        df_final_grupo,
        ALCANCE_SEMANAL_GRUPO_OUTPUT_FILE,
    )
    return df_final
