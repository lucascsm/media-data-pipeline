import os
import shutil
import requests
import mimetypes
import time
import random
from datetime import datetime, timedelta
import polars as pl
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core.exceptions import (
    ServiceUnavailable,
    DeadlineExceeded,
    InternalServerError,
    TooManyRequests,
    ResourceExhausted,
)

from src.utils_do_pai import (
    OUTPUT_PATH,
    CONFIG_GOOGLE_PATH,
    BACKUP_GADS_PATH,
    AD_ASSETS_PATH_GOOGLE,
)
from src.account_catalog import get_product_for_account, list_accounts, CONVERSION_AGGREGATION_ACCOUNT_IDS, normalize_account_id
from src.pipeline_utils import align_and_concat_frames, resolve_selected_ids

GOOGLE_PERFORMANCE_OUTPUT_FILE = os.path.join(str(OUTPUT_PATH), "dados_google_ads_all_new.parquet")
CUSTOMER_IDS_GOOGLE = list_accounts("google")


EXT_BY_ADSAPI = {
    "IMAGE_JPEG": "jpg",
    "IMAGE_PNG": "png",
    "IMAGE_GIF": "gif",
    "FLASH": "swf",
    "HTML5_AD_ZIP": "zip",
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "application/x-shockwave-flash": "swf",
    "application/zip": "zip"
}


def _is_retryable_google_ads_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            ServiceUnavailable,
            DeadlineExceeded,
            InternalServerError,
            TooManyRequests,
            ResourceExhausted,
        ),
    ):
        return True

    if isinstance(exc, GoogleAdsException):
        try:
            status_name = exc.error.code().name
        except Exception:
            status_name = ""
        return status_name in {
            "UNAVAILABLE",
            "DEADLINE_EXCEEDED",
            "INTERNAL",
            "RESOURCE_EXHAUSTED",
            "ABORTED",
        }

    return False


def _run_with_google_ads_retry(
    fn,
    context: str,
    max_retries: int = 6,
    initial_delay: float = 5.0,
    max_delay: float = 90.0,
):
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exc:
            is_retryable = _is_retryable_google_ads_error(exc)
            is_last_attempt = attempt == (max_retries - 1)
            if (not is_retryable) or is_last_attempt:
                raise

            sleep_seconds = min(delay, max_delay) + random.uniform(0, 1.5)
            print(
                f"[GoogleAds retry] {context} falhou ({type(exc).__name__}): {exc}. "
                f"Tentativa {attempt + 1}/{max_retries}, aguardando {sleep_seconds:.1f}s..."
            )
            time.sleep(sleep_seconds)
            delay = min(delay * 2, max_delay)


def inserir_substituir(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    """Merge the new extract into the parquet target using business keys."""
    def adicionar_chave_unica(df: pl.DataFrame) -> pl.DataFrame:
        id_cliente_expr = (
            pl.col("id_cliente").cast(pl.Utf8)
            if "id_cliente" in df.columns
            else pl.lit("sem_id_cliente")
        )
        # Detect whether the extract contains conversion events or performance metrics.
        if "conversion_event" in df.columns:
            # Conversion rows use event-aware business keys.
            return df.with_columns([
                pl.when(pl.col("nivel") == "campanha")
                  .then(pl.concat_str([
                      pl.lit("conv_campanha_"),
                      pl.col("id_cliente").cast(pl.Utf8), pl.lit("_"),
                      pl.col("id_campanha").cast(pl.Utf8), pl.lit("_"),
                      pl.col("data"), pl.lit("_"),
                      pl.col("conversion_event")
                  ]))
                  .when(pl.col("nivel") == "grupo_anuncio")
                  .then(pl.concat_str([
                      pl.lit("conv_grupo_"),
                      pl.col("id_cliente").cast(pl.Utf8), pl.lit("_"),
                      pl.col("id_grupo_anuncio").cast(pl.Utf8), pl.lit("_"),
                      pl.col("data"), pl.lit("_"),
                      pl.col("conversion_event")
                  ]))
                  .when(pl.col("nivel") == "anuncio")
                  .then(pl.concat_str([
                      pl.lit("conv_anuncio_"),
                      pl.col("id_cliente").cast(pl.Utf8), pl.lit("_"),
                      pl.col("id_anuncio").cast(pl.Utf8), pl.lit("_"),
                      pl.col("data"), pl.lit("_"),
                      pl.col("conversion_event")
                  ]))
                  .otherwise(pl.lit("desconhecido"))
                  .alias("chave_unica")
            ])
        else:
            # Base de performance: chave = nivel + id_cliente + id + data
            return df.with_columns([
                pl.when(pl.col("nivel") == "campanha")
                  .then(pl.concat_str([pl.lit("campanha_"), id_cliente_expr, pl.lit("_"), pl.col("id_campanha").cast(pl.Utf8), pl.lit("_"), pl.col("data")]))
                  .when(pl.col("nivel") == "grupo_anuncio")
                  .then(pl.concat_str([pl.lit("grupo_anuncio_"), id_cliente_expr, pl.lit("_"), pl.col("id_grupo_anuncio").cast(pl.Utf8), pl.lit("_"), pl.col("data")]))
                  .when(pl.col("nivel") == "anuncio")
                  .then(pl.concat_str([pl.lit("anuncio_"), id_cliente_expr, pl.lit("_"), pl.col("id_anuncio").cast(pl.Utf8), pl.lit("_"), pl.col("data")]))
                  .otherwise(pl.lit("desconhecido"))
                  .alias("chave_unica")
            ])
    df_novo = adicionar_chave_unica(df_novo).unique(subset=["chave_unica"])

    if os.path.exists(caminho_arquivo):
        agora = datetime.now()
        for arquivo in os.listdir(BACKUP_GADS_PATH):
            caminho_backup = os.path.join(BACKUP_GADS_PATH, arquivo)
            if os.path.isfile(caminho_backup):
                mtime = datetime.fromtimestamp(os.path.getmtime(caminho_backup))
                if (agora - mtime) > timedelta(days=5):
                    try:
                        os.remove(caminho_backup)
                    except:
                        pass

        data_hora = agora.strftime("%Y%m%d_%H%M%S")
        nome_base, ext = os.path.splitext(os.path.basename(caminho_arquivo))
        backup = os.path.join(BACKUP_GADS_PATH, f"{nome_base}_{data_hora}{ext}")
        shutil.copy(caminho_arquivo, backup)

        df_old = adicionar_chave_unica(pl.read_parquet(caminho_arquivo))
        novas = df_novo.select("chave_unica").to_series().to_list()
        df_old = df_old.filter(~pl.col("chave_unica").is_in(novas))

        colunas_destino = list(dict.fromkeys([*df_old.columns, *df_novo.columns]))
        for c in colunas_destino:
            tipo_novo = df_novo.schema.get(c)
            tipo_old = df_old.schema.get(c)
            tipo_destino = tipo_novo or tipo_old or pl.Utf8
            if tipo_destino == pl.Null:
                tipo_destino = tipo_old if tipo_old and tipo_old != pl.Null else pl.Utf8

            if c in df_old.columns:
                df_old = df_old.with_columns(pl.col(c).cast(tipo_destino, strict=False).alias(c))
            else:
                df_old = df_old.with_columns(pl.lit(None).cast(tipo_destino).alias(c))

            if c in df_novo.columns:
                df_novo = df_novo.with_columns(pl.col(c).cast(tipo_destino, strict=False).alias(c))
            else:
                df_novo = df_novo.with_columns(pl.lit(None).cast(tipo_destino).alias(c))

        df_comb = pl.concat([
            df_old.select(colunas_destino),
            df_novo.select(colunas_destino),
        ])
    else:
            df_comb = df_novo

    df_comb = df_comb.drop("chave_unica")
    if "frequencia" in df_comb.columns:
        df_comb = df_comb.drop("frequencia")
    df_comb.write_parquet(caminho_arquivo, compression="snappy")
    print(f"Dados salvos em {caminho_arquivo}")

def _obter_dados_nivel_duas_consultas(id_cliente: str,data_inicio: str,data_fim: str,nivel: str) -> pl.DataFrame:
    """
    Extract Google performance and conversions in separate passes
    for the requested hierarchy level and return a consolidated frame,
    including campaign type metadata.
    """
    client  = GoogleAdsClient.load_from_storage(CONFIG_GOOGLE_PATH)
    service = client.get_service("GoogleAdsService")
    mapping_list: list[str] = []

    # Query performance metrics for the selected hierarchy level.
    if nivel == "campanha":
        perf_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          metrics.impressions,
          metrics.unique_users,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.search_impression_share,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
        conv_key = ["data", "id_campanha"]

    elif nivel == "grupo_anuncio":
        perf_query = f"""
        SELECT
          segments.date,
          ad_group.id,
          ad_group.name,
          ad_group.status,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.search_impression_share,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
        conv_key = ["data", "id_campanha", "id_grupo_anuncio"]

    elif nivel == "anuncio":
        # Search impression share is not available at ad level.
        perf_query = f"""
        SELECT
          segments.date,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          ad_group.id,
          ad_group.name,
          ad_group.status,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
        conv_key = ["data", "id_campanha", "id_grupo_anuncio", "id_anuncio"]

    else:
        raise ValueError("Nivel invalido")

    # Execute the performance query.
    def _fetch_perf_rows():
        resp_perf = service.search(customer_id=id_cliente, query=perf_query)
        perf_rows = []
        for row in resp_perf:
            m = row.metrics
            perf_rows.append({
                "data":                row.segments.date,
                "nivel":               nivel,
                "tipo_campanha":       row.campaign.advertising_channel_type,
                "id_campanha":         getattr(row.campaign, "id", None),
                "nome_campanha":       getattr(row.campaign, "name", None),
                "status_campanha":     getattr(row.campaign, "status", None),
                "id_grupo_anuncio":    getattr(row.ad_group, "id", None),
                "nome_grupo_anuncio":  getattr(row.ad_group, "name", None),
                "status_grupo_anuncio":getattr(row.ad_group, "status", None),
                "id_anuncio":          getattr(row.ad_group_ad.ad, "id", None),
                "nome_anuncio":        getattr(row.ad_group_ad.ad, "name", None),
                "status_anuncio":      getattr(row.ad_group_ad, "status", None),
                "impressoes":          m.impressions,
                "alcance": (
                    m.unique_users
                    if nivel == "campanha"
                    else None
                ),
                "cliques":             m.clicks,
                "ctr":                 m.ctr,
                "custo":               m.cost_micros / 1_000_000,
                "search_impression_share":
                    (m.search_impression_share if nivel != "anuncio" else 0.0),
                "conversoes":          m.conversions,
                "conversoes_by_conversion_date":
                    m.conversions_by_conversion_date,
                "todas_conversoes":    m.all_conversions,
                "todas_conversions_by_conversion_date":
                    m.all_conversions_by_conversion_date,
                "conversao_view":      m.view_through_conversions,
                "valor_conversoes":    m.conversions_value
            })
        return perf_rows

    perf_rows = _run_with_google_ads_retry(
        _fetch_perf_rows,
        context=(
            f"performance customer={id_cliente} nivel={nivel} "
            f"periodo={data_inicio}..{data_fim}"
        ),
    )
    df_perf = pl.DataFrame(perf_rows)

    # Query conversion metrics for the selected hierarchy level.
    if nivel == "campanha":
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.advertising_channel_type,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    elif nivel == "grupo_anuncio":
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.advertising_channel_type,
          ad_group.id,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:  # anuncio
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.advertising_channel_type,
          ad_group.id,
          ad_group_ad.ad.id,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """

    # Execute the conversion query.
    def _fetch_conv_data():
        resp_conv = service.search(customer_id=id_cliente, query=conv_query)
        conv_data = []
        for row in resp_conv:
            ev = row.segments.conversion_action_name
            m = row.metrics
            rec = {
                "data":                row.segments.date,
                "tipo_campanha":       row.campaign.advertising_channel_type,
                "conversion_action_name": ev,
                "conversoes":          m.conversions,
                "conversoes_by_conversion_date":        m.conversions_by_conversion_date,
                "todas_conversoes":    m.all_conversions,
                "todas_conversions_by_conversion_date":         m.all_conversions_by_conversion_date,
                "conversao_view":      m.view_through_conversions,
                "valor_conversoes":    m.conversions_value
            }
            if nivel == "campanha":
                rec["id_campanha"] = row.campaign.id
            elif nivel == "grupo_anuncio":
                rec.update({
                    "id_campanha":      row.campaign.id,
                    "id_grupo_anuncio": row.ad_group.id
                })
            else:  # anuncio
                rec.update({
                    "id_campanha":      row.campaign.id,
                    "id_grupo_anuncio": row.ad_group.id,
                    "id_anuncio":       row.ad_group_ad.ad.id
                })
            conv_data.append(rec)
        return conv_data

    conv_data = _run_with_google_ads_retry(
        _fetch_conv_data,
        context=(
            f"conversoes customer={id_cliente} nivel={nivel} "
            f"periodo={data_inicio}..{data_fim}"
        ),
    )

    # Aggregate conversion metrics before joining them back to performance.
    if normalize_account_id(id_cliente) in CONVERSION_AGGREGATION_ACCOUNT_IDS:
        # Apply the account-specific conversion option configured in accounts.yaml.
        df_conv = pl.DataFrame(conv_data) if conv_data else pl.DataFrame({
            **{k: [] for k in conv_key},
            **{m: [] for m in [
                "conversoes", "conversoes_by_conversion_date",
                "todas_conversoes", "todas_conversions_by_conversion_date",
                "conversao_view", "valor_conversoes"
            ]}
        })
        df_cross = df_conv.group_by(conv_key).agg([
            pl.col("conversoes").sum().alias("conversoes_cross"),
            pl.col("conversoes_by_conversion_date").sum()
              .alias("conversoes_by_conversion_date_cross"),
            pl.col("todas_conversoes").sum().alias("todas_conversoes_cross"),
            pl.col("todas_conversions_by_conversion_date").sum()
              .alias("todas_conversions_by_conversion_date_cross"),
            pl.col("conversao_view").sum().alias("conversao_view_cross"),
            pl.col("valor_conversoes").sum().alias("valor_conversoes_cross")
        ])

        def _match(ev_name: str) -> str:
            for ev in mapping_list:
                if ev in ev_name:
                    return ev
            return None

        df_perf = df_perf.with_columns(
            pl.col("nome_campanha").map_elements(_match).alias("matched_event")
        )

        df_key     = df_perf.select(conv_key + ["matched_event"])
        df_join    = df_conv.join(df_key, on=conv_key, how="left")
        df_matched = df_join.filter(
            pl.col("conversion_action_name") == pl.col("matched_event")
        )
        df_pers = df_matched.group_by(conv_key).agg([
            pl.col("conversoes").sum().alias("conversoes_pers"),
            pl.col("conversoes_by_conversion_date").sum()
              .alias("conversoes_by_conversion_date_pers"),
            pl.col("todas_conversoes").sum().alias("todas_conversoes_pers"),
            pl.col("todas_conversions_by_conversion_date").sum()
              .alias("todas_conversions_by_conversion_date_pers"),
            pl.col("conversao_view").sum().alias("conversao_view_pers"),
            pl.col("valor_conversoes").sum().alias("valor_conversoes_pers")
        ])

        df_agg = df_cross.join(df_pers, on=conv_key, how="left").with_columns([
            pl.col("conversoes_pers").fill_null(pl.col("conversoes_cross")),
            pl.col("conversoes_by_conversion_date_pers")
              .fill_null(pl.col("conversoes_by_conversion_date_cross")),
            pl.col("todas_conversoes_pers")
              .fill_null(pl.col("todas_conversoes_cross")),
            pl.col("todas_conversions_by_conversion_date_pers")
              .fill_null(pl.col("todas_conversions_by_conversion_date_cross")),
            pl.col("conversao_view_pers")
              .fill_null(pl.col("conversao_view_cross")),
            pl.col("valor_conversoes_pers")
              .fill_null(pl.col("valor_conversoes_cross"))
        ])
    else:
        # Default path.
        if conv_data:
            df_conv = pl.DataFrame(conv_data)
            df_agg = df_conv.group_by(conv_key).agg([
                pl.col("conversoes").sum().alias("conversoes_pers"),
                pl.col("conversoes_by_conversion_date").sum()
                  .alias("conversoes_by_conversion_date_pers"),
                pl.col("todas_conversoes").sum().alias("todas_conversoes_pers"),
                pl.col("todas_conversions_by_conversion_date").sum()
                  .alias("todas_conversions_by_conversion_date_pers"),
                pl.col("conversao_view").sum().alias("conversao_view_pers"),
                pl.col("valor_conversoes").sum().alias("valor_conversoes_pers")
            ])
        else:
            df_agg = pl.DataFrame({
                **{k: [] for k in conv_key},
                **{m: [] for m in [
                    "conversoes_pers", "conversoes_by_conversion_date_pers",
                    "todas_conversoes_pers",
                    "todas_conversions_by_conversion_date_pers",
                    "conversao_view_pers", "valor_conversoes_pers"
                ]}
            })

    # Align data types and join performance with conversions.
    for k in conv_key:
        df_perf = df_perf.with_columns(pl.col(k).cast(pl.Utf8))
        df_agg  = df_agg.with_columns(pl.col(k).cast(pl.Utf8))

    df_merged = df_perf.join(
        df_agg, left_on=conv_key, right_on=conv_key, how="left"
    ).drop("matched_event", strict=False)

    return df_merged

def _get_raw_conversions(id_cliente: str, data_inicio: str, data_fim: str, nivel: str) -> pl.DataFrame:
    """Extract raw Google conversion events without applying business filters."""
    client = GoogleAdsClient.load_from_storage(CONFIG_GOOGLE_PATH)
    service = client.get_service("GoogleAdsService")

    if nivel == "campanha":
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    elif nivel == "grupo_anuncio":
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    elif nivel == "anuncio":
        conv_query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_ad.ad.id,
          ad_group_ad.ad.name,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_by_conversion_date,
          metrics.all_conversions,
          metrics.all_conversions_by_conversion_date,
          metrics.view_through_conversions,
          metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:
        raise ValueError("Nivel invalido")

    def _fetch_rows():
        resp = service.search(customer_id=id_cliente, query=conv_query)
        rows = []
        for row in resp:
            ev = row.segments.conversion_action_name
            m = row.metrics
            record = {
                "id_cliente": id_cliente,
                "nivel": nivel,
                "data": row.segments.date,
                "conversion_event": ev,
                "id_campanha": getattr(row.campaign, "id", None),
                "nome_campanha": getattr(row.campaign, "name", None),
                "id_grupo_anuncio": getattr(row.ad_group, "id", None),
                "nome_grupo_anuncio": getattr(row.ad_group, "name", None),
                "id_anuncio": getattr(row.ad_group_ad.ad, "id", None),
                "nome_anuncio": getattr(row.ad_group_ad.ad, "name", None),
                "conversoes": m.conversions,
                "conversoes_by_conversion_date": m.conversions_by_conversion_date,
                "todas_conversoes": m.all_conversions,
                "todas_conversions_by_conversion_date": m.all_conversions_by_conversion_date,
                "conversao_view": m.view_through_conversions,
                "valor_conversoes": m.conversions_value
            }
            rows.append(record)
        return rows

    rows = _run_with_google_ads_retry(
        _fetch_rows,
        context=(
            f"raw_conversions customer={id_cliente} nivel={nivel} "
            f"periodo={data_inicio}..{data_fim}"
        ),
    )
    
    if not rows:
        schema = {
            "id_cliente": pl.Utf8, "nivel": pl.Utf8, "data": pl.Utf8,
            "conversion_event": pl.Utf8, "id_campanha": pl.Int64,
            "nome_campanha": pl.Utf8, "id_grupo_anuncio": pl.Int64,
            "nome_grupo_anuncio": pl.Utf8, "id_anuncio": pl.Int64,
            "nome_anuncio": pl.Utf8, "conversoes": pl.Float64,
            "conversoes_by_conversion_date": pl.Float64,
            "todas_conversoes": pl.Float64,
            "todas_conversions_by_conversion_date": pl.Float64,
            "conversao_view": pl.Int64, "valor_conversoes": pl.Float64
        }
        return pl.DataFrame(data=[], schema=schema)
    
    return pl.DataFrame(rows)

def obter_dados_google_ads(id_cliente: str, data_inicio: str, data_fim: str) -> pl.DataFrame:
    """Collect Google performance data for campaign, ad group and ad levels."""
    data_i = datetime.strptime(data_inicio, "%Y%m%d").strftime("%Y-%m-%d")
    data_f = datetime.strptime(data_fim, "%Y%m%d").strftime("%Y-%m-%d")

    df_c = _obter_dados_nivel_duas_consultas(id_cliente, data_i, data_f, nivel="campanha")
    df_g = _obter_dados_nivel_duas_consultas(id_cliente, data_i, data_f, nivel="grupo_anuncio")
    df_a = _obter_dados_nivel_duas_consultas(id_cliente, data_i, data_f, nivel="anuncio")

    frames_nivel = [df_c, df_g, df_a]
    all_cols = sorted(set(df_c.columns) | set(df_g.columns) | set(df_a.columns))
    tipos = {}
    for column in all_cols:
        dtypes = [df.schema[column] for df in frames_nivel if column in df.columns]
        dtypes_nao_nulos = [dtype for dtype in dtypes if dtype != pl.Null]
        if not dtypes_nao_nulos:
            tipos[column] = pl.Utf8
            continue

        primeiro_tipo = dtypes_nao_nulos[0]
        tipos[column] = (
            primeiro_tipo
            if all(dtype == primeiro_tipo for dtype in dtypes_nao_nulos)
            else pl.Utf8
        )

    def fix(df):
        return df.select([
            pl.col(c).cast(tipos[c]) if c in df.columns else pl.lit(None).cast(tipos[c]).alias(c)
            for c in all_cols
        ])

    df = pl.concat([fix(df_c), fix(df_g), fix(df_a)], how="vertical")
    if "alcance" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("alcance"))
    else:
        df = df.with_columns(pl.col("alcance").cast(pl.Float64, strict=False).alias("alcance"))
    if "frequencia" in df.columns:
        df = df.drop("frequencia")

    produto = get_product_for_account(id_cliente, "google")
    df = df.with_columns([
        pl.lit(id_cliente).cast(pl.Utf8).alias("id_cliente"),
        pl.lit(produto).cast(pl.Utf8).alias("produto"),
    ])
    return df

def baixar_assets_midias(id_cliente: str):
    """Download recent Google ad assets for the provided customer."""
    client = GoogleAdsClient.load_from_storage(CONFIG_GOOGLE_PATH)
    service = client.get_service("GoogleAdsService")

    hoje = datetime.today()
    d1 = (hoje - timedelta(days=7)).strftime("%Y%m%d")
    d2 = hoje.strftime("%Y-%m-%d")

    consulta = f"""
        SELECT
          ad_group_ad.ad.id,
          ad_group_ad.ad.type,
          ad_group_ad.ad.app_ad.images,
          ad_group_ad.ad.responsive_display_ad.marketing_images,
          ad_group_ad.ad.responsive_display_ad.square_marketing_images,
          metrics.impressions
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{d1}' AND '{d2}'
          AND ad_group_ad.status = 'ENABLED'
          AND metrics.impressions > 0
        ORDER BY ad_group_ad.ad.id
    """
    resp = service.search(customer_id=id_cliente, query=consulta)
    assets = {}
    for row in resp:
        a = row.ad_group_ad.ad
        refs = []
        if a.responsive_display_ad:
            refs += [img.asset for img in a.responsive_display_ad.marketing_images or []]
            refs += [img.asset for img in a.responsive_display_ad.square_marketing_images or []]
        if a.app_ad:
            refs += [img.asset for img in a.app_ad.images or []]
        if refs:
            assets[row.ad_group_ad.ad.id] = refs

    if not assets:
        print("Nenhum anúncio com assets encontrado.")
        return

    unique = set(sum(assets.values(), []))
    q = f"""
        SELECT
          asset.id,
          asset.resource_name,
          asset.image_asset.mime_type,
          asset.image_asset.full_size.url
        FROM asset
        WHERE asset.resource_name IN ({','.join(f'"{r}"' for r in unique)})
    """
    resp2 = service.search(customer_id=id_cliente, query=q)
    info = {row.asset.resource_name: row.asset.image_asset for row in resp2}

    outdir = os.path.join(AD_ASSETS_PATH_GOOGLE, id_cliente)
    os.makedirs(outdir, exist_ok=True)

    for ad_id, refs in assets.items():
        for r in refs:
            ia = info.get(r)
            if not ia or not ia.full_size.url:
                continue
            mime = ia.mime_type
            ext = EXT_BY_ADSAPI.get(mime, mimetypes.guess_extension(mime) or "bin").lstrip(".")
            url = ia.full_size.url
            try:
                resp = requests.get(url, stream=True, timeout=30)
                resp.raise_for_status()
                path = os.path.join(outdir, f"{id_cliente}_{ad_id}.{ext}")
                if not os.path.exists(path):
                    with open(path, "wb") as f:
                        f.write(resp.content)
                    print(f"Salvo {path}")
            except Exception as e:
                print(f"Erro asset {r}: {e}")


def executar_google_ads(data_inicio: str,
                        data_fim: str,
                        baixar_midias: bool = True,
                        ids_clientes: list[str] | None = None,
                        data_inicio_conversoes: str | None = None):
    """Run the Google extraction flow for performance, conversions and optional assets."""
    ids_clientes = resolve_selected_ids(ids_clientes, CUSTOMER_IDS_GOOGLE)
    
    # Reuse the performance window when no dedicated conversion window is informed.
    if data_inicio_conversoes is None:
        data_inicio_conversoes = data_inicio

    all_perf = []
    all_conv = []
    for cid in ids_clientes:
        print(f"\n--- Cliente {cid} ---")
        # Collect performance metrics for the requested window.
        df_perf = obter_dados_google_ads(cid, data_inicio, data_fim)
        all_perf.append(df_perf)
        print(f"Performance processada: {df_perf.shape}")

        # Collect raw conversions for every hierarchy level.
        di_conv = datetime.strptime(data_inicio_conversoes, "%Y%m%d").strftime("%Y-%m-%d")
        dfm     = datetime.strptime(data_fim, "%Y%m%d").strftime("%Y-%m-%d")
        for nivel in ("campanha", "grupo_anuncio", "anuncio"):
            all_conv.append(_get_raw_conversions(cid, di_conv, dfm, nivel))

        # baixa assets se solicitado
        if baixar_midias:
            baixar_assets_midias(cid)

    if all_perf:
        df_perf_all = align_and_concat_frames(all_perf)
        inserir_substituir(df_perf_all, GOOGLE_PERFORMANCE_OUTPUT_FILE)
    else:
        print("Nenhum dado de performance encontrado.")

    # concatena tudo e salva
    if all_conv:
        df_conv_all = pl.concat(all_conv, how="vertical")

        df_conv_all = df_conv_all.with_columns(
            pl.col("id_cliente")
              .map_elements(lambda x: get_product_for_account(x, "google"), return_dtype=pl.Utf8)
              .alias("produto")
        )

        # reorganiza ordem de colunas
        cols = [
            "nivel", "id_cliente", "produto", "data", "conversion_event",
            "conversoes", "conversoes_by_conversion_date",
            "todas_conversoes", "todas_conversions_by_conversion_date",
            "conversao_view", "valor_conversoes",
            "id_campanha", "nome_campanha",
            "id_grupo_anuncio", "nome_grupo_anuncio",
            "id_anuncio"
        ]
        if "nome_anuncio" in df_conv_all.columns:
            cols.append("nome_anuncio")

        df_conv_all = df_conv_all.select([c for c in cols if c in df_conv_all.columns])

        path = os.path.join(str(OUTPUT_PATH), "google_ads_conversions_all_new.parquet")
        inserir_substituir(df_conv_all, path)
    else:
        print("Nenhuma conversão encontrada.")
    

def main():
    """Execute a local default Google extraction window for manual runs."""
    baixar_flag = False
    ids_especificos = []
    data_inicio_perf = (datetime.today() - timedelta(days=180)).strftime("%Y%m%d")
    data_inicio_conv = (datetime.today() - timedelta(days=180)).strftime("%Y%m%d")
    data_fim = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

    executar_google_ads(
        data_inicio=data_inicio_perf,
        data_fim=data_fim,
        baixar_midias=baixar_flag,
        ids_clientes=ids_especificos,
        data_inicio_conversoes=data_inicio_conv
    )

if __name__ == "__main__":
    main()
