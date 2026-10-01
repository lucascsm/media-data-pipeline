import os
import shutil
import requests
import json
from datetime import datetime, timedelta
import polars as pl
from typing import Optional
import mimetypes

from src.utils_do_pai import (
    OUTPUT_PATH,
    CONFIG_TIKTOK_PATH,
    BACKUP_TIKTOK_PATH,
    AD_ASSETS_PATH_TIKTOK,
    TIKTOK_API_BASE,
)
from src.account_catalog import get_product_for_account, list_accounts
from src.event_long_format import to_long_event_frame
from src.pipeline_utils import align_and_concat_frames, load_yaml_config, resolve_selected_ids

TIKTOK_OUTPUT_FILE = os.path.join(str(OUTPUT_PATH), "dados_tiktok_all_new.parquet")
TIKTOK_ACCOUNTS = list_accounts("tiktok")

TIKTOK_BASE_COLUMNS = [
    "currency",
    "spend",
    "adgroup_name",
    "data",
    "id_grupo_anuncio",
    "nivel",
    "produto",
    "alcance",
    "id_cliente",
    "adgroup_id",
    "campaign_id",
    "campaign_budget",
    "budget",
    "id_anuncio",
    "impressoes",
    "conversion",
    "nome_campanha",
    "ad_name",
    "cliques",
    "id_campanha",
    "ad_id",
    "result",
    "ctr",
    "objective_type",
    "advertiser_name",
    "app_promotion_type",
]


def _produto_por_advertiser(advertiser_id: str) -> str | None:
    return get_product_for_account(advertiser_id, "tiktok")

def carregar_config_tiktok(caminho_config: str) -> dict:
    """Load the TikTok API configuration from disk."""
    return load_yaml_config(caminho_config)


def _replace_dot_in_column_names(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {}
    drop_cols = []
    existing_cols = set(df.columns)

    for col in df.columns:
        if "." not in col and "-" not in col:
            continue

        normalized = col.replace(".", "_").replace("-", "_")
        if normalized in existing_cols or normalized in rename_map.values():
            drop_cols.append(col)
            continue
        rename_map[col] = normalized

    if rename_map:
        df = df.rename(rename_map)
    if drop_cols:
        df = df.drop(drop_cols)
    return df


def _to_tiktok_event_long_format(df: pl.DataFrame) -> pl.DataFrame:
    if "stat_time_day" in df.columns:
        df = df.drop("stat_time_day")

    if "reach" in df.columns:
        if "alcance" in df.columns:
            df = df.with_columns(
                pl.coalesce([pl.col("alcance"), pl.col("reach")]).alias("alcance")
            ).drop("reach")
        else:
            df = df.rename({"reach": "alcance"})
    for legacy_column in ("frequency", "frequencia"):
        if legacy_column in df.columns:
            df = df.drop(legacy_column)

    for column in TIKTOK_BASE_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    ordered_base_columns = [column for column in TIKTOK_BASE_COLUMNS if column in df.columns]
    return to_long_event_frame(df, ordered_base_columns)

def inserir_substituir(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    """Merge the new TikTok extract into the parquet target using stable refresh keys."""
    if df_novo.height == 0 or len(df_novo.columns) == 0:
        print(f"DataFrame vazio. Nada a inserir em {caminho_arquivo}.")
        return

    df_novo = _replace_dot_in_column_names(df_novo)

    # 2.1 Remove backups antigos
    agora = datetime.now()
    for arquivo in os.listdir(BACKUP_TIKTOK_PATH):
        caminho_backup = os.path.join(BACKUP_TIKTOK_PATH, arquivo)
        if os.path.isfile(caminho_backup):
            mtime = datetime.fromtimestamp(os.path.getmtime(caminho_backup))
            if (agora - mtime) > timedelta(days=5):
                try:
                    os.remove(caminho_backup)
                    print(f"Backup removido (mais de 5 dias): {caminho_backup}")
                except Exception as e:
                    print(f"Erro ao remover backup {caminho_backup}: {e}")

    # 2.2 Função auxiliar para criar coluna 'chave_unica'
    def adicionar_chave_unica(df: pl.DataFrame) -> pl.DataFrame:
        id_cliente_expr = (
            pl.col("id_cliente").cast(pl.Utf8)
            if "id_cliente" in df.columns
            else pl.lit("sem_id_cliente")
        )
        evento_expr = (
            pl.when(pl.col("evento").cast(pl.Utf8).fill_null("") == "")
              .then(pl.lit(""))
              .otherwise(pl.concat_str([pl.lit("_"), pl.col("evento").cast(pl.Utf8)]))
            if "evento" in df.columns
            else pl.lit("")
        )
        if "stat_time_day" not in df.columns:
            if "data" in df.columns:
                df = df.with_columns([pl.col("data").alias("stat_time_day")])
            else:
                df = df.with_columns([pl.lit("").alias("stat_time_day")])
        return df.with_columns([
            pl.when(pl.col("nivel") == "campanha")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("campaign_"), pl.col("id_campanha").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day"), evento_expr
              ]))
              .when(pl.col("nivel") == "grupo_anuncio")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("adgroup_"), pl.col("id_grupo_anuncio").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day"), evento_expr
              ]))
              .when(pl.col("nivel") == "anuncio")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("ad_"), pl.col("id_anuncio").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day"), evento_expr
              ]))
              .otherwise(pl.lit("desconhecido"))
              .alias("chave_unica")
        ])

    def adicionar_chave_refresh(df: pl.DataFrame) -> pl.DataFrame:
        id_cliente_expr = (
            pl.col("id_cliente").cast(pl.Utf8)
            if "id_cliente" in df.columns
            else pl.lit("sem_id_cliente")
        )
        if "stat_time_day" not in df.columns:
            if "data" in df.columns:
                df = df.with_columns([pl.col("data").alias("stat_time_day")])
            else:
                df = df.with_columns([pl.lit("").alias("stat_time_day")])
        return df.with_columns([
            pl.when(pl.col("nivel") == "campanha")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("campaign_"), pl.col("id_campanha").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day")
              ]))
              .when(pl.col("nivel") == "grupo_anuncio")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("adgroup_"), pl.col("id_grupo_anuncio").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day")
              ]))
              .when(pl.col("nivel") == "anuncio")
              .then(pl.concat_str([
                  id_cliente_expr, pl.lit("_"),
                  pl.lit("ad_"), pl.col("id_anuncio").cast(pl.Utf8),
                  pl.lit("_"), pl.col("stat_time_day")
              ]))
              .otherwise(pl.lit("desconhecido"))
              .alias("chave_refresh")
        ])

    df_novo = adicionar_chave_refresh(adicionar_chave_unica(df_novo))

    # 2.4 Se já existir arquivo, faz backup + merge
    if os.path.exists(caminho_arquivo):
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
        nome_backup = f"{nome_base}_{data_hora}{extensao}"
        arquivo_backup = os.path.join(BACKUP_TIKTOK_PATH, nome_backup)
        try:
            shutil.copy2(caminho_arquivo, arquivo_backup)
            print(f"Backup realizado: {arquivo_backup}")
        except Exception as e:
            print(f"Erro ao realizar backup do arquivo {caminho_arquivo}: {e}")

        df_antigo = pl.read_parquet(caminho_arquivo)
        df_antigo = _replace_dot_in_column_names(df_antigo)
        df_antigo = adicionar_chave_refresh(adicionar_chave_unica(df_antigo))

        all_cols = list(set(df_antigo.columns).union(set(df_novo.columns)))
        for col in all_cols:
            if col not in df_novo.columns:
                dtype = df_antigo.schema[col] if col in df_antigo.columns else pl.Utf8
                df_novo = df_novo.with_columns([pl.lit(None).cast(dtype).alias(col)])
            if col not in df_antigo.columns:
                dtype = df_novo.schema[col] if col in df_novo.columns else pl.Utf8
                df_antigo = df_antigo.with_columns([pl.lit(None).cast(dtype).alias(col)])
        
        df_novo = df_novo.select(all_cols)
        df_antigo = df_antigo.select(all_cols)

        for col in all_cols:
            tipo_novo = df_novo.schema[col]
            tipo_antigo = df_antigo.schema[col]
            if tipo_novo == pl.Null and tipo_antigo != pl.Null:
                df_novo = df_novo.with_columns([pl.col(col).cast(tipo_antigo)])
            elif tipo_antigo == pl.Null and tipo_novo != pl.Null:
                df_antigo = df_antigo.with_columns([pl.col(col).cast(tipo_novo)])

        numeric_casts = {
            "impressoes": pl.Int64, "cliques": pl.Int64, "ctr": pl.Float64,
            "alcance": pl.Float64, "spend": pl.Float64,
            "conversion": pl.Int64, "real_time_conversion": pl.Int64,
            "result": pl.Int64, "real_time_result": pl.Int64, "secondary_goal_result": pl.Int64,
            "valor": pl.Float64,
        }

        for col, dtype in numeric_casts.items():
            if col in df_novo.columns:
                df_novo = df_novo.with_columns([pl.col(col).cast(dtype, strict=False).alias(col)])
            if col in df_antigo.columns:
                df_antigo = df_antigo.with_columns([pl.col(col).cast(dtype, strict=False).alias(col)])

        chaves_refresh = df_novo.select("chave_refresh").unique().to_series().to_list()
        df_antigo_filtrado = df_antigo.filter(~pl.col("chave_refresh").is_in(chaves_refresh))
        df_combinado = pl.concat([df_antigo_filtrado, df_novo])
    else:
        df_combinado = df_novo

    # 2.5 Remove chaves auxiliares e salva
    drop_cols = [col for col in ("chave_unica", "chave_refresh") if col in df_combinado.columns]
    if drop_cols:
        df_combinado = df_combinado.drop(drop_cols)
    if "frequencia" in df_combinado.columns:
        df_combinado = df_combinado.drop("frequencia")
    df_combinado.write_parquet(caminho_arquivo, compression="snappy")
    print(f"Dados salvos em {caminho_arquivo}")

def _has_next_page(page: int, page_size: int, page_rows: int, page_info: dict) -> bool:
    has_next = page_info.get("has_next_page")
    if isinstance(has_next, bool):
        return has_next

    total_page = page_info.get("total_page")
    if total_page is not None:
        try:
            return page < int(total_page)
        except (TypeError, ValueError):
            pass

    total_number = page_info.get("total_number")
    if total_number is not None:
        try:
            return page * page_size < int(total_number)
        except (TypeError, ValueError):
            pass

    return page_rows >= page_size


def _is_tiktok_metric_error(body: dict, metric: str) -> bool:
    message = str(body.get("message") or "").lower()
    return metric.lower() in message or "metric" in message or "metrics" in message


def _log_tiktok_reach_response(report_level: str, body: dict) -> None:
    if body.get("code") != 0:
        print(
            f"TikTok {report_level}: chamada retornou erro antes de confirmar alcance "
            f"(code={body.get('code')}, message={body.get('message')})."
        )
        return

    rows = ((body.get("data") or {}).get("list") or [])
    if not rows:
        print(f"TikTok {report_level}: resposta sem linhas; reach foi solicitado, mas nao ha amostra para confirmar.")
        return

    sample_metrics = rows[0].get("metrics") or {}
    if "reach" in sample_metrics:
        print(f"TikTok {report_level}: metric reach retornou e sera gravada como alcance.")
    elif "frequency" in sample_metrics:
        print(
            f"TikTok {report_level}: aviso - resposta retornou frequency, nao reach; "
            "alcance ficara nulo nessa coleta."
        )
    else:
        print(
            f"TikTok {report_level}: reach nao apareceu na resposta; "
            f"metricas recebidas na amostra: {sorted(sample_metrics.keys())}."
        )


def _fetch_integrated_paginated(
    endpoint: str,
    headers: dict,
    base_params: dict,
    context_label: str,
) -> dict:
    page_size = int(base_params.get("page_size", 1000))
    page = 1
    all_records: list[dict] = []
    max_pages = 5000

    while page <= max_pages:
        params = dict(base_params)
        params["page"] = page
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            body = response.json()
        except Exception as e:
            return {
                "code": -1,
                "message": f"Erro ao consultar API ({context_label}) na pagina {page}: {e}",
                "data": {"list": all_records},
            }

        if body.get("code") != 0:
            return body

        data = body.get("data", {}) or {}
        records = data.get("list", []) or []
        all_records.extend(records)

        page_info = data.get("page_info", {}) or {}
        if not records or not _has_next_page(page, page_size, len(records), page_info):
            if page > 1:
                print(f"{context_label}: {len(all_records)} registros coletados em {page} paginas.")
            return {
                "code": 0,
                "message": "OK",
                "data": {
                    "list": all_records,
                    "page_info": {
                        "last_page_fetched": page,
                        "rows_collected": len(all_records),
                    },
                },
            }

        page += 1

    print(f"{context_label}: limite de paginas atingido ({max_pages}).")
    return {
        "code": 0,
        "message": "OK",
        "data": {
            "list": all_records,
            "page_info": {
                "last_page_fetched": max_pages,
                "rows_collected": len(all_records),
            },
        },
    }


def fetch_report_data(advertiser_id: str, report_level: str, start_date: str, end_date: str, access_token: str) -> dict:
    """Fetch paginated TikTok performance metrics for one hierarchy level."""
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    endpoint = f"{TIKTOK_API_BASE}/report/integrated/get/"
    if report_level == "AUCTION_CAMPAIGN":
        dimensions = ["stat_time_day", "campaign_id"]
        metrics = [
            "impressions", "reach", "clicks", "spend", "ctr", "conversion", "real_time_conversion",
            "result", "real_time_result", "secondary_goal_result",
            "campaign_name", "objective_type", "advertiser_name", "campaign_budget", "currency"
        ]
    elif report_level == "AUCTION_ADGROUP":
        dimensions = ["stat_time_day", "adgroup_id"]
        metrics = [
            "impressions", "reach", "clicks", "spend", "ctr", "conversion", "real_time_conversion",
            "result", "real_time_result", "secondary_goal_result", "adgroup_name","campaign_name"
        ]
    elif report_level == "AUCTION_AD":
        dimensions = ["stat_time_day", "ad_id"]
        metrics = [
            "impressions", "reach", "clicks", "spend", "ctr", "conversion", "real_time_conversion",
            "result", "real_time_result", "secondary_goal_result", "ad_name","adgroup_name","campaign_name"
        ]
    else:
        print("Nível inválido:", report_level)
        return {}
    params = {
        "advertiser_id": advertiser_id, "report_type": "BASIC", "data_level": report_level,
        "dimensions": json.dumps(dimensions), "metrics": json.dumps(metrics),
        "start_date": start_date, "end_date": end_date, "page": 1, "page_size": 1000
    }
    body = _fetch_integrated_paginated(
        endpoint=endpoint,
        headers=headers,
        base_params=params,
        context_label=f"Performance {report_level}",
    )
    if body.get("code") != 0 and _is_tiktok_metric_error(body, "reach"):
        fallback_metrics = [metric for metric in metrics if metric != "reach"]
        fallback_params = dict(params)
        fallback_params["metrics"] = json.dumps(fallback_metrics)
        print(f"TikTok nao retornou metric reach para {report_level}; tentando sem alcance.")
        fallback_body = _fetch_integrated_paginated(
            endpoint=endpoint,
            headers=headers,
            base_params=fallback_params,
            context_label=f"Performance {report_level} sem reach",
        )
        print(f"TikTok {report_level}: coleta concluida sem reach; coluna alcance ficara nula.")
        return fallback_body
    _log_tiktok_reach_response(report_level, body)
    return body
    
def fetch_custom_event_data(advertiser_id: str, report_level: str, start_date: str, end_date: str, access_token: str) -> pl.DataFrame:
    """Fetch TikTok custom events and pivot them before the long-format conversion."""
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    endpoint = f"{TIKTOK_API_BASE}/report/integrated/get/"

    id_map = {
        "AUCTION_CAMPAIGN": "campaign_id", "AUCTION_ADGROUP": "adgroup_id", "AUCTION_AD": "ad_id"
    }
    id_dimension = id_map.get(report_level)
    if not id_dimension:
        print(f"Nível de relatório de eventos inválido: {report_level}")
        return pl.DataFrame()

    dimensions = ["stat_time_day", id_dimension, "custom_event_type"]
    metrics = ["custom_app_events"]
    params = {
        "advertiser_id": advertiser_id, "report_type": "BASIC", "data_level": report_level,
        "dimensions": json.dumps(dimensions), "metrics": json.dumps(metrics),
        "start_date": start_date, "end_date": end_date, "page_size": 1000
    }
    
    print(f"Coletando Custom Events para {report_level}...")
    body = _fetch_integrated_paginated(
        endpoint=endpoint,
        headers=headers,
        base_params=params,
        context_label=f"Custom Events {report_level}",
    )

    if body.get("code") != 0:
        print(f"Erro ao buscar Custom Events para {report_level}: {body.get('message')}")
        return pl.DataFrame()

    records = body.get("data", {}).get("list", [])
    if not records:
        print(f"Nenhum Custom Event encontrado para {report_level}.")
        return pl.DataFrame()

    rows = []
    for item in records:
        dims = item["dimensions"]
        mets = item["metrics"]
        rows.append({
            "stat_time_day": dims.get("stat_time_day"),
            "id": dims.get(id_dimension),
            "custom_event_type": dims.get("custom_event_type"),
            "custom_app_events": int(mets.get("custom_app_events", 0))
        })
    
    df = pl.DataFrame(rows)

    # 1. Normaliza os nomes dos eventos ANTES de pivotar para evitar duplicatas de-
    #    correntes de diferenças de capitalização (ex: "Evento" e "evento").
    df = df.with_columns(
        pl.col("custom_event_type").str.to_lowercase().alias("normalized_event_type")
    )

    # 2. Agrega os valores para os nomes de eventos agora normalizados.
    #    Se havia um 'Evento' e um 'evento' para o mesmo dia/id, seus valores serão somados.
    df_aggregated = df.group_by(["stat_time_day", "id", "normalized_event_type"]).agg(
        pl.col("custom_app_events").sum()
    )

    # 3. Pivota o DataFrame agregado usando os nomes já normalizados, garantindo que não haverá duplicatas.
    df_pivoted = df_aggregated.pivot(
        index=["stat_time_day", "id"],
        on="normalized_event_type",
        values="custom_app_events",
    ).fill_null(0)
    
    # 4. Renomeia as colunas adicionando o prefixo "evento_". A normalização já foi feita.
    new_column_names = {
        col: f"evento_{col}" 
        for col in df_pivoted.columns if col not in ["stat_time_day", "id"]
    }
    
    return df_pivoted.rename(new_column_names)

def transformar_registro(reg: dict, advertiser_id: str) -> dict:
    """Normalize a TikTok API row into the project schema."""
    dims = reg.get("dimensions", {})
    metrics_data = reg.get("metrics", {})

    def safe_get(key: str, default: Optional[str] = "", dtype=str):
        val = metrics_data.get(key) or dims.get(key) or default
        try:
            return dtype(str(val).replace("-", "0").replace(",", "."))
        except Exception:
            return default

    dims.setdefault("campaign_id", "")
    dims.setdefault("adgroup_id", "")
    dims.setdefault("ad_id", "")

    registro = {
        "data": reg.get("stat_time_day") or dims.get("stat_time_day", ""),
        "nivel": reg.get("nivel", ""), "id_cliente": advertiser_id,
        "campaign_id": dims.get("campaign_id"),
        "id_campanha": dims.get("campaign_id") if reg.get("nivel") == "AUCTION_CAMPAIGN" else "",
        "nome_campanha": safe_get("campaign_name", "", str), "objective_type": safe_get("objective_type", "", str),
        "advertiser_name": safe_get("advertiser_name", "", str),
        "campaign_budget": safe_get("campaign_budget", "0.0", float),
        "currency": safe_get("currency", "BRL", str),
        "adgroup_id": dims.get("adgroup_id"),
        "id_grupo_anuncio": dims.get("adgroup_id") if reg.get("nivel") == "AUCTION_ADGROUP" else "",
        "adgroup_name": safe_get("adgroup_name", "", str), "ad_id": dims.get("ad_id"),
        "id_anuncio": dims.get("ad_id") if reg.get("nivel") == "AUCTION_AD" else "",
        "ad_name": safe_get("ad_name", "", str), "budget": safe_get("budget", "0.0", float),
        "app_promotion_type": safe_get("app_promotion_type", "", str),
        "impressoes": safe_get("impressions", "0", int),
        "alcance": safe_get("reach", None, float),
        "cliques": safe_get("clicks", "0", int),
        "ctr": safe_get("ctr", "0.0", float), "spend": safe_get("spend", "0.0", float),
        "conversion": safe_get("conversion", "0", int), "real_time_conversion": safe_get("real_time_conversion", "0", int),
        "result": safe_get("result", "0", int), "real_time_result": safe_get("real_time_result", "0", int),
        "secondary_goal_result": safe_get("secondary_goal_result", "0", int)
    }
    if reg.get("nivel") == "AUCTION_CAMPAIGN":
        registro["nivel"] = "campanha"
    elif reg.get("nivel") == "AUCTION_ADGROUP":
        registro["nivel"] = "grupo_anuncio"
    elif reg.get("nivel") == "AUCTION_AD":
        registro["nivel"] = "anuncio"
    return registro

def obter_dados_tiktok(advertiser_id: str, data_inicio: str, data_fim: str, config: dict) -> pl.DataFrame:
    """Collect TikTok performance and event data for the requested advertiser window."""
    access_token = config.get("access_token")
    if not access_token:
        print("Access token não encontrado na configuração.")
        return pl.DataFrame()

    start_date_fmt = datetime.strptime(data_inicio, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_fmt = datetime.strptime(data_fim, "%Y%m%d").strftime("%Y-%m-%d")

    levels = ["AUCTION_CAMPAIGN", "AUCTION_ADGROUP", "AUCTION_AD"]
    registros_unificados = []

    for level in levels:
        # --- 1. Coleta dados de performance ---
        print(f"\nColetando dados de performance para {level}...")
        resposta_performance = fetch_report_data(advertiser_id, level, start_date_fmt, end_date_fmt, access_token)
        if resposta_performance.get("code") != 0:
            print(f"Erro no Integrated Report para {level}: {resposta_performance.get('message')}")
            continue
        
        lista_performance = resposta_performance.get("data", {}).get("list", [])
        if not lista_performance:
            print(f"Nenhum dado de performance coletado para {level}.")
            continue

        print(f"{len(lista_performance)} registros de performance coletados para {level}.")
        
        registros_performance_transformados = []
        for rec in lista_performance:
            rec["nivel"] = level
            registros_performance_transformados.append(transformar_registro(rec, advertiser_id))
        
        df_performance = pl.DataFrame(registros_performance_transformados)

        # --- 2. Coleta dados de eventos customizados ---
        df_eventos = fetch_custom_event_data(advertiser_id, level, start_date_fmt, end_date_fmt, access_token)
        
        # --- 3. Une os dois DataFrames ---
        if df_eventos.height > 0:
            id_col_map = {
                "AUCTION_CAMPAIGN": "campaign_id", "AUCTION_ADGROUP": "adgroup_id", "AUCTION_AD": "ad_id"
            }
            id_col = id_col_map[level]
            
            df_eventos = df_eventos.rename({"id": id_col})

            df_nivel_combinado = df_performance.join(
                df_eventos,
                left_on=["data", id_col],
                right_on=["stat_time_day", id_col],
                how="left"
            )

            for col in df_eventos.columns:
                 if col.startswith("evento_") and col in df_nivel_combinado.columns:
                     df_nivel_combinado = df_nivel_combinado.with_columns(
                         pl.col(col).fill_null(0).alias(col)
                     )
        else:
            df_nivel_combinado = df_performance

        registros_unificados.append(df_nivel_combinado)

    if not registros_unificados:
        print("Nenhum dado coletado.")
        return pl.DataFrame()

    df_final = pl.concat(registros_unificados, how="diagonal")
    df_final = _replace_dot_in_column_names(df_final)
    produto = _produto_por_advertiser(advertiser_id)
    df_final = df_final.with_columns(pl.lit(produto).cast(pl.Utf8).alias("produto"))

    return _to_tiktok_event_long_format(df_final)


# =============================================================================
# 6. Função para download de assets 
# =============================================================================
def baixar_assets_tiktok(advertiser_id: str, config: dict) -> None:
    """Download TikTok assets for the provided advertiser."""
    headers = {"Access-Token": config["access_token"], "Content-Type": "application/json"}
    url_ads = f"{TIKTOK_API_BASE}/ad/get/"
    payload = {"advertiser_id": advertiser_id, "filtering": {"ad_status": ["ENABLE"]}, "page": 1, "page_size": 1000}
    try:
        resp = requests.post(url_ads, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data_resp = resp.json()
        lista_ads = data_resp.get("data", {}).get("list", [])
    except Exception as e:
        print(f"Erro ao consultar anúncios para assets no advertiser {advertiser_id}: {e}")
        lista_ads = []

    diretorio_saida = os.path.join(AD_ASSETS_PATH_TIKTOK, advertiser_id)
    if not os.path.exists(diretorio_saida):
        os.makedirs(diretorio_saida,exist_ok=True)

    total_baixados = 0
    for ad in lista_ads:
        ad_id = ad.get("ad_id")
        image_url = ad.get("image_url")
        video_url = ad.get("video_url")
        for asset_url in [image_url, video_url]:
            if not asset_url:
                continue
            try:
                resposta = requests.get(asset_url, timeout=30)
                resposta.raise_for_status()
            except Exception as e:
                print(f"Erro ao baixar asset para anúncio {ad_id}: {e}")
                continue
            tipo_conteudo = resposta.headers.get("Content-Type", "").lower()
            extensao = (mimetypes.guess_extension(tipo_conteudo) or ".bin").lstrip(".")
            nome_arquivo = f"{advertiser_id}_{ad_id}.{extensao}"
            caminho_arquivo = os.path.join(diretorio_saida, nome_arquivo)
            if os.path.exists(caminho_arquivo):
                print(f"Asset do anúncio {ad_id} já existe: {caminho_arquivo}")
                continue
            try:
                with open(caminho_arquivo, "wb") as f:
                    f.write(resposta.content)
                print(f"Asset salvo para anúncio {ad_id}: {caminho_arquivo}")
                total_baixados += 1
            except Exception as e:
                print(f"Erro ao salvar asset {caminho_arquivo}: {e}")
    if total_baixados == 0:
        print("Nenhum asset baixado.")
    else:
        print(f"Total de {total_baixados} assets baixados.")

# =============================================================================
# 7. Função principal para executar o fluxo completo
# =============================================================================
def executar_tiktok(
    data_inicio: str,
    data_fim: str,
    baixar_midias: bool = False,
    ids_clientes: list[str] | None = None,
) -> pl.DataFrame:
    """Run the TikTok extraction flow for performance, events and optional assets."""
    config = carregar_config_tiktok(CONFIG_TIKTOK_PATH)
    ids_clientes = resolve_selected_ids(ids_clientes, TIKTOK_ACCOUNTS)
    resultados = []

    for advertiser_id in ids_clientes:
        print(f"\n--- Processando advertiser {advertiser_id} ---")
        df_resultado = obter_dados_tiktok(advertiser_id, data_inicio, data_fim, config)
        if df_resultado.height > 0:
            resultados.append(df_resultado)
        print(f"Dados do TikTok para o advertiser {advertiser_id} processados. (DataFrame shape: {df_resultado.shape})")
        if baixar_midias:
            print(f"\nIniciando download de assets para o advertiser {advertiser_id}...")
            baixar_assets_tiktok(advertiser_id, config)

    if not resultados:
        print("Nenhum dado do TikTok para salvar.")
        return pl.DataFrame()

    df_final = align_and_concat_frames(resultados)
    inserir_substituir(df_final, TIKTOK_OUTPUT_FILE)
    return df_final

def main() -> None:
    """Execute a local default TikTok extraction window for manual runs."""
    baixar_midias_flag = False
    ids_especificos = []
    data_inicio = (datetime.today() - timedelta(days=180)).strftime("%Y%m%d")
    data_fim = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    executar_tiktok(
        data_inicio,
        data_fim,
        baixar_midias=baixar_midias_flag,
        ids_clientes=ids_especificos,
    )

if __name__ == "__main__":
    main()
