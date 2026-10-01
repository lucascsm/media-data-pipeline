import os
import shutil
from datetime import datetime, timedelta
import polars as pl
import time
import random

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
from src.utils_do_pai import OUTPUT_PATH, CONFIG_META_PATH, BACKUP_META_PATH
from src.account_catalog import get_product_for_account, list_accounts
from src.event_long_format import to_long_event_frame
from src.pipeline_utils import align_and_concat_frames, load_yaml_config, resolve_selected_ids

META_OUTPUT_FILE = os.path.join(str(OUTPUT_PATH), "meta_ads_data_all_new.parquet")
META_ACCOUNTS = list_accounts("meta")

META_BASE_COLUMNS = [
    "clicks",
    "data",
    "gasto",
    "id_anuncio",
    "id_campanha",
    "id_cliente",
    "id_conta",
    "id_grupo_anuncio",
    "impressoes",
    "alcance",
    "link_click",
    "nivel",
    "nome_anuncio",
    "nome_campanha",
    "nome_grupo_anuncio",
    "objetivo",
    "produto",
    "buying_type",
    "created_time",
    "custom_event_type",
    "optimization_goal",
    "start_time",
    "status_anuncio",
    "status_campanha",
    "status_grupo_anuncio",
    "stop_time",
]

META_BASE_DTYPES = {
    "clicks": pl.Int64,
    "gasto": pl.Float64,
    "impressoes": pl.Int64,
    "alcance": pl.Float64,
    "link_click": pl.Float64,
}


def _normalizar_id_conta_meta(conta_id: str) -> str:
    conta = str(conta_id or "").strip()
    return conta[4:] if conta.startswith("act_") else conta


def _produto_por_conta_meta(conta_id: str) -> str | None:
    return get_product_for_account(conta_id, "meta")


def _extrair_info_erro_meta(exc: FacebookRequestError) -> dict:
    try:
        body = exc.body()
    except Exception:
        body = None
    return {
        "http_status": exc.http_status() if hasattr(exc, "http_status") else None,
        "code": exc.api_error_code() if hasattr(exc, "api_error_code") else None,
        "subcode": exc.api_error_subcode() if hasattr(exc, "api_error_subcode") else None,
        "type": exc.api_error_type() if hasattr(exc, "api_error_type") else None,
        "message": exc.api_error_message() if hasattr(exc, "api_error_message") else None,
        "fbtrace_id": exc.fbtrace_id() if hasattr(exc, "fbtrace_id") else None,
        "body": body,
    }


def _erro_meta_retryable(info: dict) -> bool:
    error_body = info.get("body") or {}
    is_transient = error_body.get("error", {}).get("is_transient", False)
    retryable_codes = {1, 2, 4, 17, 32, 613, 80000, 80004, 80014, 80015, 80016}
    return is_transient or (info.get("code") in retryable_codes)

def _erro_meta_timeout(info: dict) -> bool:
    if info.get("subcode") == 1504018:
        return True
    error_body = info.get("body") or {}
    error_data = error_body.get("error") or {}
    user_title = str(error_data.get("error_user_title") or "").lower()
    user_msg = str(error_data.get("error_user_msg") or "").lower()
    return "timed out" in user_title or "timed out" in user_msg

def _erro_meta_payload_grande(info: dict) -> bool:
    msg = str(info.get("message") or "").lower()
    error_body = info.get("body") or {}
    error_data = error_body.get("error") or {}
    user_title = str(error_data.get("error_user_title") or "").lower()
    user_msg = str(error_data.get("error_user_msg") or "").lower()
    return (
        "reduce the amount of data" in msg
        or "reduce the amount of data" in user_msg
        or "reduce the amount of data" in user_title
        or "try a smaller date range" in user_msg
        or "fetch less data" in user_msg
        or "use async jobs" in user_msg
    )


def executar_com_retry(
    api_call,
    max_retries=6,
    initial_delay=60,
    max_delay=900,
    allow_timeout=False,
    allow_payload_split=False,
):
    """
    Executa chamada da API do Meta com retry para erros transientes.

    :param api_call: Funcao lambda/partial que representa a chamada de API.
    :param max_retries: Numero maximo de tentativas.
    :param initial_delay: Tempo de espera inicial em segundos. Dobra a cada tentativa.
    :return: Resultado da chamada de API bem-sucedida.
    :raises Exception: Se a chamada falhar apos todas as tentativas.
    :param allow_timeout: Se True, erros de timeout sao repassados para o caller tratar.
    :param allow_payload_split: Se True, erros de payload alto sao repassados para o caller tratar.
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return list(api_call())
        except FacebookRequestError as e:
            info = _extrair_info_erro_meta(e)
            if allow_timeout and _erro_meta_timeout(info):
                raise
            if allow_payload_split and _erro_meta_payload_grande(info):
                raise
            if _erro_meta_retryable(info):
                msg = info.get("message") or "erro transiente"
                if info.get("code") == 80004 or info.get("subcode") == 2446079:
                    delay = max(delay, 300)
                sleep_base = min(delay, max_delay)
                jitter = random.randint(0, 30)
                sleep_seconds = sleep_base + jitter
                print(
                    f"Erro transiente da API do Meta (tentativa {attempt + 1}/{max_retries}): {msg}. "
                    f"Detalhes: {info}. Aguardando {sleep_seconds} segundos antes de tentar novamente."
                )
                time.sleep(sleep_seconds)
                delay = min(delay * 2, max_delay)
            else:
                print(f"Erro nao tratavel da API do Meta. Detalhes: {info}")
                raise

    raise Exception(f"A chamada a API do Meta falhou apos {max_retries} tentativas.")


def _iterar_blocos_datas(data_inicio: str, data_fim: str, intervalo_dias: int):
    if intervalo_dias < 1:
        intervalo_dias = 1
    dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
    dt_fim = datetime.strptime(data_fim, "%Y-%m-%d")
    current_start = dt_inicio
    while current_start <= dt_fim:
        current_end = current_start + timedelta(days=intervalo_dias - 1)
        if current_end > dt_fim:
            current_end = dt_fim
        yield current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
        current_start = current_end + timedelta(days=1)


def _obter_insights_com_fallback(conta, fields, params, level, chunk_days=3):
    time_range = params.get("time_range") or {}
    since = time_range.get("since")
    until = time_range.get("until")

    def _call_insights(range_since, range_until, use_light=False):
        local_params = dict(params)
        local_params["time_range"] = {"since": range_since, "until": range_until}
        if use_light:
            local_params = dict(local_params)
            local_params.pop("action_breakdowns", None)
            if "limit" in local_params:
                try:
                    local_params["limit"] = min(int(local_params["limit"]), 500)
                except Exception:
                    local_params["limit"] = 500
        return executar_com_retry(
            lambda: conta.get_insights(
                fields=fields,
                params=dict(local_params, level=level)
            ),
            allow_timeout=True,
            allow_payload_split=True,
        )

    try:
        if since and until:
            return _call_insights(since, until)
        return executar_com_retry(
            lambda: conta.get_insights(fields=fields, params=dict(params, level=level)),
            allow_timeout=True,
            allow_payload_split=True,
        )
    except FacebookRequestError as e:
        info = _extrair_info_erro_meta(e)
        is_timeout = _erro_meta_timeout(info)
        is_payload = _erro_meta_payload_grande(info)
        if not (since and until) or not (is_timeout or is_payload):
            raise

        chunk_days = max(1, chunk_days)
        reason = "timeout" if is_timeout else "payload alto"
        print(
            f"Erro da API do Meta ({reason}) para {level}. Dividindo {since} a {until} em blocos de {chunk_days} dias."
        )
        resultados = []
        for sub_inicio, sub_fim in _iterar_blocos_datas(since, until, chunk_days):
            try:
                resultados.extend(_call_insights(sub_inicio, sub_fim))
            except FacebookRequestError as sub_e:
                sub_info = _extrair_info_erro_meta(sub_e)
                if (_erro_meta_timeout(sub_info) or _erro_meta_payload_grande(sub_info)) and chunk_days > 1:
                    print(
                        f"Erro persistente para {level} em {sub_inicio} a {sub_fim}. "
                        "Reduzindo para blocos de 1 dia."
                    )
                    for dia_inicio, dia_fim in _iterar_blocos_datas(sub_inicio, sub_fim, 1):
                        try:
                            resultados.extend(_call_insights(dia_inicio, dia_fim))
                        except FacebookRequestError as day_e:
                            day_info = _extrair_info_erro_meta(day_e)
                            if _erro_meta_timeout(day_info) or _erro_meta_payload_grande(day_info):
                                print(
                                    f"Erro persistente para {level} em {dia_inicio} a {dia_fim}. "
                                    "Tentando chamada mais leve (sem action_breakdowns)."
                                )
                                resultados.extend(_call_insights(dia_inicio, dia_fim, use_light=True))
                            else:
                                raise
                else:
                    if _erro_meta_timeout(sub_info) or _erro_meta_payload_grande(sub_info):
                        print(
                            f"Erro persistente para {level} em {sub_inicio} a {sub_fim}. "
                            "Tentando chamada mais leve (sem action_breakdowns)."
                        )
                        resultados.extend(_call_insights(sub_inicio, sub_fim, use_light=True))
                    else:
                        raise
        return resultados


def inserir_substituir(df_novo: pl.DataFrame, caminho_arquivo: str) -> None:
    """Merge the new Meta extract into the parquet target using stable keys."""
    if df_novo is None:
        print("Aviso: df_novo is None; skipping insert/overwrite.")
        return
    if not df_novo.columns or df_novo.height == 0:
        print("Aviso: df_novo vazio (sem linhas/colunas); mantendo arquivo existente.")
        return
    if "nivel" not in df_novo.columns:
        print("Aviso: df_novo sem coluna 'nivel'; mantendo arquivo existente.")
        return

    def adicionar_chave_unica(df: pl.DataFrame) -> pl.DataFrame:
        id_conta_expr = (
            pl.col("id_cliente").cast(pl.Utf8)
            if "id_cliente" in df.columns
            else (
                pl.col("id_conta").cast(pl.Utf8)
                if "id_conta" in df.columns
                else pl.lit("sem_id_conta")
            )
        )
        evento_expr = (
            pl.when(pl.col("evento").cast(pl.Utf8).fill_null("") == "")
              .then(pl.lit(""))
              .otherwise(pl.concat_str([pl.lit("_"), pl.col("evento").cast(pl.Utf8)]))
            if "evento" in df.columns
            else pl.lit("")
        )
        return df.with_columns([
            pl.when(pl.col("nivel") == "campanha")
              .then(pl.concat_str([
                  pl.lit("campanha_"),
                  id_conta_expr,
                  pl.lit("_"),
                  pl.col("id_campanha").cast(pl.Utf8),
                  pl.lit("_"),
                  pl.col("data"),
                  evento_expr,
              ]))
              .when(pl.col("nivel") == "grupo_anuncio")
              .then(pl.concat_str([
                  pl.lit("grupo_anuncio_"),
                  id_conta_expr,
                  pl.lit("_"),
                  pl.col("id_grupo_anuncio").cast(pl.Utf8),
                  pl.lit("_"),
                  pl.col("data"),
                  evento_expr,
              ]))
              .when(pl.col("nivel") == "anuncio")
              .then(pl.concat_str([
                  pl.lit("anuncio_"),
                  id_conta_expr,
                  pl.lit("_"),
                  pl.col("id_anuncio").cast(pl.Utf8),
                  pl.lit("_"),
                  pl.col("data"),
                  evento_expr,
              ]))
              .otherwise(pl.lit("desconhecido"))
              .alias("chave_unica")
        ])
    
    # Calcula a chave única para o novo DataFrame
    df_novo = adicionar_chave_unica(df_novo)
    
    if os.path.exists(caminho_arquivo):
        # Realiza backup do arquivo existente
        agora = datetime.now()
        if not os.path.exists(BACKUP_META_PATH):
            os.makedirs(BACKUP_META_PATH,exist_ok=True)
        for arquivo in os.listdir(BACKUP_META_PATH):
            caminho_backup = os.path.join(BACKUP_META_PATH, arquivo)
            if os.path.isfile(caminho_backup):
                mtime = datetime.fromtimestamp(os.path.getmtime(caminho_backup))
                if (agora - mtime) > timedelta(days=5):
                    try:
                        os.remove(caminho_backup)
                    except Exception as e:
                        print(f"Erro ao remover backup {caminho_backup}: {e}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_base, extensao = os.path.splitext(os.path.basename(caminho_arquivo))
        nome_backup = f"{nome_base}_{timestamp}{extensao}"
        caminho_backup = os.path.join(BACKUP_META_PATH, nome_backup)
        try:
            shutil.copy(caminho_arquivo, caminho_backup)
            print(f"Backup realizado: {caminho_backup}")
        except Exception as e:
            print(f"Erro ao realizar backup do arquivo {caminho_arquivo}: {e}")
        
        # Carrega o DataFrame antigo e calcula sua chave única
        df_antigo = pl.read_parquet(caminho_arquivo)
        df_antigo = adicionar_chave_unica(df_antigo)
        
        # Filtra os registros do antigo cujas chaves não estão presentes no novo
        chaves_novas = df_novo.select("chave_unica").to_series().to_list()
        df_antigo_filtrado = df_antigo.filter(~pl.col("chave_unica").is_in(chaves_novas))
        
        # Alinha os esquemas
        colunas_antigo = set(df_antigo_filtrado.columns)
        colunas_novo = set(df_novo.columns)
        all_cols = list(colunas_antigo.union(colunas_novo))
        
        for col in all_cols:
            if col not in df_antigo_filtrado.columns:
                df_antigo_filtrado = df_antigo_filtrado.with_columns([pl.lit(None).cast(pl.Utf8).alias(col)])
            if col not in df_novo.columns:
                df_novo = df_novo.with_columns([pl.lit(None).cast(pl.Utf8).alias(col)])
            
            dtype_antigo = df_antigo_filtrado.schema[col]
            dtype_novo = df_novo.schema[col]
            if dtype_antigo != dtype_novo:
                df_antigo_filtrado = df_antigo_filtrado.with_columns([pl.col(col).cast(pl.Utf8).alias(col)])
                df_novo = df_novo.with_columns([pl.col(col).cast(pl.Utf8).alias(col)])
        
        ordered_cols = sorted(all_cols)
        df_antigo_filtrado = df_antigo_filtrado.select(ordered_cols)
        df_novo = df_novo.select(ordered_cols)
        
        df_combinado = pl.concat([df_antigo_filtrado, df_novo])
        df_combinado = df_combinado.unique(subset="chave_unica")
    else:
        df_combinado = df_novo

    # Remove a coluna auxiliar e salva
    df_combinado = df_combinado.drop("chave_unica")
    if "frequencia" in df_combinado.columns:
        df_combinado = df_combinado.drop("frequencia")
    df_combinado.write_parquet(caminho_arquivo, compression="snappy")
    print(f"Dados salvos em {caminho_arquivo}")

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


def _to_meta_event_long_format(df: pl.DataFrame) -> pl.DataFrame:
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

    for column, dtype in META_BASE_DTYPES.items():
        if column in df.columns:
            df = df.with_columns(pl.col(column).cast(dtype, strict=False).alias(column))

    for column in META_BASE_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    ordered_base_columns = [column for column in META_BASE_COLUMNS if column in df.columns]
    return to_long_event_frame(df, ordered_base_columns)


def get_all_action_sums(actions, conta_id=None):
    """
    Extrai conversões de actions usando o campo 'value' que já contém
    a soma de todas as janelas de atribuição (7d_click + 1d_click + 1d_view)
    conforme mostrado na plataforma Meta Ads.
    """
    sums = {}
    if not actions: return sums
    
    for act in actions:
        atype = act.get("action_type")
        if not atype: continue
        
        # Usa 'value' que já contém a soma agregada de todas as janelas
        # Conforme a plataforma Meta Ads:
        # - Clique de 7 dias
        # - Clique de 1 dia
        # - Clique de 7 dias OU visualização de 1 dia
        val = int(act.get("value", 0))
        
        col_name = atype.replace(".", "_").replace("-", "_").lower()
        sums[col_name] = sums.get(col_name, 0) + val
    return sums

def get_all_conversion_sums(conversions, conta_id=None):
    """
    Extrai conversões de conversions usando o campo 'value' que já contém
    a soma de todas as janelas de atribuição (7d_click + 1d_click + 1d_view)
    conforme mostrado na plataforma Meta Ads.
    """
    sums = {}
    if not conversions: return sums
    
    for conv in conversions:
        atype = conv.get("action_type")
        if not atype: continue
        
        # Usa 'value' que já contém a soma agregada de todas as janelas
        # Conforme a plataforma Meta Ads:
        # - Clique de 7 dias
        # - Clique de 1 dia  
        # - Clique de 7 dias OU visualização de 1 dia
        val = int(conv.get("value", 0))
        
        col_name = atype.replace(".", "_").replace("-", "_").lower()
        sums[col_name] = sums.get(col_name, 0) + val
    return sums

def obter_dados_meta_ads(conta_id: str, data_inicio: str, data_fim: str) -> pl.DataFrame:
    """Collect Meta performance and conversion events for the requested account window."""
    conta = AdAccount(conta_id)
    registros = []
    
    params_insights = {
        'time_range': {'since': data_inicio, 'until': data_fim},
        'time_increment': 1,
        'action_attribution_windows': ['7d_click', '1d_view'],
        'action_breakdowns': ['action_type'],
        'limit': 1000,
    }
    
    # --- CAMPANHAS ---
    print(f"Obtendo campanhas da conta {conta_id}...")
    fields_campanhas = [
        Campaign.Field.id, Campaign.Field.name, Campaign.Field.status,
        Campaign.Field.objective, Campaign.Field.buying_type,
        Campaign.Field.start_time, Campaign.Field.stop_time
    ]
    campanhas = executar_com_retry(
        lambda: conta.get_campaigns(fields=fields_campanhas, params={'limit': 5000})
    )
    camp_dict = {camp.get('id'): camp for camp in campanhas}

    print(f"Obtendo insights diários das campanhas...")
    insights_campanhas = _obter_insights_com_fallback(
        conta,
        fields=['campaign_id', 'impressions', 'reach', 'clicks', 'spend', 'date_start', 'actions', 'conversions'],
        params=params_insights,
        level='campaign',
    )
    for insight in insights_campanhas:
        camp_id = insight.get('campaign_id')
        camp = camp_dict.get(camp_id, {})
        action_sums = get_all_action_sums(insight.get('actions', []), conta_id)
        conv_sums = get_all_conversion_sums(insight.get('conversions', []), conta_id)
        record = {
            "data": insight.get('date_start'), "nivel": "campanha", "id_conta": conta_id,
            "id_campanha": camp_id, "nome_campanha": camp.get('name'), "status_campanha": camp.get('status'),
            "objetivo": camp.get('objective'), "buying_type": camp.get('buying_type'),
            "start_time": camp.get('start_time'), "stop_time": camp.get('stop_time'),
            "impressoes": insight.get('impressions'), "alcance": insight.get('reach'),
            "clicks": insight.get('clicks'), "gasto": insight.get('spend')
        }
        record.update(action_sums)
        record.update(conv_sums)
        registros.append(record)
    
    # --- CONJUNTOS DE ANUNCIOS ---
    print(f"Obtendo conjuntos de anúncios (ad sets)...")
    fields_adsets = [
        AdSet.Field.id, AdSet.Field.name, AdSet.Field.status, AdSet.Field.daily_budget,
        AdSet.Field.start_time, AdSet.Field.end_time, AdSet.Field.campaign_id,
        AdSet.Field.promoted_object, AdSet.Field.optimization_goal
    ]
    adsets = executar_com_retry(
        lambda: conta.get_ad_sets(fields=fields_adsets, params={'limit': 5000})
    )
    
    adset_dict = {}
    for adset in adsets:
        promoted_obj = adset.get('promoted_object', {})
        custom_event = promoted_obj.get('custom_event_type') or promoted_obj.get('pixel_event')
        adset_dict[adset.get('id')] = {
            'campaign_id': adset.get('campaign_id'), 'name': adset.get('name'), 'status': adset.get('status'),
            'start_time': adset.get('start_time'), 'end_time': adset.get('end_time'),
            'custom_event_type': custom_event, 'optimization_goal': adset.get('optimization_goal')
        }

    print(f"Obtendo insights diários dos ad sets...")
    insights_adsets = _obter_insights_com_fallback(
        conta,
        fields=['adset_id', 'impressions', 'reach', 'clicks', 'spend', 'date_start', 'actions', 'conversions'],
        params=params_insights,
        level='adset',
    )
    for insight in insights_adsets:
        adset_id = insight.get('adset_id')
        adset_info = adset_dict.get(adset_id, {})
        action_sums = get_all_action_sums(insight.get('actions', []), conta_id)
        conv_sums = get_all_conversion_sums(insight.get('conversions', []), conta_id)
        record = {
            "data": insight.get('date_start'), "nivel": "grupo_anuncio", "id_conta": conta_id,
            "id_campanha": adset_info.get('campaign_id'), "id_grupo_anuncio": adset_id,
            "nome_grupo_anuncio": adset_info.get('name'), "status_grupo_anuncio": adset_info.get('status'),
            "impressoes": insight.get('impressions'), "alcance": insight.get('reach'),
            "clicks": insight.get('clicks'), "gasto": insight.get('spend'),
            "start_time": adset_info.get('start_time'), "stop_time": adset_info.get('end_time'),
            "custom_event_type": adset_info.get('custom_event_type'),
            "optimization_goal": adset_info.get('optimization_goal')
        }
        record.update(action_sums)
        record.update(conv_sums)
        registros.append(record)

    # --- ANUNCIOS ---
    print(f"Obtendo anúncios...")
    fields_anuncios = [
        Ad.Field.id, Ad.Field.name, Ad.Field.status, Ad.Field.created_time,
        Ad.Field.adset_id, Ad.Field.campaign_id
    ]
    anuncios = executar_com_retry(
        lambda: conta.get_ads(fields=fields_anuncios, params={'limit': 5000})
    )
    ad_dict = {ad.get('id'): ad for ad in anuncios}

    print(f"Obtendo insights diários dos anúncios...")
    insights_ads = _obter_insights_com_fallback(
        conta,
        fields=['ad_id', 'impressions', 'reach', 'clicks', 'spend', 'date_start', 'actions', 'conversions'],
        params=params_insights,
        level='ad',
    )
    for insight in insights_ads:
        ad_id = insight.get('ad_id')
        ad = ad_dict.get(ad_id, {})
        adset_id = ad.get('adset_id')
        adset_info = adset_dict.get(adset_id, {})
        action_sums = get_all_action_sums(insight.get('actions', []), conta_id)
        conv_sums = get_all_conversion_sums(insight.get('conversions', []), conta_id)
        record = {
            "data": insight.get('date_start'), "nivel": "anuncio", "id_conta": conta_id,
            "id_campanha": ad.get('campaign_id'), "id_grupo_anuncio": adset_id, "id_anuncio": ad_id,
            "nome_anuncio": ad.get('name'), "status_anuncio": ad.get('status'),
            "impressoes": insight.get('impressions'), "alcance": insight.get('reach'),
            "clicks": insight.get('clicks'), "gasto": insight.get('spend'),
            "created_time": ad.get('created_time'),
            "custom_event_type": adset_info.get('custom_event_type'),
            "optimization_goal": adset_info.get('optimization_goal')
        }
        record.update(action_sums)
        record.update(conv_sums)
        registros.append(record)

    if not registros:
        print(f"Aviso: Nenhum registro foi retornado para a conta {conta_id} no período especificado.")
        return pl.DataFrame()

    df = pl.DataFrame(registros)
    
    expected_columns = [
        "data", "nivel", "id_conta", "id_campanha", "nome_campanha", "status_campanha",
        "objetivo", "buying_type", "start_time", "stop_time", "impressoes", "alcance", "clicks", "gasto",
        "id_grupo_anuncio", "nome_grupo_anuncio", "status_grupo_anuncio", "id_anuncio",
        "nome_anuncio", "status_anuncio", "created_time", "custom_event_type", "optimization_goal"
    ]
    for col in expected_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = _replace_dot_in_column_names(df)

    id_cliente = _normalizar_id_conta_meta(conta_id)
    produto = _produto_por_conta_meta(conta_id)
    df = df.with_columns([
        pl.lit(id_cliente).cast(pl.Utf8).alias("id_cliente"),
        pl.lit(produto).cast(pl.Utf8).alias("produto"),
    ])

    return _to_meta_event_long_format(df)

def executar_meta_ads(
    data_inicio: str,
    data_fim: str,
    baixar_midias: bool = True,
    contas: list[str] | None = None,
):
    """Run the Meta extraction flow for the requested accounts and interval."""
    contas = resolve_selected_ids(contas, META_ACCOUNTS)
    
    resultados = []
    for conta_id in contas:
        print(f"\n--- Processando conta {conta_id} ---")
        df = obter_dados_meta_ads(conta_id, data_inicio, data_fim)
        if df.height > 0:
            resultados.append(df)
        print(f"Dados do Meta Ads para a conta {conta_id} processados. (DataFrame shape: {df.shape})")
        
        if baixar_midias:
            print("[Meta] Download de assets nao faz parte do fluxo atual.")
    df_final = align_and_concat_frames(resultados)
    if df_final.height > 0:
        inserir_substituir(df_final, META_OUTPUT_FILE)
    else:
        print("Nenhum dado do Meta para salvar.")
    return df_final

def main():
    """Execute a local default Meta extraction window for manual runs."""
    baixar_midias_flag = False
    contas_especificas = []
    segredos = load_yaml_config(CONFIG_META_PATH / "meta.yaml")

    access_token = segredos.get("access_token")
    app_id = segredos.get("app_id")
    app_secret = segredos.get("app_secret")
    
    FacebookAdsApi.init(app_id=app_id, app_secret=app_secret, access_token=access_token)
    
    data_inicio = (datetime.today() - timedelta(days=180)).strftime("%Y-%m-%d")
    data_fim = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    df_meta = executar_meta_ads(
        data_inicio,
        data_fim,
        baixar_midias=baixar_midias_flag,
        contas=contas_especificas,
    )
    print("Processamento finalizado.")
    print(df_meta.head())

if __name__ == "__main__":
    main()
