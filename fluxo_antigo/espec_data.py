from src.legacy_config import legacy_rule
from pathlib import Path
import os
import re
import polars as pl
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime, timedelta
from functools import reduce
import operator

# Importa funções do relatório semanal

# ----------------------------------------------------------------------
# CONFIGURAÇÕES GERAIS - Caminhos relativos
# ----------------------------------------------------------------------

from src.utils_do_pai import GCS_MOUNT
OUTPUT_PATH = GCS_MOUNT / "output"
SOT_OUTPUT_FILE = OUTPUT_PATH / "sot_mesa_performance.csv"
DATA_DIR = GCS_MOUNT
#
# """
# Para rodar local:
# GCS_MOUNT = Path(__file__).parent.resolve()
# OUTPUT_PATH = Path("/path/to/project")
# SOT_OUTPUT_FILE = Path("/path/to/project")
# DATA_DIR = Path("/path/to/project")
#  """


# Arquivos do Google
from src.account_catalog import ACCOUNT_CATALOG, list_accounts, normalize_account_id
GOOGLE_FILES = [OUTPUT_PATH / f"dados_google_ads_{value}.parquet" for value in list_accounts("google")]
TIKTOK_FILES = [OUTPUT_PATH / f"dados_tiktok_{value}.parquet" for value in list_accounts("tiktok")]
META_FILES = [OUTPUT_PATH / f"meta_ads_data_{value}.parquet" for value in list_accounts("meta")]
MAPPING_PRODUTO = {normalize_account_id(row["account_id"]): row["product"] for row in ACCOUNT_CATALOG}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def ensure_float_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """
    Guaranteed that every name in ``columns`` exists in ``df`` as a Float64 series.
    Missing columns are created with zeros to avoid runtime errors when chaining
    expressions that assume their presence.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        df = df.with_columns(
            [pl.lit(0).cast(pl.Float64).alias(col) for col in missing]
        )
    return df

# -----------------------------------------------------------------------------
# FUNÇÃO AUXILIAR PARA NORMALIZAR O ID DO CLIENTE
# -----------------------------------------------------------------------------
def normalize_id_cliente(id_cliente) -> str:
    id_str = str(id_cliente).strip()
    if id_str.startswith("act_"):
        return id_str[4:]
    return id_str

# ----------------------------------------------------------------------------
# FUNÇÃO AUXILIAR PARA EXTRAIR O ID DA CONTA A PARTIR DO NOME DO ARQUIVO
# Exemplo: dados_google_ads_0000000000.parquet => "0000000000"
# ----------------------------------------------------------------------------
def get_file_id(file_path: Path) -> str:
    match = re.search(r'(\d+)\.parquet$', file_path.name)
    if match:
        return match.group(1)
    return ""

# ----------------------------------------------------------------------------
# FUNÇÃO PARA EXTRAIR A CREATIVE LINE COMPLETA
# ----------------------------------------------------------------------------
def extract_creative_line_completo(nome_original: str) -> str:
    patterns = [
        r'(concorrência & produto[^\|]*)',
        r'(produto & marca[^\|]*)',
        r'(produto genérico[^\|]*)',
        r'(financiamento imobil[^\|]*)',
        r'(egi[^\|]*)'
    ]
    if nome_original is None:
        return ""
    if not isinstance(nome_original, str):
        nome_original = str(nome_original)
    nome_original_lower = nome_original.lower()
    for pat in patterns:
        match = re.search(pat, nome_original_lower)
        if match:
            start = match.start()
            pipe_index = nome_original.find("|", start)
            if pipe_index == -1:
                return nome_original[start:].strip()
            else:
                return nome_original[start:pipe_index].strip()
    return ""

# ----------------------------------------------------------------------------
# FUNÇÃO PARA DETECÇÃO DO TIPO DE CAMPANHA
# ----------------------------------------------------------------------------
def get_campaign_type(nome_lower: str, veiculo: str) -> str:
    if "search" in nome_lower:
        return "Google Search"
    veiculo_lower = (veiculo or "").lower()
    if veiculo_lower == "google":
        return "Google App Campaign"
    elif veiculo_lower == "meta":
        return "Meta App Campaign"
    elif veiculo_lower == "tiktok":
        return "TikTok App Campaign"
    else:
        return "App Campaign"

# ----------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL DE RENOMEAR CAMPANHA (NOVA LÓGICA)
# Utiliza o produto mapeado para ter precedência sobre o nome original.
# ----------------------------------------------------------------------------
def renomear_campanha(nome: str, mapped_product: str, veiculo: str) -> str:
    nome = "" if nome is None else str(nome)
    mapped_product = "" if mapped_product is None else str(mapped_product)
    veiculo = "" if veiculo is None else str(veiculo)

    nome_lower = nome.lower()
    produto_lower = mapped_product.lower()  # produto mapeado tem precedência

    campaign_type = get_campaign_type(nome_lower, veiculo)

    # Caso seja Produto B
    if legacy_rule("LABEL_OR_MATCH_01") in produto_lower or legacy_rule("LABEL_OR_MATCH_02") in produto_lower:
        if legacy_rule("LABEL_OR_MATCH_03") in nome_lower:
            subproduto = "Auto"
        elif legacy_rule("LABEL_OR_MATCH_04") in nome_lower:
            subproduto = "Imóveis"
        elif "tela_escolher_segmento" in nome_lower:
            subproduto = legacy_rule("LABEL_OR_MATCH_05")
        else:
            subproduto = "Geral"
        creative_line = extract_creative_line_completo(nome)
        result = f"Produto B | {subproduto} - {campaign_type}"
        if campaign_type == "Google Search" and creative_line:
            result += f" ({creative_line})"
        return result

    # Caso seja Produto A (aqui foi alterado para "Categoria A" conforme exemplo)
    elif legacy_rule("LABEL_OR_MATCH_06") in produto_lower:
        subproduto = "Geral"
        if legacy_rule("LABEL_OR_MATCH_07") in nome_lower:
            if "lal" in nome_lower:
                creative_line = "LAL"
            elif "pangle" in nome_lower:
                creative_line = "Pangle"
            elif "lista" in nome_lower:
                creative_line = "Lista, App e Site"
            else:
                creative_line = ""
        else:
            creative_line = extract_creative_line_completo(nome)
        result = f"Categoria A | {subproduto} - {campaign_type}"
        if campaign_type == "Google Search" and creative_line:
            result += f" ({creative_line})"
        elif campaign_type == "TikTok App Campaign" and legacy_rule("LABEL_OR_MATCH_07") in nome_lower:
            if creative_line:
                result += f" ({creative_line})"
        return result

    # Caso seja Produto C
    elif legacy_rule("LABEL_OR_MATCH_08") in produto_lower or legacy_rule("LABEL_OR_MATCH_09") in produto_lower:
        if legacy_rule("LABEL_OR_MATCH_10") in nome_lower:
            subproduto = "Automático"
        elif legacy_rule("LABEL_OR_MATCH_11") in nome_lower:
            subproduto = "Categoria C"
        elif ("renovacao" in nome_lower) or ("renovação" in nome_lower) or ("renovac" in nome_lower):
            subproduto = "Renovação"
        elif ("financiamento imobil" in nome_lower) or ("egi" in nome_lower):
            subproduto = "Categoria B"
        else:
            subproduto = "Geral"
        creative_line = extract_creative_line_completo(nome)
        result = f"Produto C PF e PJ | {subproduto} - {campaign_type}"
        if campaign_type == "Google Search" and creative_line:
            result += f" ({creative_line})"
        return result

    return nome

# -----------------------------------------------------------------------------
# FUNÇÕES DE TRANSFORMAÇÃO DE ESQUEMA
# -----------------------------------------------------------------------------
def rename_common(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {}
    if "id_conta" in df.columns and "id_cliente" not in df.columns:
        rename_map["id_conta"] = "id_cliente"
    if "spend" in df.columns and "custo" not in df.columns:
        rename_map["spend"] = "custo"
    if "gasto" in df.columns and "custo" not in df.columns:
        rename_map["gasto"] = "custo"
    if "budget" in df.columns and "orcamento" not in df.columns:
        rename_map["budget"] = "orcamento"
    if "conversion" in df.columns and "conversoes" not in df.columns:
        rename_map["conversion"] = "conversoes"
    if "clicks" in df.columns and "cliques" not in df.columns:
        rename_map["clicks"] = "cliques"
    if "impressions" in df.columns and "impressoes" not in df.columns:
        rename_map["impressions"] = "impressoes"
    if "date" in df.columns and "data" not in df.columns:
        rename_map["date"] = "data"
    if rename_map:
        df = df.rename(rename_map)
    return df

def unify_schema(dfs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    if not dfs:
        return []
    all_cols = set()
    for df in dfs:
        all_cols.update(df.columns)
    all_cols = list(all_cols)
    union_types = {}
    for df in dfs:
        for col, dtype in df.schema.items():
            if col not in union_types:
                union_types[col] = dtype
            else:
                if union_types[col] != dtype:
                    union_types[col] = pl.Utf8
    new_dfs = []
    for df in dfs:
        missing_cols = [c for c in all_cols if c not in df.columns]
        for c in missing_cols:
            df = df.with_columns(pl.lit(None).cast(union_types[c]).alias(c))
        for c in df.columns:
            df = df.with_columns(pl.col(c).cast(union_types[c]))
        df = df.select(all_cols)
        new_dfs.append(df)
    return new_dfs

# -----------------------------------------------------------------------------
# FUNÇÃO DE CARREGAMENTO DOS DADOS CONSOLIDADOS
# -----------------------------------------------------------------------------
def load_data_consolidado() -> pl.DataFrame:
    df_list = []
    # Processamento para arquivos do Google
    for f in GOOGLE_FILES:
        if not f.exists():
            continue
        df_g = pl.read_parquet(f)
        
        # Verificar se é um arquivo válido (deve ter nome_campanha)
        if "nome_campanha" not in df_g.columns and "campaign_name" not in df_g.columns:
            continue
            
        if "nivel" not in df_g.columns:
            df_g = df_g.with_columns(pl.lit("campanha").alias("nivel"))
        df_g = df_g.filter(pl.col("nivel").is_in(["campanha", "grupo_anuncio", "anuncio"]))
        df_g = df_g.with_columns(pl.lit("Google").alias("veiculo"))
        filename = os.path.basename(f)
        account_id = filename.split("_")[-1].replace(".parquet", "")
        df_g = df_g.with_columns(pl.lit(account_id).alias("id_cliente"))
        df_g = rename_common(df_g)
        df_list.append(df_g)
        
    # Processamento para arquivos do TikTok
    for f in TIKTOK_FILES:
        if not f.exists():
            continue
        df_t = pl.read_parquet(f)
        
        # Verificar se é um arquivo válido (deve ter nome_campanha)
        if "nome_campanha" not in df_t.columns and "campaign_name" not in df_t.columns:
            continue
            
        if "nivel" not in df_t.columns:
            df_t = df_t.with_columns(pl.lit("campanha").alias("nivel"))
        df_t = df_t.filter(pl.col("nivel").is_in(["campanha", "grupo_anuncio", "anuncio"]))
        df_t = df_t.with_columns(pl.lit("TikTok").alias("veiculo"))
        df_t = rename_common(df_t)
        df_list.append(df_t)
        
    # Processamento para arquivos do Meta
    for f in META_FILES:
        if not f.exists():
            continue
        df_m = pl.read_parquet(f)
        
        # Verificar se é um arquivo válido (deve ter nome_campanha)
        if "nome_campanha" not in df_m.columns and "campaign_name" not in df_m.columns:
            continue
            
        if "nivel" not in df_m.columns:
            df_m = df_m.with_columns(pl.lit("campanha").alias("nivel"))
        df_m = df_m.filter(pl.col("nivel").is_in(["campanha", "grupo_anuncio", "anuncio"]))
        df_m = df_m.with_columns(pl.lit("Meta").alias("veiculo"))
        df_m = rename_common(df_m)
        # Para Meta, utiliza a coluna "link_click" como cliques, se existir
        if "link_click" in df_m.columns:
            df_m = df_m.with_columns(pl.col("link_click").alias("cliques"))
        df_list.append(df_m)
    if not df_list:
        raise Exception("Nenhum arquivo de dados válido encontrado para processar espec_data")
    
    df_list = unify_schema(df_list)
    df_all = pl.concat(df_list, how="vertical").unique()
    if "data" in df_all.columns:
        df_all = df_all.with_columns(
            pl.col("data").str.slice(0, 10).alias("data")
        )
        df_all = df_all.with_columns(
            pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias("data")
        )
    if "id_cliente" in df_all.columns:
        normalized_ids = [normalize_id_cliente(x) for x in df_all["id_cliente"].to_list()]
        produtos_list = [MAPPING_PRODUTO.get(norm_id, legacy_rule("LABEL_OR_MATCH_12")) for norm_id in normalized_ids]
        df_all = df_all.with_columns(pl.Series("Produto", produtos_list))
    else:
        df_all = df_all.with_columns(pl.lit(legacy_rule("LABEL_OR_MATCH_12")).alias("Produto"))
    return df_all

# -----------------------------------------------------------------------------
# FUNÇÃO PARA AJUSTAR AS CONVERSÕES DO GOOGLE ADS (LÓGICA _PERS)
# -----------------------------------------------------------------------------
def process_google_conversoes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica regras de conversão para Google Ads baseado no produto:
    - Produto A e Produto C: conversoes + conversao_view
    - Outros produtos (Produto B, Produto D, Produto E): todas_conversoes - conversao_view
    """
    df = df.with_columns(
        pl.when(pl.col("veiculo") == "Google")
          .then(
              pl.when(pl.col("Produto").is_in([legacy_rule("LABEL_OR_MATCH_12"), legacy_rule("LABEL_OR_MATCH_13")]))
                .then(
                    pl.col("conversoes_pers").fill_null(0).cast(pl.Float64) +
                    pl.col("conversao_view_pers").fill_null(0).cast(pl.Float64)
                )
                .otherwise(
                    pl.col("todas_conversoes_pers").fill_null(0).cast(pl.Float64) -
                    pl.col("conversao_view_pers").fill_null(0).cast(pl.Float64)
                )
          )
          .otherwise(pl.col("conversoes"))
          .alias("conversoes")
    )
    return df

# -----------------------------------------------------------------------------
# NOVA FUNÇÃO: PROCESSA AS CONVERSÕES _CROSS PARA SOT (AGORA INCLUINDO META)
# -----------------------------------------------------------------------------
def process_conversoes_cross(df: pl.DataFrame) -> pl.DataFrame:
    """
    Para o relatório SOT:
      - Se o veículo for Google e o Produto for "Produto C": utiliza as colunas cross (view + direct) já calculadas.
      - Se o veículo for Meta e o Produto for "Produto C": somatória dos eventos mobile relevantes.
      - Caso contrário, mantém o valor existente em "conversoes".
    """
    df = df.with_columns(
        pl.when((pl.col("veiculo") == "Google") & (pl.col("Produto") == legacy_rule("LABEL_OR_MATCH_13")))
          .then(
              pl.col("conversoes_cross").fill_null(0).cast(pl.Float64) +
              pl.col("conversao_view_cross").fill_null(0).cast(pl.Float64)
          )
          .when((pl.col("veiculo") == "Meta") & (pl.col("Produto") == legacy_rule("LABEL_OR_MATCH_13")))
          .then(
              pl.col("start_trial_mobile_app").fill_null(0).cast(pl.Float64) +
              pl.col("app_custom_event_fb_mobile_complete_registration").fill_null(0).cast(pl.Float64) +
              pl.col("app_custom_event_fb_mobile_add_to_wishlist").fill_null(0).cast(pl.Float64)
          )
          .otherwise(pl.col("conversoes"))
          .alias("conversoes_cross")
    )
    return df

# -----------------------------------------------------------------------------
# FUNÇÃO PARA AJUSTAR AS CONVERSÕES DO META ADS (CORRIGIDA)
# -----------------------------------------------------------------------------
def process_meta_conversoes(df: pl.DataFrame) -> pl.DataFrame:
    # Renomeia a coluna "conversoes" para evitar duplicidade na expressão
    df = df.with_columns(pl.col("conversoes").alias("conversoes_old"))

    fields_produto_b = [
        legacy_rule("CUSTOM_EVENT_14"),
        legacy_rule("CUSTOM_EVENT_15"),
        legacy_rule("CUSTOM_EVENT_16"),
        legacy_rule("CUSTOM_EVENT_17"),
        legacy_rule("CUSTOM_EVENT_18")
    ]

    # Produto E: preencha com os eventos desejados (ex.: offsite/custom/app events)
    fields_produto_e = [
       "offsite_conversion.fb_pixel_custom.web_demo_conversion",
       legacy_rule("CUSTOM_EVENT_19")
    ]

    # Produto D: preencha com os eventos desejados (ex.: app_custom_event_* específicos)
    fields_produto_d = [
        "app_custom_event_fb_mobile_complete_registration"
    ]
    # Produto D: preencha com os eventos desejados (ex.: app_custom_event_* específicos)
    fields_produto_a = [
        legacy_rule("CUSTOM_EVENT_20"),
        legacy_rule("CUSTOM_EVENT_21"),
        legacy_rule("CUSTOM_EVENT_22"),
        "app_custom_event_fb_mobile_add_payment_info"
    ]
    df = ensure_float_columns(
        df,
        fields_produto_b
        + fields_produto_e
        + fields_produto_d
        + fields_produto_a
        + [
            "app_custom_event_fb_mobile_add_payment_info",
            "app_custom_event_fb_mobile_add_to_wishlist",
            "app_custom_event_fb_mobile_complete_registration",
            "start_trial_mobile_app",
        ],
    )

    # Para "Produto A" utilizamos a coluna única definida
    # Para "Produto C" o mapeamento será feito de acordo com o evento extraído do nome da campanha
    META_CONVERSION_MAPPING = {
        legacy_rule("LABEL_OR_MATCH_12"): fields_produto_a,
        legacy_rule("LABEL_OR_MATCH_05"): fields_produto_b,
        legacy_rule("LABEL_OR_MATCH_13"): "custom",  # Lógica customizada
        legacy_rule("LABEL_OR_MATCH_23"): fields_produto_e,  # Preencha os eventos acima
        legacy_rule("LABEL_OR_MATCH_24"): fields_produto_d,  # Preencha os eventos acima
    }

    # Certifica que conversoes_old seja numérico, se necessário
    expr = pl.col("conversoes_old").cast(pl.Float64)

    for produto, campo in META_CONVERSION_MAPPING.items():
        if produto == legacy_rule("LABEL_OR_MATCH_13"):
            # Extrai o último segmento da campanha usando regex
            conversion_event = (
                pl.col("nome_campanha")
                .fill_null("")
                .str.extract(r".*\|\s*(.+)$")
                .fill_null("")
                .str.to_lowercase()
            )
            default_conv = pl.col("conversoes_old").cast(pl.Float64).fill_null(0)
            conversao_expr = (
                pl.when(conversion_event == legacy_rule("LABEL_OR_MATCH_10"))
                  .then(pl.col("app_custom_event_fb_mobile_add_to_wishlist").cast(pl.Float64).fill_null(0))
                .when(conversion_event == legacy_rule("LABEL_OR_MATCH_25"))
                  .then(pl.col("app_custom_event_fb_mobile_complete_registration").cast(pl.Float64).fill_null(0))
                .when(conversion_event == legacy_rule("CAMPAIGN_EVENT_26"))
                  .then(pl.col("start_trial_mobile_app").cast(pl.Float64).fill_null(0))
                .otherwise(default_conv)
            )
        elif isinstance(campo, (list, tuple)):
            if campo:
                conversao_expr = reduce(
                    operator.add,
                    [pl.col(f).cast(pl.Float64).fill_null(0) for f in campo]
                )
            else:
                conversao_expr = pl.lit(0.0)
        else:
            conversao_expr = pl.col(campo).cast(pl.Float64).fill_null(0)

        expr = pl.when((pl.col("veiculo") == "Meta") & (pl.col("Produto") == produto)) \
               .then(conversao_expr) \
               .otherwise(expr)

    df = df.with_columns(expr.alias("conversoes")).drop("conversoes_old")
    return df

# -----------------------------------------------------------------------------
# NOVA FUNÇÃO: PROCESSA AS CONVERSÕES DO TIKTOK (PARA PRODUTO B)
# -----------------------------------------------------------------------------
def process_tiktok_conversoes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula as conversões para o produto 'Produto B' do TikTok.
    A conversão é a soma dos eventos s2s: contratado_au e contratado_im.
    Para outros produtos ou veículos, mantém o valor original.
    """
    # Nomes das colunas de eventos que serão somadas
    produto_b_event_cols = [
        legacy_rule("S2S_EVENT_27"),
        legacy_rule("S2S_EVENT_28"),
        legacy_rule("S2S_EVENT_29"),
        legacy_rule("S2S_EVENT_30"),
        legacy_rule("S2S_EVENT_31"),
    ]

    # Garante que as colunas de eventos existam no DataFrame para evitar erros.
    # Se uma coluna não existir, ela é criada com valor 0.
    for col_name in produto_b_event_cols:
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(0).cast(pl.Float64).alias(col_name))

    # Aplica a lógica de cálculo
    df = df.with_columns(
        pl.when(
            (pl.col("veiculo") == "TikTok") &
            (pl.col("Produto") == legacy_rule("LABEL_OR_MATCH_05"))
        )
        .then(
            # Soma as colunas de eventos, preenchendo nulos com 0
            pl.col(legacy_rule("S2S_EVENT_27")).fill_null(0).cast(pl.Float64) +
            pl.col(legacy_rule("S2S_EVENT_28")).fill_null(0).cast(pl.Float64) + 
            pl.col(legacy_rule("S2S_EVENT_29")).fill_null(0).cast(pl.Float64) +
            pl.col(legacy_rule("S2S_EVENT_30")).fill_null(0).cast(pl.Float64) +
            pl.col(legacy_rule("S2S_EVENT_31")).fill_null(0).cast(pl.Float64)
        )
        .otherwise(
            # Para TikTok, usa "result"; demais veículos mantêm "conversoes"
            pl.when(pl.col("veiculo") == "TikTok")
              .then(pl.col("result"))
              .otherwise(pl.col("conversoes"))
        )
        .alias("conversoes")  # O resultado da operação sobrescreve a coluna 'conversoes'
    )
    return df

# -----------------------------------------------------------------------------
# FUNÇÃO PARA GERAR UM RESUMO DIÁRIO COM CAMPANHA E GRUPO-ANÚNCIO (Resumo Padrão)
# -----------------------------------------------------------------------------
def summarize_daily(df: pl.DataFrame) -> pl.DataFrame:
    # 0) Normalizar nome da coluna produto/Produto
    if "produto" in df.columns and "Produto" not in df.columns:
        df = df.rename({"produto": "Produto"})
    
    # 1) Garantir colunas essenciais e tipos numéricos
    for col_ in ["impressoes", "cliques", "conversoes", "custo"]:
        if col_ not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col_))
    df = df.with_columns([
        pl.col("impressoes").fill_null(0).cast(pl.Float64),
        pl.col("cliques").fill_null(0).cast(pl.Float64),
        pl.col("conversoes").fill_null(0).cast(pl.Float64),
        pl.col("custo").fill_null(0).cast(pl.Float64),
    ])

    # 2) Nível: campanha
    df_campanha = (
        df.filter(pl.col("nivel") == "campanha")
          .with_columns([
              pl.col("nome_campanha"),
              pl.lit(None).cast(pl.Utf8).alias("nome_grupo_anuncio"),
              pl.lit(None).cast(pl.Utf8).alias("nome_anuncio"),
          ])
    )
    ck = ["data", "Produto", "veiculo", "nome_campanha", "nome_grupo_anuncio", "nome_anuncio"]
    summary_campanha = (
        df_campanha
        .group_by(ck)
        .agg([
            pl.col("impressoes").sum().alias("impressoes"),
            pl.col("cliques").sum().alias("cliques"),
            pl.col("conversoes").sum().alias("conversoes"),
            pl.col("custo").sum().alias("custo"),
        ])
        .with_columns([
            pl.when(pl.col("conversoes") > 0).then(pl.col("custo") / pl.col("conversoes")).otherwise(0).alias("CPA"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("cliques") / pl.col("impressoes")) * 100).otherwise(0).alias("CTR"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("custo") / pl.col("impressoes")) * 1000).otherwise(0).alias("CPM"),
            pl.lit("campanha").alias("nivel"),
            pl.lit("").cast(pl.Utf8).alias("id_anuncio"),
        ])
    )

    # 3) Nível: grupo de anúncio
    df_grupo = (
        df.filter(pl.col("nivel") == "grupo_anuncio")
          .with_columns([
              pl.col("nome_campanha"),
              pl.col("nome_grupo_anuncio"),
              pl.lit("").cast(pl.Utf8).alias("nome_anuncio"),
          ])
    )
    gk = ["data", "Produto", "veiculo", "nome_campanha", "nome_grupo_anuncio", "nome_anuncio"]
    summary_grupo = (
        df_grupo
        .group_by(gk)
        .agg([
            pl.col("impressoes").sum().alias("impressoes"),
            pl.col("cliques").sum().alias("cliques"),
            pl.col("conversoes").sum().alias("conversoes"),
            pl.col("custo").sum().alias("custo"),
        ])
        .with_columns([
            pl.when(pl.col("conversoes") > 0).then(pl.col("custo") / pl.col("conversoes")).otherwise(0).alias("CPA"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("cliques") / pl.col("impressoes")) * 100).otherwise(0).alias("CTR"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("custo") / pl.col("impressoes")) * 1000).otherwise(0).alias("CPM"),
            pl.lit("grupo_anuncio").alias("nivel"),
            pl.lit("").cast(pl.Utf8).alias("id_anuncio"),
        ])
    )

    # 4) Nível: anúncio
    df_anuncio = df.filter(pl.col("nivel") == "anuncio")
    if "id_anuncio" in df_anuncio.columns:
        df_anuncio = df_anuncio.with_columns([
            pl.col("nome_anuncio").cast(pl.Utf8),
            pl.col("id_anuncio").cast(pl.Utf8),
        ])
    else:
        df_anuncio = df_anuncio.with_columns([
            pl.col("nome_anuncio").cast(pl.Utf8),
            pl.lit("").cast(pl.Utf8).alias("id_anuncio"),
        ])
    df_anuncio = df_anuncio.with_columns([
        pl.col("nome_campanha"),
        pl.col("nome_grupo_anuncio"),
    ])
    ak = ["data", "Produto", "veiculo", "nome_campanha", "nome_grupo_anuncio", "nome_anuncio", "id_anuncio"]
    summary_anuncio = (
        df_anuncio
        .group_by(ak)
        .agg([
            pl.col("impressoes").sum().alias("impressoes"),
            pl.col("cliques").sum().alias("cliques"),
            pl.col("conversoes").sum().alias("conversoes"),
            pl.col("custo").sum().alias("custo"),
        ])
        .with_columns([
            pl.when(pl.col("conversoes") > 0).then(pl.col("custo") / pl.col("conversoes")).otherwise(0).alias("CPA"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("cliques") / pl.col("impressoes")) * 100).otherwise(0).alias("CTR"),
            pl.when(pl.col("impressoes") > 0).then((pl.col("custo") / pl.col("impressoes")) * 1000).otherwise(0).alias("CPM"),
            pl.lit("anuncio").alias("nivel"),
        ])
    )

    # 5) Concatena tudo num único DataFrame
    final_cols = [
        "nivel", "nome_campanha", "nome_grupo_anuncio", "nome_anuncio", "id_anuncio",
        "Produto", "veiculo", "data", "impressoes", "cliques",
        "conversoes", "custo", "CPA", "CTR", "CPM"
    ]
    
    # Debug: verificar todas as colunas
    # Debug: sumários criados
    
    # Adicionar colunas faltantes com valores padrão antes de selecionar
    for df_name, df_summ in [("campanha", summary_campanha), ("grupo", summary_grupo), ("anuncio", summary_anuncio)]:
        for col in final_cols:
            if col not in df_summ.columns:
                # Adicionando coluna faltante
                if col == "id_anuncio":
                    df_summ = df_summ.with_columns(pl.lit("").cast(pl.Utf8).alias(col))
                elif col in ["nome_campanha", "nome_grupo_anuncio", "nome_anuncio"]:
                    df_summ = df_summ.with_columns(pl.lit("").cast(pl.Utf8).alias(col))
                elif col in ["impressoes", "cliques", "conversoes", "custo", "CPA", "CTR", "CPM"]:
                    df_summ = df_summ.with_columns(pl.lit(0.0).alias(col))
                else:
                    df_summ = df_summ.with_columns(pl.lit("").alias(col))
        
        # Atualizar a variável
        if df_name == "campanha":
            summary_campanha = df_summ
        elif df_name == "grupo":
            summary_grupo = df_summ
        else:
            summary_anuncio = df_summ
    
    summary_all = pl.concat([
        summary_campanha.select(final_cols).cast({"id_anuncio": pl.Utf8, "nome_campanha": pl.Utf8, "nome_grupo_anuncio": pl.Utf8, "nome_anuncio": pl.Utf8}),
        summary_grupo.select(final_cols).cast({"id_anuncio": pl.Utf8, "nome_campanha": pl.Utf8, "nome_grupo_anuncio": pl.Utf8, "nome_anuncio": pl.Utf8}),
        summary_anuncio.select(final_cols).cast({"id_anuncio": pl.Utf8, "nome_campanha": pl.Utf8, "nome_grupo_anuncio": pl.Utf8, "nome_anuncio": pl.Utf8}),
    ], how="vertical")

    # 6) Agora sim cria o tipo de campanha
    summary_all = summary_all.with_columns(
        pl.when(pl.col("nome_campanha")
               .str.to_lowercase()
               .str.contains("search"))
          .then(pl.lit("Search"))
          .otherwise(pl.lit("App"))
          .alias("tipo_campanha")
    )

    # 7) E finalmente ordena — incluindo tipo_campanha, se desejar
    summary_all = summary_all.sort(
        ["data", "veiculo", "tipo_campanha", "nome_campanha"]
    )

    return summary_all

# -----------------------------------------------------------------------------
# NOVA FUNÇÃO: GERA O EXCEL SOT COM OS CAMPOS SOLICITADOS
# -----------------------------------------------------------------------------
def summarize_sot(df: pl.DataFrame) -> pl.DataFrame:
    # Aplica a lógica para a coluna conversoes_cross, agora incluindo Meta para produto Produto C
    df = df.with_columns(
        pl.when((pl.col("veiculo") == "Google") & (pl.col("Produto") == legacy_rule("LABEL_OR_MATCH_13")))
          .then(
              pl.col("conversoes_cross").fill_null(0).cast(pl.Float64) +
              pl.col("conversao_view_cross").fill_null(0).cast(pl.Float64)
          )
          .when((pl.col("veiculo") == "Meta") & (pl.col("Produto") == legacy_rule("LABEL_OR_MATCH_13")))
          .then(
              pl.col("start_trial_mobile_app").fill_null(0).cast(pl.Float64) +
              pl.col("app_custom_event_fb_mobile_complete_registration").fill_null(0).cast(pl.Float64) +
              pl.col("app_custom_event_fb_mobile_add_to_wishlist").fill_null(0).cast(pl.Float64)
          )
          .otherwise(pl.col("conversoes"))
          .alias("conversoes_cross")
    )

    # As demais transformações
    df = df.with_columns([
        pl.col("impressoes").fill_null(0).cast(pl.Float64),
        pl.col("cliques").fill_null(0).cast(pl.Float64),
        pl.col("conversoes").fill_null(0).cast(pl.Float64),
        pl.col("custo").fill_null(0).cast(pl.Float64)
    ])
    if "conversoes_by_conversion_date_pers" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("conversoes_by_conversion_date_pers"))
    if "todas_conversions_by_conversion_date_pers" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("todas_conversions_by_conversion_date_pers"))

    df = df.with_columns([
        pl.when(pl.col("veiculo") == "Google")
          .then(pl.col("conversoes_by_conversion_date_pers"))
          .otherwise(pl.lit(0))
          .alias("conversoes_by_conversion_date_pers"),
        pl.when(pl.col("veiculo") == "Google")
          .then(pl.col("todas_conversions_by_conversion_date_pers"))
          .otherwise(pl.lit(0))
          .alias("todas_conversions_by_conversion_date_pers")
    ])

    group_keys_camp = ["data", "Produto", "veiculo", "nome_campanha"]
    summary_camp = df.filter(pl.col("nivel") == "campanha").group_by(group_keys_camp).agg([
        pl.col("impressoes").sum().alias("Impressões"),
        pl.col("cliques").sum().alias("Cliques"),
        pl.col("conversoes").sum().alias("Aquisições_negociais"),
        pl.col("conversoes_cross").sum().alias("Aquisições_negociais_cross"),
        pl.col("custo").sum().alias("Investimento"),
        pl.col("conversoes_by_conversion_date_pers").sum().alias("Conversao - no momento da conversao"),
        pl.col("todas_conversions_by_conversion_date_pers").sum().alias("Todas as conversoes - no momento da conversao")
    ]).with_columns([
        pl.lit("campanha").alias("Nível"),
        pl.col("nome_campanha").alias("Campanha"),
        pl.lit("").alias("Grupo Anúncios"),
        pl.col("data").alias("Data"),
        pl.col("veiculo").alias("Veículo")
    ])

    group_keys_grupo = ["data", "Produto", "veiculo", "nome_campanha", "nome_grupo_anuncio"]
    summary_grupo = df.filter(pl.col("nivel") == "grupo_anuncio").group_by(group_keys_grupo).agg([
        pl.col("impressoes").sum().alias("Impressões"),
        pl.col("cliques").sum().alias("Cliques"),
        pl.col("conversoes").sum().alias("Aquisições_negociais"),
        pl.col("conversoes_cross").sum().alias("Aquisições_negociais_cross"),
        pl.col("custo").sum().alias("Investimento"),
        pl.col("conversoes_by_conversion_date_pers").sum().alias("Conversao - no momento da conversao"),
        pl.col("todas_conversions_by_conversion_date_pers").sum().alias("Todas as conversoes - no momento da conversao")
    ]).with_columns([
        pl.lit("grupo_anuncio").alias("Nível"),
        pl.col("nome_campanha").alias("Campanha"),
        pl.col("nome_grupo_anuncio").alias("Grupo Anúncios"),
        pl.col("data").alias("Data"),
        pl.col("veiculo").alias("Veículo")
    ])

    final_cols = ["Nível", "nome_campanha", "Grupo Anúncios", "Produto", "Data", "Veículo",
                  "Investimento", "Impressões", "Cliques", "Aquisições_negociais", "Aquisições_negociais_cross"]

    summary_sot = pl.concat([summary_camp.select(final_cols), summary_grupo.select(final_cols)], how="vertical")
    summary_sot = summary_sot.sort(["Data", "Veículo", "nome_campanha"])
    return summary_sot

def gerar_sot(df: pl.DataFrame) -> None:
    df_sot = summarize_sot(df)
    try:
        if os.path.exists(SOT_OUTPUT_FILE):
            os.remove(SOT_OUTPUT_FILE)
    except PermissionError:
        print("O arquivo SOT está aberto ou sendo usado. Feche-o e tente novamente.")
        return
    df_sot.to_pandas().to_csv(SOT_OUTPUT_FILE, index=False)
    print(f"Arquivo SOT exportado para {SOT_OUTPUT_FILE}")

# -----------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    df = load_data_consolidado()
    
    # DataFrame consolidado carregado
    
    if "week_short" not in df.columns:
        # Coluna 'week_short' não encontrada
        pass

    if "adgroup_name" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("veiculo") == "TikTok")
              .then(pl.col("adgroup_name"))
              .otherwise(pl.col("nome_grupo_anuncio"))
              .alias("nome_grupo_anuncio")
        )

    df_meta = df.filter(pl.col("veiculo") == "Meta")

    if df_meta.height > 0:
        # 1) Extrai um único nome de campanha por id_campanha
        meta_campaigns = (
            df_meta
            .filter(pl.col("nivel") == "campanha")
            .select(["id_campanha", "nome_campanha"])
            .unique(subset=["id_campanha"])       # <-- garante 1 linha por campanha
            .rename({"nome_campanha": "nome_campanha_camp"})
        )

        if meta_campaigns.height > 0:
            # 2) Faz o join LEFT garantindo que não haja multiplicação de linhas
            df_meta = (
                df_meta
                .join(meta_campaigns, on="id_campanha", how="left")
                .with_columns(
                    pl.coalesce(["nome_campanha_camp", "nome_campanha"])
                    .alias("nome_campanha")
                )
                .drop("nome_campanha_camp")
            )

        # 3) Reune Meta (ajustado) com os demais veículos
        df = pl.concat(
            [df.filter(pl.col("veiculo") != "Meta"), df_meta],
            how="vertical"
        )

    # Processa as conversões
    df = process_google_conversoes(df)
    df = process_meta_conversoes(df)
    df = process_tiktok_conversoes(df) # <-- NOVA LÓGICA APLICADA AQUI

    # Salva o nome original da campanha para o SOT em uma nova coluna
    df = df.with_columns(pl.col("nome_campanha").alias("nome_campanha_original"))

    # Para o resumo normal, renomeia as campanhas (usando a função renomear_campanha)
    rename_expr = (
        pl.when(pl.col("nivel").is_in(["campanha", "grupo_anuncio"]))
          .then(
              pl.struct(["nome_campanha", "Produto", "veiculo"])
                .map_elements(
                    lambda row: renomear_campanha(row["nome_campanha"], row["Produto"], row["veiculo"]),
                    return_dtype=pl.Utf8,
                )
          )
          .otherwise(pl.col("nome_campanha"))
    )
    df_renamed = (
        df.with_columns(rename_expr.alias("nome_campanha"))
          .filter(pl.col("nome_campanha").is_not_null() & (pl.col("nome_campanha") != ""))
    )

    df_resumo = summarize_daily(df_renamed)

    output_file = os.path.join(OUTPUT_PATH, "resumo_diario_powerbi.csv")
    try:
        if os.path.exists(output_file):
            os.remove(output_file)
    except PermissionError:
        print("O arquivo está aberto ou sendo usado. Feche-o e tente novamente.")
        return

    df_resumo.to_pandas().to_csv(output_file, index=False)
    print(f"Resumo diário completo exportado para {output_file}")

    # Para o relatório SOT, utiliza o nome original da campanha sem renomeá-lo
    df_sot = df.with_columns(pl.col("nome_campanha_original").alias("nome_campanha"))
    # Aplica a lógica _cross para SOT, agora incluindo Meta para produto Produto C
    df_sot = process_conversoes_cross(df_sot)
    gerar_sot(df_sot)
    
if __name__ == "__main__":
    main()
