from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

from src.depara_config import carregar_depara
from src.utils_do_pai import CONFIG_GOOGLE_PATH


VISOES_CONFIG, EVENTOS_POR_VISAO = carregar_depara()


@dataclass(frozen=True)
class WeeklyReportConfig:
    """Descreve os insumos e regras de um relatório semanal."""

    product: str
    google_customer_id: str
    output_file: Path
    meta_parquet: Path | None = None
    tiktok_parquet: Path | None = None
    google_app_conversion_mode: str = "click_plus_view"
    meta_name_candidates: tuple[str, ...] = (
        "nome_campanha",
        "campaign_name",
        "ad_set_name",
        "adset_name",
    )
    tiktok_name_candidates: tuple[str, ...] = (
        "nome_campanha",
        "campaign_name",
        "adgroup_name",
    )
    meta_event_aliases: dict[str, frozenset[str]] = field(default_factory=dict)
    tiktok_fallback_conversion_column: str = "conversoes"


def get_week_range(date_obj: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Retorna segunda-feira e domingo da semana da data informada."""
    monday = date_obj - pd.Timedelta(days=date_obj.weekday())
    sunday = monday + pd.Timedelta(days=6)
    return monday, sunday


def format_date_slash(date_obj: pd.Timestamp) -> str:
    """Formata a data como `dd/mm/yyyy`."""
    return date_obj.strftime("%d/%m/%Y")


def format_week_numbered(numero: int, monday: pd.Timestamp, sunday: pd.Timestamp) -> str:
    """Formata a linha da semana com o prefixo ordinal esperado pelo relatório."""
    return f"{numero}- {monday.strftime('%d/%m/%Y')} - {sunday.strftime('%d/%m/%Y')}"


def parse_data_mista(datas: pd.Series) -> pd.Series:
    """Converte datas ISO e BR sem inverter dia e mês."""
    datas_str = datas.astype(str).str.strip()
    datas_parseadas = pd.Series(pd.NaT, index=datas.index, dtype="datetime64[ns]")

    mask_iso = datas_str.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if mask_iso.any():
        datas_parseadas.loc[mask_iso] = pd.to_datetime(
            datas_str.loc[mask_iso],
            format="%Y-%m-%d",
            errors="coerce",
        )

    mask_br = datas_str.str.match(r"^\d{1,2}/\d{1,2}/\d{4}$", na=False)
    if mask_br.any():
        datas_parseadas.loc[mask_br] = pd.to_datetime(
            datas_str.loc[mask_br],
            format="%d/%m/%Y",
            errors="coerce",
        )

    mask_restante = datas_parseadas.isna()
    if mask_restante.any():
        datas_parseadas.loc[mask_restante] = pd.to_datetime(
            datas.loc[mask_restante],
            errors="coerce",
        )

    return datas_parseadas


def identificar_visao(veiculo: str, produto: str, nome_base: str) -> str:
    """Resolve a visão configurada para um nome de campanha ou grupo."""
    if not nome_base or pd.isna(nome_base):
        return "Outro"

    visoes = VISOES_CONFIG.get((veiculo, produto), [])
    nome_base_lower = str(nome_base).lower()

    for visao_nome, palavras_chave in visoes:
        if all(palavra.lower() in nome_base_lower for palavra in palavras_chave):
            return visao_nome

    return "Outro"


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _sum_allowed_events(
    row: pd.Series,
    eventos: list[str],
    available_cols: set[str],
    alias_groups: dict[str, frozenset[str]] | None = None,
) -> float:
    """Soma os eventos permitidos respeitando aliases equivalentes."""
    total = 0.0
    seen_groups: set[tuple[str, ...]] = set()
    alias_groups = alias_groups or {}

    for evento in eventos:
        aliases = tuple(sorted(alias_groups.get(evento, frozenset({evento}))))
        if aliases in seen_groups:
            continue
        seen_groups.add(aliases)

        valores = [
            float(row.get(alias, 0) or 0)
            for alias in aliases
            if alias in available_cols and pd.notna(row.get(alias))
        ]
        if valores:
            total += max(valores) if len(aliases) > 1 else valores[0]

    return total


def _rename_first_match(df: pd.DataFrame, target: str, candidates: tuple[str, ...]) -> None:
    for candidate in candidates:
        if candidate in df.columns:
            df.rename(columns={candidate: target}, inplace=True)
            return


def _normalize_platform_frame(
    path: Path,
    *,
    level: str,
    name_candidates: tuple[str, ...],
    include_conversion_column: bool,
) -> pd.DataFrame:
    df = pl.read_parquet(path).to_pandas()

    if "nivel" in df.columns:
        df = df[df["nivel"] == level].copy()

    _rename_first_match(df, "data", ("data", "date", "date_start", "stat_time_day"))
    _rename_first_match(df, "nome_campanha", name_candidates)
    _rename_first_match(df, "custo", ("custo", "gasto", "spend", "cost"))
    _rename_first_match(df, "impressoes", ("impressoes", "impressions", "show", "impression"))
    _rename_first_match(df, "cliques", ("link_click","cliques", "clicks", "click"))

    if include_conversion_column and "result" in df.columns and "conversoes" not in df.columns:
        df.rename(columns={"result": "conversoes"}, inplace=True)

    df = df.loc[:, ~df.columns.duplicated()].copy()

    text_cols = ["data", "nome_campanha"]
    numeric_cols = ["custo", "impressoes", "cliques"]
    if include_conversion_column:
        numeric_cols.append("conversoes")

    for col in text_cols:
        if col not in df.columns:
            df[col] = None

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0

    df = df[df["data"].notna()].copy()
    df["nome_campanha"] = df["nome_campanha"].astype("string").str.strip()
    df = df[df["nome_campanha"].notna() & (df["nome_campanha"] != "")].copy()

    for col in numeric_cols:
        df[col] = _to_numeric(df[col])

    return df


def _compute_google_conversions(
    visao: str,
    conv_df: pd.DataFrame,
    app_mode: str,
) -> float:
    if conv_df.empty:
        return 0.0

    clique_total = _to_numeric(conv_df["conversoes_clique"]).sum()
    view_total = _to_numeric(conv_df["conversoes_view"]).sum()
    total_conversions = _to_numeric(conv_df["conversoes_total"]).sum()

    if "app" in visao.lower() and app_mode == "all_minus_view":
        return max(0.0, total_conversions - view_total)

    return max(0.0, clique_total + view_total)


def buscar_dados_google(
    config: WeeklyReportConfig,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Busca performance e conversões do Google Ads filtradas pelo de-para."""
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException

        client = GoogleAdsClient.load_from_storage(str(CONFIG_GOOGLE_PATH))
        ga_service = client.get_service("GoogleAdsService")

        query_metrics = f"""
        SELECT
          segments.date,
          campaign.name,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.search_impression_share
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND customer.id = {config.google_customer_id}
        """

        metrics_rows: list[dict[str, float | str]] = []
        for batch in ga_service.search_stream(
            customer_id=config.google_customer_id,
            query=query_metrics,
        ):
            for row in batch.results:
                metrics_rows.append(
                    {
                        "data": str(row.segments.date),
                        "nome_campanha": row.campaign.name,
                        "impressoes": row.metrics.impressions,
                        "cliques": row.metrics.clicks,
                        "custo": row.metrics.cost_micros / 1_000_000,
                        "parcela_impr": row.metrics.search_impression_share or 0,
                    }
                )

        df_metrics = pd.DataFrame(metrics_rows)
        if df_metrics.empty:
            return pd.DataFrame()

        query_conv = f"""
        SELECT
          segments.date,
          campaign.name,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.view_through_conversions,
          metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND customer.id = {config.google_customer_id}
        """

        conv_rows: list[dict[str, float | str]] = []
        for batch in ga_service.search_stream(
            customer_id=config.google_customer_id,
            query=query_conv,
        ):
            for row in batch.results:
                conv_rows.append(
                    {
                        "data": str(row.segments.date),
                        "nome_campanha": row.campaign.name,
                        "evento": row.segments.conversion_action_name,
                        "conversoes_clique": row.metrics.conversions,
                        "conversoes_view": row.metrics.view_through_conversions,
                        "conversoes_total": row.metrics.all_conversions,
                    }
                )

        df_conv = pd.DataFrame(conv_rows)
        grouped_conv = (
            {
                chave: group.copy()
                for chave, group in df_conv.groupby(["data", "nome_campanha"])
            }
            if not df_conv.empty
            else {}
        )

        linhas: list[dict[str, float | str]] = []
        for metric_row in df_metrics.itertuples(index=False):
            visao = identificar_visao("Google", config.product, metric_row.nome_campanha)
            eventos_permitidos = EVENTOS_POR_VISAO.get(("Google", config.product, visao), [])
            conv_group = grouped_conv.get((metric_row.data, metric_row.nome_campanha), pd.DataFrame())

            if eventos_permitidos and not conv_group.empty:
                conv_group = conv_group[conv_group["evento"].isin(eventos_permitidos)].copy()

            conversoes = _compute_google_conversions(
                visao,
                conv_group,
                config.google_app_conversion_mode,
            )

            linhas.append(
                {
                    "data": metric_row.data,
                    "nome_campanha": metric_row.nome_campanha,
                    "visao": visao,
                    "impressoes": metric_row.impressoes,
                    "cliques": metric_row.cliques,
                    "custo": metric_row.custo,
                    "parcela_impr": metric_row.parcela_impr,
                    "conversoes": conversoes,
                    "plataforma": "google",
                }
            )

        return pd.DataFrame(linhas)
    except Exception as exc:
        google_exc = exc.__class__.__name__ == "GoogleAdsException"
        if google_exc:
            print(f"Erro ao buscar dados Google Ads de {config.product}: {exc}")
            return pd.DataFrame()
        print(f"Erro inesperado ao buscar Google Ads de {config.product}: {exc}")
        return pd.DataFrame()


def buscar_dados_meta(config: WeeklyReportConfig) -> pd.DataFrame:
    """Lê o parquet do Meta e soma os eventos permitidos por visão."""
    if config.meta_parquet is None:
        return pd.DataFrame()
    if not config.meta_parquet.exists():
        print(f"Arquivo Meta não encontrado: {config.meta_parquet}")
        return pd.DataFrame()

    try:
        df = _normalize_platform_frame(
            config.meta_parquet,
            level="campanha",
            name_candidates=config.meta_name_candidates,
            include_conversion_column=False,
        )
        if df.empty:
            return df

        available_cols = set(df.columns)
        linhas: list[dict[str, float | str]] = []
        for _, row in df.iterrows():
            visao = identificar_visao("Meta", config.product, row["nome_campanha"])
            eventos_permitidos = EVENTOS_POR_VISAO.get(("Meta", config.product, visao), [])
            conversoes = _sum_allowed_events(
                row,
                eventos_permitidos,
                available_cols,
                config.meta_event_aliases,
            )
            linhas.append(
                {
                    "data": row["data"],
                    "nome_campanha": row["nome_campanha"],
                    "visao": visao,
                    "custo": row["custo"],
                    "impressoes": row["impressoes"],
                    "cliques": row["cliques"],
                    "conversoes": conversoes,
                    "parcela_impr": 0.0,
                    "plataforma": "meta",
                }
            )

        return pd.DataFrame(linhas)
    except Exception as exc:
        print(f"Erro ao ler dados Meta de {config.product}: {exc}")
        return pd.DataFrame()


def buscar_dados_tiktok(config: WeeklyReportConfig) -> pd.DataFrame:
    """Lê o parquet do TikTok usando eventos configurados ou fallback de conversão."""
    if config.tiktok_parquet is None:
        return pd.DataFrame()
    if not config.tiktok_parquet.exists():
        print(f"Arquivo TikTok não encontrado: {config.tiktok_parquet}")
        return pd.DataFrame()

    try:
        df = _normalize_platform_frame(
            config.tiktok_parquet,
            level="campanha",
            name_candidates=config.tiktok_name_candidates,
            include_conversion_column=True,
        )
        if df.empty:
            return df

        df["data"] = parse_data_mista(df["data"]).dt.strftime("%Y-%m-%d")
        df = df[df["data"].notna()].copy()
        available_cols = set(df.columns)

        event_cols = [
            col for col in df.columns if col.startswith("evento_") or col.startswith("app_custom_event.")
        ]
        for col in event_cols:
            df[col] = _to_numeric(df[col])

        linhas: list[dict[str, float | str]] = []
        for _, row in df.iterrows():
            visao = identificar_visao("TikTok", config.product, row["nome_campanha"])
            eventos_permitidos = EVENTOS_POR_VISAO.get(("TikTok", config.product, visao), [])
            conversoes = float(row.get(config.tiktok_fallback_conversion_column, 0) or 0)

            if eventos_permitidos:
                conversoes = _sum_allowed_events(row, eventos_permitidos, available_cols)

            linhas.append(
                {
                    "data": row["data"],
                    "nome_campanha": row["nome_campanha"],
                    "visao": visao,
                    "custo": row["custo"],
                    "impressoes": row["impressoes"],
                    "cliques": row["cliques"],
                    "conversoes": conversoes,
                    "parcela_impr": 0.0,
                    "plataforma": "tiktok",
                }
            )

        return pd.DataFrame(linhas)
    except Exception as exc:
        print(f"Erro ao ler dados TikTok de {config.product}: {exc}")
        return pd.DataFrame()


def agregar_por_semana(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega as métricas diárias por semana, visão e plataforma."""
    if df.empty or "data" not in df.columns:
        return pd.DataFrame()

    df = df.loc[:, ~df.columns.duplicated()].copy()
    df["data"] = parse_data_mista(df["data"])
    df = df.dropna(subset=["data"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["monday"] = df["data"].apply(lambda value: get_week_range(value)[0])
    df["sunday"] = df["data"].apply(lambda value: get_week_range(value)[1])
    df["parcela_impr_ponderada"] = df["parcela_impr"] * df["impressoes"]

    group_cols = ["monday", "nome_campanha", "visao", "plataforma"]
    df_agg = (
        df.groupby(group_cols, as_index=False)
        .agg(
            custo=("custo", "sum"),
            impressoes=("impressoes", "sum"),
            cliques=("cliques", "sum"),
            conversoes=("conversoes", "sum"),
            parcela_impr_ponderada=("parcela_impr_ponderada", "sum"),
        )
        .rename(columns={"monday": "semana"})
    )
    df_agg["parcela_impr"] = df_agg.apply(
        lambda row: (
            row["parcela_impr_ponderada"] / row["impressoes"]
            if row["impressoes"] > 0
            else 0.0
        ),
        axis=1,
    )

    return df_agg.drop(columns=["parcela_impr_ponderada"])


def gerar_estrutura_2_linhas(df: pd.DataFrame) -> pd.DataFrame:
    """Gera as linhas da semana anterior e da semana atual para cada campanha."""
    if df.empty:
        return pd.DataFrame()

    linhas: list[dict[str, float | str]] = []
    todas_semanas = sorted(df["semana"].unique())

    for semana_atual in todas_semanas:
        semana_anterior = semana_atual - pd.Timedelta(days=7)
        sunday_atual = semana_atual + pd.Timedelta(days=6)
        sunday_anterior = semana_anterior + pd.Timedelta(days=6)
        semana_str = format_date_slash(semana_atual)

        df_atual = df[df["semana"] == semana_atual]
        df_anterior = df[df["semana"] == semana_anterior]
        chaves = pd.concat(
            [
                df_atual[["nome_campanha", "visao", "plataforma"]],
                df_anterior[["nome_campanha", "visao", "plataforma"]],
            ],
            ignore_index=True,
        ).drop_duplicates()

        for _, chave in chaves.iterrows():
            filtro_atual = (
                (df_atual["nome_campanha"] == chave["nome_campanha"])
                & (df_atual["visao"] == chave["visao"])
                & (df_atual["plataforma"] == chave["plataforma"])
            )
            filtro_anterior = (
                (df_anterior["nome_campanha"] == chave["nome_campanha"])
                & (df_anterior["visao"] == chave["visao"])
                & (df_anterior["plataforma"] == chave["plataforma"])
            )

            dados_atual = df_atual.loc[filtro_atual]
            dados_anterior = df_anterior.loc[filtro_anterior]

            linhas.append(
                {
                    "semana": semana_str,
                    "nome_semana": format_week_numbered(1, semana_anterior, sunday_anterior),
                    "nome_campanha": chave["nome_campanha"],
                    "visao": chave["visao"],
                    "plataforma": chave["plataforma"],
                    "custo": dados_anterior["custo"].sum(),
                    "impressoes": dados_anterior["impressoes"].sum(),
                    "cliques": dados_anterior["cliques"].sum(),
                    "parcela_impr": dados_anterior["parcela_impr"].mean() if not dados_anterior.empty else 0.0,
                    "conversoes": dados_anterior["conversoes"].sum(),
                }
            )
            linhas.append(
                {
                    "semana": semana_str,
                    "nome_semana": format_week_numbered(2, semana_atual, sunday_atual),
                    "nome_campanha": chave["nome_campanha"],
                    "visao": chave["visao"],
                    "plataforma": chave["plataforma"],
                    "custo": dados_atual["custo"].sum(),
                    "impressoes": dados_atual["impressoes"].sum(),
                    "cliques": dados_atual["cliques"].sum(),
                    "parcela_impr": dados_atual["parcela_impr"].mean() if not dados_atual.empty else 0.0,
                    "conversoes": dados_atual["conversoes"].sum(),
                }
            )

    return pd.DataFrame(linhas)


def _empty_report() -> pd.DataFrame:
    """Cria um DataFrame vazio com o schema final do relatório semanal."""
    return pd.DataFrame(
        columns=[
            "semana",
            "nome_semana",
            "nome_campanha",
            "visao",
            "plataforma",
            "custo",
            "impressoes",
            "cliques",
            "parcela_impr",
            "conversoes",
        ]
    )


def run_weekly_report(config: WeeklyReportConfig) -> pd.DataFrame:
    """Executa a geração completa do relatório semanal parametrizado."""
    print("=" * 60)
    print(f"GERANDO RELATÓRIO SEMANAL - {config.product.upper()}")
    print("=" * 60)

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=180)
    print(f"\nPeríodo: {start_date} a {end_date}")

    print("\n[1/4] Buscando dados Google Ads...")
    df_google = buscar_dados_google(config, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    print(f"  -> {len(df_google)} linhas obtidas")

    print("\n[2/4] Lendo dados Meta...")
    df_meta = buscar_dados_meta(config)
    print(f"  -> {len(df_meta)} linhas obtidas")

    print("\n[3/4] Lendo dados TikTok...")
    df_tiktok = buscar_dados_tiktok(config)
    print(f"  -> {len(df_tiktok)} linhas obtidas")

    print("\n[4/4] Processando e agregando...")
    dfs = [df for df in (df_google, df_meta, df_tiktok) if not df.empty]
    if not dfs:
        df_final = _empty_report()
        pl.from_pandas(df_final).write_parquet(config.output_file)
        print(f"Arquivo vazio salvo em {config.output_file}")
        return df_final

    df_all = pd.concat(dfs, ignore_index=True)
    df_semanal = agregar_por_semana(df_all)
    df_final = gerar_estrutura_2_linhas(df_semanal)

    if df_final.empty:
        df_final = _empty_report()

    base_cols = [
        "semana",
        "nome_semana",
        "nome_campanha",
        "visao",
        "plataforma",
        "custo",
        "impressoes",
        "cliques",
        "parcela_impr",
        "conversoes",
    ]
    df_final = df_final[base_cols]

    pl.from_pandas(df_final).write_parquet(config.output_file)
    print(f"\nRelatório salvo em {config.output_file}")
    print(f"  Total de linhas: {len(df_final)}")
    print(f"  Visões encontradas: {df_final['visao'].nunique() if not df_final.empty else 0}")
    return df_final
