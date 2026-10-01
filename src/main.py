from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
from facebook_business.api import FacebookAdsApi

from src.account_catalog import list_accounts
from src.alcance_semanal import executar_alcance_semanal
from src.google_ads import executar_google_ads
from src.kwai import executar_kwai
from src.metads import inserir_substituir as inserir_meta_ads, obter_dados_meta_ads
from src.pipeline_utils import align_and_concat_frames, load_yaml_config
from src.tiktok import (
    TIKTOK_OUTPUT_FILE,
    baixar_assets_tiktok,
    carregar_config_tiktok,
    executar_tiktok,
    inserir_substituir as inserir_tiktok,
    obter_dados_tiktok,
)
from src.utils_do_pai import CONFIG_META_PATH, CONFIG_TIKTOK_PATH, OUTPUT_PATH, PIPELINE_TIMEZONE


META_ACCOUNTS = list_accounts("meta")
TIKTOK_ACCOUNTS = list_accounts("tiktok")
KWAI_ACCOUNTS = list_accounts("kwai")
META_OUTPUT_FILE = OUTPUT_PATH / "meta_ads_data_all_new.parquet"
SAO_PAULO_TZ = ZoneInfo(PIPELINE_TIMEZONE)
DAILY_BLOCK_DAYS = 7
DAILY_BLOCK_SLEEP_SECONDS = 80 # mudei
DAILY_META_TIKTOK_WINDOW_DAYS = 90 # mudei
DAILY_KWAI_WINDOW_DAYS = 90 # mudei
WEEKLY_REACH_WINDOW_DAYS = 365 # mudei


def init_facebook_api() -> None:
    """Initialize the Meta SDK with the configured application credentials."""
    secrets = load_yaml_config(CONFIG_META_PATH / "meta.yaml")
    FacebookAdsApi.init(
        app_id=secrets.get("app_id"),
        app_secret=secrets.get("app_secret"),
        access_token=secrets.get("access_token"),
    )


def _executar_com_isolamento(etapa: str, sucessos: list[str], falhas: list[str], func) -> bool:
    """Run an extraction step and keep the process alive if it fails."""
    try:
        func()
        sucessos.append(etapa)
        return True
    except Exception as exc:
        detalhe = f"{etapa}: {type(exc).__name__}: {exc}"
        falhas.append(detalhe)
        print(f"[Runner] Falha em {detalhe}")
        print("[Runner] Continuando com as proximas etapas.")
        return False


def _imprimir_resumo_job(sucessos: list[str], falhas: list[str]) -> None:
    print("===== RESUMO DO JOB =====")
    if sucessos:
        print("[Runner] SUCESSOS:")
        for sucesso in sucessos:
            print(f"[Runner] - {sucesso}")
    else:
        print("[Runner] SUCESSOS: nenhum")

    if falhas:
        print("[Runner] ERROS:")
        for falha in falhas:
            print(f"[Runner] - {falha}")
    else:
        print("[Runner] ERROS: nenhum")


def iterar_blocos_datas(data_inicio: str, data_fim: str, fmt: str, intervalo_dias: int) -> list[tuple[str, str]]:
    """Split an inclusive date range into sequential blocks."""
    if intervalo_dias < 1:
        raise ValueError("intervalo_dias precisa ser >= 1.")

    dt_inicio = datetime.strptime(data_inicio, fmt)
    dt_fim = datetime.strptime(data_fim, fmt)
    if dt_inicio > dt_fim:
        raise ValueError("data_inicio nao pode ser maior que data_fim.")

    blocos: list[tuple[str, str]] = []
    current_start = dt_inicio
    while current_start <= dt_fim:
        current_end = min(current_start + timedelta(days=intervalo_dias - 1), dt_fim)
        blocos.append((current_start.strftime(fmt), current_end.strftime(fmt)))
        current_start = current_end + timedelta(days=1)
    return blocos


def executar_meta_ads_reprocess(data_inicio: str, data_fim: str, contas: list[str] | None = None) -> pl.DataFrame:
    """Extract Meta data in seven-day windows and persist a single consolidated file."""
    target_accounts = contas or META_ACCOUNTS
    dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
    dt_fim = datetime.strptime(data_fim, "%Y-%m-%d")
    resultados: list[pl.DataFrame] = []

    for conta_id in target_accounts:
        print(f"[Meta] Processando conta {conta_id} de {data_inicio} ate {data_fim}")
        current_start = dt_inicio
        blocos_conta: list[pl.DataFrame] = []

        while current_start <= dt_fim:
            current_end = min(current_start + timedelta(days=6), dt_fim)
            start_str = current_start.strftime("%Y-%m-%d")
            end_str = current_end.strftime("%Y-%m-%d")
            print(f"[Meta] Conta {conta_id}: bloco {start_str} a {end_str}")
            blocos_conta.append(obter_dados_meta_ads(conta_id, start_str, end_str))
            current_start = current_end + timedelta(days=1)

        df_conta = align_and_concat_frames(blocos_conta)
        if df_conta.height == 0:
            print(f"[Meta] Nenhum dado coletado para a conta {conta_id}.")
            continue

        resultados.append(df_conta)

    df_final = align_and_concat_frames(resultados)
    if df_final.height > 0:
        inserir_meta_ads(df_final, str(META_OUTPUT_FILE))
    return df_final


def executar_tiktok_reprocess(data_inicio: str, data_fim: str, baixar_midias: bool = False) -> pl.DataFrame:
    """Extract TikTok data in rolling seven-day windows for a reprocessing interval."""
    dt_inicio = datetime.strptime(data_inicio, "%Y%m%d")
    dt_fim = datetime.strptime(data_fim, "%Y%m%d")
    resultados: list[pl.DataFrame] = []
    current_start = dt_inicio

    while current_start < dt_fim:
        current_end = min(current_start + timedelta(days=7), dt_fim)
        start_str = current_start.strftime("%Y%m%d")
        end_str = current_end.strftime("%Y%m%d")
        print(f"[TikTok] Processando bloco {start_str} a {end_str}")
        df = executar_tiktok(start_str, end_str, baixar_midias)
        if df.height > 0:
            resultados.append(df)
        current_start = current_end

    return align_and_concat_frames(resultados)


def executar_kwai_reprocess(data_inicio: str, data_fim: str) -> pl.DataFrame:
    """Extract Kwai data in rolling seven-day windows for a reprocessing interval."""
    dt_inicio = datetime.strptime(data_inicio, "%Y%m%d")
    dt_fim = datetime.strptime(data_fim, "%Y%m%d")
    resultados: list[pl.DataFrame] = []
    current_start = dt_inicio

    while current_start <= dt_fim:
        current_end = min(current_start + timedelta(days=6), dt_fim)
        start_str = current_start.strftime("%Y%m%d")
        end_str = current_end.strftime("%Y%m%d")
        print(f"[Kwai] Processando bloco {start_str} a {end_str}")
        df = executar_kwai(start_str, end_str, contas=KWAI_ACCOUNTS)
        if df.height > 0:
            resultados.append(df)
        current_start = current_end + timedelta(days=1)

    return align_and_concat_frames(resultados)


def executar_meta_ads_diario_em_blocos(
    data_inicio: str,
    data_fim: str,
    contas: list[str] | None = None,
    block_days: int = DAILY_BLOCK_DAYS,
    block_sleep_seconds: int = DAILY_BLOCK_SLEEP_SECONDS,
) -> None:
    """Run the daily Meta refresh in seven-day blocks across every product/account."""
    target_accounts = contas or META_ACCOUNTS
    blocos = iterar_blocos_datas(data_inicio, data_fim, "%Y-%m-%d", block_days)

    print(f"[Meta] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[Meta] Contas selecionadas: {target_accounts}")
    print(f"[Meta] Quantidade de blocos: {len(blocos)}")

    for indice_bloco, (start_str, end_str) in enumerate(blocos, start=1):
        print(f"[Meta] Iniciando bloco {indice_bloco}/{len(blocos)}: {start_str} a {end_str}")
        for conta_id in target_accounts:
            print(f"[Meta] Conta {conta_id}: bloco {start_str} a {end_str}")
            df = obter_dados_meta_ads(conta_id, start_str, end_str)
            if df.height > 0:
                inserir_meta_ads(df, str(META_OUTPUT_FILE))
                print(f"[Meta] Bloco salvo para a conta {conta_id}. Shape: {df.shape}")
            else:
                print(f"[Meta] Bloco sem dados para a conta {conta_id}.")

        if block_sleep_seconds > 0 and indice_bloco < len(blocos):
            print(f"[Meta] Aguardando {block_sleep_seconds}s antes do proximo bloco.")
            time.sleep(block_sleep_seconds)


def executar_tiktok_diario_em_blocos(
    data_inicio: str,
    data_fim: str,
    baixar_midias: bool = False,
    block_days: int = DAILY_BLOCK_DAYS,
    block_sleep_seconds: int = DAILY_BLOCK_SLEEP_SECONDS,
) -> None:
    """Run the daily TikTok refresh in seven-day blocks across every product/advertiser."""
    blocos = iterar_blocos_datas(data_inicio, data_fim, "%Y-%m-%d", block_days)
    advertisers = TIKTOK_ACCOUNTS
    config = carregar_config_tiktok(CONFIG_TIKTOK_PATH)

    print(f"[TikTok] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[TikTok] Advertisers selecionados: {advertisers}")
    print(f"[TikTok] Quantidade de blocos: {len(blocos)}")

    for indice_bloco, (start_str, end_str) in enumerate(blocos, start=1):
        start_fmt = start_str.replace("-", "")
        end_fmt = end_str.replace("-", "")
        print(f"[TikTok] Iniciando bloco {indice_bloco}/{len(blocos)}: {start_fmt} a {end_fmt}")
        for advertiser_id in advertisers:
            print(f"[TikTok] Advertiser {advertiser_id}: bloco {start_fmt} a {end_fmt}")
            df = obter_dados_tiktok(advertiser_id, start_fmt, end_fmt, config)
            if df.height > 0:
                inserir_tiktok(df, TIKTOK_OUTPUT_FILE)
                print(f"[TikTok] Bloco salvo para o advertiser {advertiser_id}. Shape: {df.shape}")
            else:
                print(f"[TikTok] Bloco sem dados para o advertiser {advertiser_id}.")

        if block_sleep_seconds > 0 and indice_bloco < len(blocos):
            print(f"[TikTok] Aguardando {block_sleep_seconds}s antes do proximo bloco.")
            time.sleep(block_sleep_seconds)

    if baixar_midias:
        print("[TikTok] Iniciando download de assets apos concluir os blocos diarios.")
        for advertiser_id in advertisers:
            baixar_assets_tiktok(advertiser_id, config)


def executar_kwai_diario_em_blocos(
    data_inicio: str,
    data_fim: str,
    block_days: int = DAILY_BLOCK_DAYS,
    block_sleep_seconds: int = DAILY_BLOCK_SLEEP_SECONDS,
) -> None:
    """Run the daily Kwai refresh in seven-day blocks across every configured account."""
    blocos = iterar_blocos_datas(data_inicio, data_fim, "%Y-%m-%d", block_days)

    print(f"[Kwai] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[Kwai] Accounts selecionados: {KWAI_ACCOUNTS}")
    print(f"[Kwai] Quantidade de blocos: {len(blocos)}")

    for indice_bloco, (start_str, end_str) in enumerate(blocos, start=1):
        start_fmt = start_str.replace("-", "")
        end_fmt = end_str.replace("-", "")
        print(f"[Kwai] Iniciando bloco {indice_bloco}/{len(blocos)}: {start_fmt} a {end_fmt}")
        executar_kwai(start_fmt, end_fmt, contas=KWAI_ACCOUNTS)

        if block_sleep_seconds > 0 and indice_bloco < len(blocos):
            print(f"[Kwai] Aguardando {block_sleep_seconds}s antes do proximo bloco.")
            time.sleep(block_sleep_seconds)


def parse_channels_arg(channels_arg: str | None) -> list[str]:
    """Validate the requested channels and return the active extraction list."""
    channels = [item.strip().lower() for item in (channels_arg or "meta,google,tiktok,kwai").split(",") if item.strip()]
    valid_channels = {"google", "tiktok", "meta", "kwai"}
    unknown_channels = [channel for channel in channels if channel not in valid_channels]
    if unknown_channels:
        print(f"Canais ignorados por nao serem suportados: {unknown_channels}")
    return [channel for channel in channels if channel in valid_channels]


def executar_extracoes_diarias(hoje: datetime, channels: list[str], baixar_midias: bool) -> None:
    """Run the daily incremental windows for the selected ad platforms."""
    sucessos: list[str] = []
    falhas: list[str] = []

    if "meta" in channels:
        data_fim_meta = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
        data_inicio_meta = (hoje - timedelta(days=DAILY_META_TIKTOK_WINDOW_DAYS)).strftime("%Y-%m-%d")
        _executar_com_isolamento(
            "Meta diario",
            sucessos,
            falhas,
            lambda: (
                init_facebook_api(),
                print(f"[Meta] Processando dados de {data_inicio_meta} ate {data_fim_meta}"),
                executar_meta_ads_diario_em_blocos(data_inicio_meta, data_fim_meta, contas=META_ACCOUNTS),
            ),
        )

    if "google" in channels:
        data_fim_google = (hoje - timedelta(days=1)).strftime("%Y%m%d")
        data_inicio_google = (hoje - timedelta(days=90)).strftime("%Y%m%d")
        data_inicio_conv = (hoje - timedelta(days=180)).strftime("%Y%m%d")
        _executar_com_isolamento(
            "Google diario",
            sucessos,
            falhas,
            lambda: (
                print(f"[Google] Processando dados de {data_inicio_google} ate {data_fim_google}"),
                executar_google_ads(
                    data_inicio_google,
                    data_fim_google,
                    baixar_midias=baixar_midias,
                    data_inicio_conversoes=data_inicio_conv,
                ),
            ),
        )

    if "tiktok" in channels:
        data_fim_tiktok = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
        data_inicio_tiktok = (hoje - timedelta(days=DAILY_META_TIKTOK_WINDOW_DAYS)).strftime("%Y-%m-%d")
        _executar_com_isolamento(
            "TikTok diario",
            sucessos,
            falhas,
            lambda: (
                print(f"[TikTok] Processando dados de {data_inicio_tiktok} ate {data_fim_tiktok}"),
                executar_tiktok_diario_em_blocos(
                    data_inicio_tiktok,
                    data_fim_tiktok,
                    baixar_midias=baixar_midias,
                ),
            ),
        )

    if "kwai" in channels:
        data_fim_kwai = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
        data_inicio_kwai = (hoje - timedelta(days=DAILY_KWAI_WINDOW_DAYS)).strftime("%Y-%m-%d")
        _executar_com_isolamento(
            "Kwai diario",
            sucessos,
            falhas,
            lambda: (
                print(f"[Kwai] Processando dados de {data_inicio_kwai} ate {data_fim_kwai}"),
                executar_kwai_diario_em_blocos(data_inicio_kwai, data_fim_kwai),
            ),
        )

    data_fim_alcance = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")
    data_inicio_alcance = (hoje - timedelta(days=WEEKLY_REACH_WINDOW_DAYS)).strftime("%Y-%m-%d")
    for channel in channels:
        _executar_com_isolamento(
            f"Alcance semanal {channel}",
            sucessos,
            falhas,
            lambda channel=channel: (
                init_facebook_api() if channel == "meta" else None,
                print(f"[Alcance semanal] Processando {channel} de {data_inicio_alcance} ate {data_fim_alcance}"),
                executar_alcance_semanal(data_inicio_alcance, data_fim_alcance, channels=[channel]),
            ),
        )

    _imprimir_resumo_job(sucessos, falhas)


def executar_reprocessamento(channels: list[str], start_date: str, end_date: str, baixar_midias: bool) -> None:
    """Run a controlled reprocessing window across the selected channels."""
    target_start_google = start_date.replace("-", "")
    target_end_google = end_date.replace("-", "")
    sucessos: list[str] = []
    falhas: list[str] = []

    print("===== Modo Reprocessamento =====")
    print(f"Periodo alvo: {start_date} a {end_date}")
    print(f"Canais: {channels}")

    if "meta" in channels:
        _executar_com_isolamento(
            "Meta reprocessamento",
            sucessos,
            falhas,
            lambda: (
                init_facebook_api(),
                print(f"[Meta] Reprocessando de {start_date} ate {end_date}"),
                executar_meta_ads_reprocess(start_date, end_date, contas=META_ACCOUNTS),
            ),
        )

    if "google" in channels:
        _executar_com_isolamento(
            "Google reprocessamento",
            sucessos,
            falhas,
            lambda: (
                print(f"[Google] Reprocessando de {target_start_google} ate {target_end_google}"),
                executar_google_ads(target_start_google, target_end_google, baixar_midias=baixar_midias),
            ),
        )

    if "tiktok" in channels:
        _executar_com_isolamento(
            "TikTok reprocessamento",
            sucessos,
            falhas,
            lambda: (
                print(f"[TikTok] Reprocessando de {target_start_google} ate {target_end_google}"),
                executar_tiktok_reprocess(target_start_google, target_end_google, baixar_midias=baixar_midias),
            ),
        )

    if "kwai" in channels:
        _executar_com_isolamento(
            "Kwai reprocessamento",
            sucessos,
            falhas,
            lambda: (
                print(f"[Kwai] Reprocessando de {target_start_google} ate {target_end_google}"),
                executar_kwai_reprocess(target_start_google, target_end_google),
            ),
        )

    for channel in channels:
        _executar_com_isolamento(
            f"Alcance semanal {channel}",
            sucessos,
            falhas,
            lambda channel=channel: (
                init_facebook_api() if channel == "meta" else None,
                print(f"[Alcance semanal] Reprocessando {channel} de {start_date} ate {end_date}"),
                executar_alcance_semanal(start_date, end_date, channels=[channel]),
            ),
        )

    _imprimir_resumo_job(sucessos, falhas)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line interface for the extraction runtime."""
    parser = argparse.ArgumentParser(description="Extracao diaria e reprocessamento das bases de Ads.")
    parser.add_argument("--reprocess", action="store_true", help="Executa reprocessamento para a janela informada.")
    parser.add_argument("--start_date", type=str, help="Data inicial no formato YYYY-MM-DD.")
    parser.add_argument("--end_date", type=str, help="Data final no formato YYYY-MM-DD.")
    parser.add_argument("--channels", type=str, help="Lista separada por virgula entre meta,google,tiktok,kwai.")
    parser.add_argument(
        "--download-assets",
        action="store_true",
        help="Baixa assets de Google e TikTok durante a extracao.",
    )
    return parser


def main() -> None:
    """Execute the active extraction runtime for the selected mode and channels."""
    args = build_parser().parse_args()
    channels = parse_channels_arg(args.channels)
    if not channels:
        raise SystemExit("Nenhum canal valido foi informado.")

    if any(not list_accounts(channel) for channel in channels):
        raise SystemExit("Configure accounts.yaml for every selected channel before collecting.")

    hoje = datetime.now(SAO_PAULO_TZ)
    if args.reprocess:
        if not args.start_date or not args.end_date:
            raise SystemExit("Informe --start_date e --end_date no formato YYYY-MM-DD para reprocessamento.")
        executar_reprocessamento(channels, args.start_date, args.end_date, baixar_midias=args.download_assets)
    else:
        executar_extracoes_diarias(hoje, channels, baixar_midias=args.download_assets)

    print("Processamento concluido.")


if __name__ == "__main__":
    main()
