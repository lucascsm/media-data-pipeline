from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from typing import TypeVar

import polars as pl
from facebook_business.api import FacebookAdsApi

from src.account_catalog import get_product_for_account, list_accounts
from src.alcance_semanal import executar_alcance_semanal
from src.google_ads import (
    GOOGLE_PERFORMANCE_OUTPUT_FILE,
    _get_raw_conversions,
    baixar_assets_midias,
    inserir_substituir as inserir_google,
    obter_dados_google_ads,
)
from src.kwai import executar_kwai
from src.metads import (
    META_OUTPUT_FILE,
    inserir_substituir as inserir_meta_ads,
    obter_dados_meta_ads,
)
from src.pipeline_utils import align_and_concat_frames, load_yaml_config
from src.tiktok import (
    TIKTOK_OUTPUT_FILE,
    baixar_assets_tiktok,
    carregar_config_tiktok,
    inserir_substituir as inserir_tiktok,
    obter_dados_tiktok,
)
from src.utils_do_pai import CONFIG_META_PATH, CONFIG_TIKTOK_PATH, OUTPUT_PATH


GOOGLE_ACCOUNTS = list_accounts("google")
META_ACCOUNTS = list_accounts("meta")
TIKTOK_ACCOUNTS = list_accounts("tiktok")
KWAI_ACCOUNTS = list_accounts("kwai")
GOOGLE_CONVERSIONS_OUTPUT_FILE = OUTPUT_PATH / "google_ads_conversions_all_new.parquet"
GOOGLE_CONVERSION_COLUMNS = [
    "nivel",
    "id_cliente",
    "produto",
    "data",
    "conversion_event",
    "conversoes",
    "conversoes_by_conversion_date",
    "todas_conversoes",
    "todas_conversions_by_conversion_date",
    "conversao_view",
    "valor_conversoes",
    "id_campanha",
    "nome_campanha",
    "id_grupo_anuncio",
    "nome_grupo_anuncio",
    "id_anuncio",
    "nome_anuncio",
]
T = TypeVar("T")


def _iterar_blocos(data_inicio: str, data_fim: str, intervalo_dias: int, fmt: str) -> list[tuple[str, str]]:
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


def _iterar_lotes(itens: list[T], blocks_per_batch: int) -> list[list[T]]:
    if blocks_per_batch < 1:
        raise ValueError("blocks_per_batch precisa ser >= 1.")
    return [itens[i:i + blocks_per_batch] for i in range(0, len(itens), blocks_per_batch)]


def _parse_channels(channels_arg: str | None) -> list[str]:
    aliases = {
        "google_pre": "google_pre_click",
        "google_pre_click": "google_pre_click",
        "google_preclick": "google_pre_click",
        "google_pos": "google_pos_click",
        "google_pos_click": "google_pos_click",
        "google_posclick": "google_pos_click",
        "google_post": "google_pos_click",
        "google_post_click": "google_pos_click",
        "google_postclick": "google_pos_click",
    }
    raw_channels = [
        item.strip().lower().replace("-", "_").replace(" ", "_").replace("clique", "click")
        for item in (channels_arg or "meta,tiktok").split(",")
        if item.strip()
    ]
    channels = [aliases.get(channel, channel) for channel in raw_channels]
    valid_channels = {"google", "google_pre_click", "google_pos_click", "meta", "tiktok", "kwai"}
    unknown_channels = [channel for channel in channels if channel not in valid_channels]
    if unknown_channels:
        print(f"Canais ignorados por nao serem suportados no reprocessamento dedicado: {unknown_channels}")
    return [channel for channel in channels if channel in valid_channels]


def init_facebook_api() -> None:
    secrets = load_yaml_config(CONFIG_META_PATH / "meta.yaml")
    FacebookAdsApi.init(
        app_id=secrets.get("app_id"),
        app_secret=secrets.get("app_secret"),
        access_token=secrets.get("access_token"),
    )


def _executar_com_isolamento(etapa: str, sucessos: list[str], falhas: list[str], func) -> bool:
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


def _preparar_google_pos_click(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col("id_cliente")
            .map_elements(lambda x: get_product_for_account(x, "google"), return_dtype=pl.Utf8)
            .alias("produto")
        )
        .select([column for column in GOOGLE_CONVERSION_COLUMNS if column in df.columns or column == "produto"])
    )


def executar_google_reprocess_em_blocos(
    data_inicio: str,
    data_fim: str,
    block_days: int,
    blocks_per_batch: int = 5,
    batch_sleep_seconds: int = 300,
    baixar_midias: bool = False,
    pre_click: bool = True,
    pos_click: bool = True,
) -> None:
    if not pre_click and not pos_click:
        raise ValueError("Informe pelo menos um lado do Google: pre_click ou pos_click.")

    blocos = _iterar_blocos(data_inicio, data_fim, block_days, "%Y-%m-%d")
    blocos_indexados = list(enumerate(blocos, start=1))
    lotes = _iterar_lotes(blocos_indexados, blocks_per_batch)
    lados = []
    if pre_click:
        lados.append("pre-click/performance")
    if pos_click:
        lados.append("pos-click/conversions")

    print(f"[Google] Contas selecionadas: {GOOGLE_ACCOUNTS}")
    print(f"[Google] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[Google] Lados: {', '.join(lados)}")
    print(f"[Google] Quantidade de blocos por conta: {len(blocos)}")
    print(f"[Google] Quantidade de lotes por conta: {len(lotes)}")

    for customer_id in GOOGLE_ACCOUNTS:
        print(f"[Google] Iniciando customer {customer_id}")
        for lote_indice, lote_blocos in enumerate(lotes, start=1):
            print(
                f"[Google] Customer {customer_id}: lote {lote_indice}/{len(lotes)} "
                f"com {len(lote_blocos)} bloco(s)."
            )
            for indice_global, (bloco_inicio, bloco_fim) in lote_blocos:
                inicio_fmt = bloco_inicio.replace("-", "")
                fim_fmt = bloco_fim.replace("-", "")
                print(
                    f"[Google] Customer {customer_id}: bloco {indice_global}/{len(blocos)} "
                    f"de {inicio_fmt} ate {fim_fmt}"
                )

                if pre_click:
                    df_perf = obter_dados_google_ads(customer_id, inicio_fmt, fim_fmt)
                    if df_perf.height > 0:
                        inserir_google(df_perf, str(GOOGLE_PERFORMANCE_OUTPUT_FILE))
                        print(f"[Google] Pre-click salvo com shape {df_perf.shape}")
                    else:
                        print(f"[Google] Pre-click sem dados para o customer {customer_id}.")

                if pos_click:
                    frames_conv = [
                        _get_raw_conversions(customer_id, bloco_inicio, bloco_fim, nivel)
                        for nivel in ("campanha", "grupo_anuncio", "anuncio")
                    ]
                    df_conv = align_and_concat_frames(frames_conv)
                    if df_conv.height > 0:
                        df_conv = _preparar_google_pos_click(df_conv)
                        inserir_google(df_conv, str(GOOGLE_CONVERSIONS_OUTPUT_FILE))
                        print(f"[Google] Pos-click salvo com shape {df_conv.shape}")
                    else:
                        print(f"[Google] Pos-click sem dados para o customer {customer_id}.")

            if batch_sleep_seconds > 0 and lote_indice < len(lotes):
                print(f"[Google] Aguardando {batch_sleep_seconds}s antes do proximo lote.")
                time.sleep(batch_sleep_seconds)

        if baixar_midias:
            print(f"[Google] Baixando assets para o customer {customer_id}.")
            baixar_assets_midias(customer_id)


def executar_meta_ads_reprocess_em_blocos(
    data_inicio: str,
    data_fim: str,
    block_days: int,
    blocks_per_batch: int = 5,
    batch_sleep_seconds: int = 300,
) -> None:
    blocos = _iterar_blocos(data_inicio, data_fim, block_days, "%Y-%m-%d")
    blocos_indexados = list(enumerate(blocos, start=1))
    lotes = _iterar_lotes(blocos_indexados, blocks_per_batch)
    print(f"[Meta] Contas selecionadas: {META_ACCOUNTS}")
    print(f"[Meta] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[Meta] Quantidade de blocos por conta: {len(blocos)}")
    print(f"[Meta] Quantidade de lotes por conta: {len(lotes)}")

    for conta_id in META_ACCOUNTS:
        print(f"[Meta] Iniciando conta {conta_id}")
        for lote_indice, lote_blocos in enumerate(lotes, start=1):
            print(
                f"[Meta] Conta {conta_id}: lote {lote_indice}/{len(lotes)} "
                f"com {len(lote_blocos)} bloco(s)."
            )
            for indice_global, (bloco_inicio, bloco_fim) in lote_blocos:
                print(
                    f"[Meta] Conta {conta_id}: bloco {indice_global}/{len(blocos)} "
                    f"de {bloco_inicio} ate {bloco_fim}"
                )
                df = obter_dados_meta_ads(conta_id, bloco_inicio, bloco_fim)
                if df.height > 0:
                    inserir_meta_ads(df, META_OUTPUT_FILE)
                    print(f"[Meta] Bloco salvo com shape {df.shape}")
                else:
                    print(f"[Meta] Bloco sem dados para a conta {conta_id}.")

            if batch_sleep_seconds > 0 and lote_indice < len(lotes):
                print(f"[Meta] Aguardando {batch_sleep_seconds}s antes do proximo lote.")
                time.sleep(batch_sleep_seconds)


def executar_tiktok_reprocess_em_blocos(
    data_inicio: str,
    data_fim: str,
    block_days: int,
    blocks_per_batch: int = 5,
    batch_sleep_seconds: int = 300,
    baixar_midias: bool = False,
) -> None:
    blocos = _iterar_blocos(data_inicio, data_fim, block_days, "%Y-%m-%d")
    blocos_indexados = list(enumerate(blocos, start=1))
    lotes = _iterar_lotes(blocos_indexados, blocks_per_batch)
    config = carregar_config_tiktok(CONFIG_TIKTOK_PATH)
    print(f"[TikTok] Advertisers selecionados: {TIKTOK_ACCOUNTS}")
    print(f"[TikTok] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[TikTok] Quantidade de blocos por advertiser: {len(blocos)}")
    print(f"[TikTok] Quantidade de lotes por advertiser: {len(lotes)}")

    for advertiser_id in TIKTOK_ACCOUNTS:
        print(f"[TikTok] Iniciando advertiser {advertiser_id}")
        for lote_indice, lote_blocos in enumerate(lotes, start=1):
            print(
                f"[TikTok] Advertiser {advertiser_id}: lote {lote_indice}/{len(lotes)} "
                f"com {len(lote_blocos)} bloco(s)."
            )
            for indice_global, (bloco_inicio, bloco_fim) in lote_blocos:
                inicio_fmt = bloco_inicio.replace("-", "")
                fim_fmt = bloco_fim.replace("-", "")
                print(
                    f"[TikTok] Advertiser {advertiser_id}: bloco {indice_global}/{len(blocos)} "
                    f"de {inicio_fmt} ate {fim_fmt}"
                )
                df = obter_dados_tiktok(advertiser_id, inicio_fmt, fim_fmt, config)
                if df.height > 0:
                    inserir_tiktok(df, TIKTOK_OUTPUT_FILE)
                    print(f"[TikTok] Bloco salvo com shape {df.shape}")
                else:
                    print(f"[TikTok] Bloco sem dados para o advertiser {advertiser_id}.")

            if batch_sleep_seconds > 0 and lote_indice < len(lotes):
                print(f"[TikTok] Aguardando {batch_sleep_seconds}s antes do proximo lote.")
                time.sleep(batch_sleep_seconds)

        if baixar_midias:
            print(f"[TikTok] Baixando assets para o advertiser {advertiser_id}.")
            baixar_assets_tiktok(advertiser_id, config)


def executar_kwai_reprocess_em_blocos(
    data_inicio: str,
    data_fim: str,
    block_days: int,
    blocks_per_batch: int = 5,
    batch_sleep_seconds: int = 300,
) -> None:
    blocos = _iterar_blocos(data_inicio, data_fim, block_days, "%Y-%m-%d")
    blocos_indexados = list(enumerate(blocos, start=1))
    lotes = _iterar_lotes(blocos_indexados, blocks_per_batch)
    print(f"[Kwai] Accounts selecionados: {KWAI_ACCOUNTS}")
    print(f"[Kwai] Periodo total: {data_inicio} ate {data_fim}")
    print(f"[Kwai] Quantidade de blocos: {len(blocos)}")
    print(f"[Kwai] Quantidade de lotes: {len(lotes)}")

    for lote_indice, lote_blocos in enumerate(lotes, start=1):
        print(f"[Kwai] Lote {lote_indice}/{len(lotes)} com {len(lote_blocos)} bloco(s).")
        for indice_global, (bloco_inicio, bloco_fim) in lote_blocos:
            inicio_fmt = bloco_inicio.replace("-", "")
            fim_fmt = bloco_fim.replace("-", "")
            print(f"[Kwai] Bloco {indice_global}/{len(blocos)} de {inicio_fmt} ate {fim_fmt}")
            df = executar_kwai(inicio_fmt, fim_fmt, contas=KWAI_ACCOUNTS)
            if df.height > 0:
                print(f"[Kwai] Bloco salvo com shape {df.shape}")
            else:
                print("[Kwai] Bloco sem dados.")

        if batch_sleep_seconds > 0 and lote_indice < len(lotes):
            print(f"[Kwai] Aguardando {batch_sleep_seconds}s antes do proximo lote.")
            time.sleep(batch_sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reprocessamento dedicado de Google, Meta, TikTok e Kwai em blocos.")
    parser.add_argument("--start_date", required=True, type=str, help="Data inicial no formato YYYY-MM-DD.")
    parser.add_argument("--end_date", required=True, type=str, help="Data final no formato YYYY-MM-DD.")
    parser.add_argument(
        "--channels",
        type=str,
        help=(
            "Lista separada por virgula entre google,google_pre_click,"
            "google_pos_click,meta,tiktok,kwai."
        ),
    )
    parser.add_argument("--google-block-days", type=int, default=7, help="Tamanho do bloco do Google em dias.")
    parser.add_argument("--meta-block-days", type=int, default=7, help="Tamanho do bloco do Meta em dias.")
    parser.add_argument("--tiktok-block-days", type=int, default=7, help="Tamanho do bloco do TikTok em dias.")
    parser.add_argument("--kwai-block-days", type=int, default=7, help="Tamanho do bloco do Kwai em dias.")
    parser.add_argument(
        "--blocks-per-batch",
        type=int,
        default=5,
        help="Quantidade de blocos executados antes da pausa.",
    )
    parser.add_argument(
        "--batch-sleep-seconds",
        "--block-sleep-seconds",
        dest="batch_sleep_seconds",
        type=int,
        default=300,
        help="Pausa entre lotes de blocos do mesmo canal e conta/advertiser.",
    )
    parser.add_argument(
        "--download-assets",
        action="store_true",
        help="Baixa assets de Google e TikTok apos concluir os blocos.",
    )
    parser.add_argument(
        "--reach-only",
        "--reach_only",
        dest="reach_only",
        action="store_true",
        help="Executa somente a coleta de alcance/frequencia semanal.",
    )
    return parser


def _executar_alcance_semanal_canais(
    channels: list[str],
    start_date: str,
    end_date: str,
    sucessos: list[str],
    falhas: list[str],
    meta_initialized: bool,
) -> None:
    reach_channels = sorted(
        {
            "google" if channel.startswith("google") else channel
            for channel in channels
            if channel.startswith("google") or channel in {"meta", "tiktok", "kwai"}
        }
    )
    for channel in reach_channels:
        def _run_reach(channel=channel) -> None:
            nonlocal meta_initialized
            if channel == "meta" and not meta_initialized:
                init_facebook_api()
                meta_initialized = True
            print(f"[Alcance semanal] Reprocessando {channel} de {start_date} ate {end_date}")
            executar_alcance_semanal(start_date, end_date, channels=[channel])

        _executar_com_isolamento(
            f"Alcance semanal {channel}",
            sucessos,
            falhas,
            _run_reach,
        )


def main() -> None:
    args = build_parser().parse_args()
    channels = _parse_channels(args.channels)
    if not channels:
        raise SystemExit("Nenhum canal valido foi informado para o reprocessamento dedicado.")
    sucessos: list[str] = []
    falhas: list[str] = []

    if args.reach_only:
        print("===== Modo Alcance Semanal =====")
    else:
        print("===== Modo Reprocessamento Dedicado =====")
    print(f"Periodo alvo: {args.start_date} a {args.end_date}")
    print(f"Canais: {channels}")
    if not args.reach_only:
        print(f"Google block days: {args.google_block_days}")
        print(f"Meta block days: {args.meta_block_days}")
        print(f"TikTok block days: {args.tiktok_block_days}")
        print(f"Kwai block days: {args.kwai_block_days}")
        print(f"Blocks per batch: {args.blocks_per_batch}")
        print(f"Sleep entre lotes: {args.batch_sleep_seconds}s")

    meta_initialized = False
    if args.reach_only:
        _executar_alcance_semanal_canais(
            channels,
            args.start_date,
            args.end_date,
            sucessos,
            falhas,
            meta_initialized,
        )
        _imprimir_resumo_job(sucessos, falhas)
        print("Reprocessamento de alcance semanal concluido.")
        return

    for channel in channels:
        print(f"[Runner] Iniciando canal {channel}")
        if channel == "google":
            _executar_com_isolamento(
                "Google reprocessamento dedicado",
                sucessos,
                falhas,
                lambda: executar_google_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.google_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                    baixar_midias=args.download_assets,
                    pre_click=True,
                    pos_click=True,
                ),
            )
        elif channel == "google_pre_click":
            _executar_com_isolamento(
                "Google pre-click reprocessamento dedicado",
                sucessos,
                falhas,
                lambda: executar_google_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.google_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                    baixar_midias=args.download_assets,
                    pre_click=True,
                    pos_click=False,
                ),
            )
        elif channel == "google_pos_click":
            _executar_com_isolamento(
                "Google pos-click reprocessamento dedicado",
                sucessos,
                falhas,
                lambda: executar_google_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.google_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                    baixar_midias=False,
                    pre_click=False,
                    pos_click=True,
                ),
            )
        elif channel == "meta":
            def _run_meta() -> None:
                nonlocal meta_initialized
                if not meta_initialized:
                    init_facebook_api()
                    meta_initialized = True
                executar_meta_ads_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.meta_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                )

            _executar_com_isolamento(
                "Meta reprocessamento dedicado",
                sucessos,
                falhas,
                _run_meta,
            )
        elif channel == "tiktok":
            _executar_com_isolamento(
                "TikTok reprocessamento dedicado",
                sucessos,
                falhas,
                lambda: executar_tiktok_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.tiktok_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                    baixar_midias=args.download_assets,
                ),
            )
        elif channel == "kwai":
            _executar_com_isolamento(
                "Kwai reprocessamento dedicado",
                sucessos,
                falhas,
                lambda: executar_kwai_reprocess_em_blocos(
                    data_inicio=args.start_date,
                    data_fim=args.end_date,
                    block_days=args.kwai_block_days,
                    blocks_per_batch=args.blocks_per_batch,
                    batch_sleep_seconds=args.batch_sleep_seconds,
                ),
            )

    _executar_alcance_semanal_canais(
        channels,
        args.start_date,
        args.end_date,
        sucessos,
        falhas,
        meta_initialized,
    )

    _imprimir_resumo_job(sucessos, falhas)
    print("Reprocessamento dedicado concluido.")


if __name__ == "__main__":
    main()
