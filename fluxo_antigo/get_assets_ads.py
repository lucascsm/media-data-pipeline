from src.legacy_config import load_legacy_settings
from pathlib import Path
import os
import csv
import time
import random
import pandas as pd
import yaml

from google.ads.googleads.client import GoogleAdsClient
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

from datetime import datetime, timedelta

# ----------------------------------------------------------------------
# CONFIGURAÇÃO DE CAMINHOS
# ----------------------------------------------------------------------
from src.utils_do_pai import GCS_MOUNT
OUTPUT_PATH = GCS_MOUNT / "output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

CREATIVES_CSV         = OUTPUT_PATH / "creatives_final.csv"
RESUMO_FILE           = OUTPUT_PATH / "resumo_diario_powerbi.csv"
ANUNCIOS_IMAGENS_FILE = OUTPUT_PATH / "anuncios_imagens.csv"

# Importa configurações e contas
from src.utils_do_pai import CONFIG_GOOGLE_PATH, CONFIG_META_PATH
from src.account_catalog import list_accounts

# URL padrão para anúncios sem imagem
DEFAULT_URL = load_legacy_settings()["default_asset_url"]
META_PAGE_LIMIT = 200
CUSTOMER_IDS_GOOGLE = list_accounts("google")
META_ACCOUNTS = list_accounts("meta")


def _is_meta_rate_limit_error(exc: FacebookRequestError) -> bool:
    msg = (exc.api_error_message() or "").lower()
    code = exc.api_error_code() if hasattr(exc, "api_error_code") else None
    subcode = exc.api_error_subcode() if hasattr(exc, "api_error_subcode") else None
    retryable_codes = {1, 2, 4, 17, 32, 613, 80000, 80004, 80014, 80015, 80016}
    return (
        "too many calls" in msg
        or "rate limiting" in msg
        or (code in retryable_codes)
        or (subcode in retryable_codes)
    )

def _is_meta_reduce_data_error(exc: FacebookRequestError) -> bool:
    msg = (exc.api_error_message() or "").lower()
    return "reduce the amount of data" in msg


def _run_meta_with_retry(api_call, max_retries=5, initial_delay=60, max_delay=600):
    delay = initial_delay
    retries = 0
    iterator = iter(api_call())
    results = []
    while True:
        try:
            results.append(next(iterator))
            if retries:
                retries = 0
                delay = initial_delay
        except StopIteration:
            return results
        except FacebookRequestError as e:
            if _is_meta_reduce_data_error(e):
                raise
            if not _is_meta_rate_limit_error(e):
                raise
            retries += 1
            wait_seconds = min(delay, max_delay) + random.randint(0, 20)
            msg = e.api_error_message() or "rate limit"
            print(
                f"   Rate limit Meta (tentativa {retries}/{max_retries}): {msg}. "
                f"Aguardando {wait_seconds} segundos."
            )
            time.sleep(wait_seconds)
            delay = min(delay * 2, max_delay)
            if retries >= max_retries:
                raise

def _run_meta_with_limit_backoff(api_call_factory, initial_limit, min_limit=25, max_attempts=4):
    limit = initial_limit
    attempts = 0
    while True:
        try:
            return _run_meta_with_retry(lambda: api_call_factory(limit))
        except FacebookRequestError as e:
            if not _is_meta_reduce_data_error(e):
                raise
            if limit <= min_limit or attempts >= max_attempts:
                raise
            limit = max(min_limit, limit // 2)
            attempts += 1
            print(f"   Meta payload alto; reduzindo limit para {limit} e tentando novamente.")


def get_google_ads_creatives(data_inicio: str, data_fim: str) -> pd.DataFrame:
    client  = GoogleAdsClient.load_from_storage(CONFIG_GOOGLE_PATH)
    service = client.get_service("GoogleAdsService")
    results = []

    for customer_id in CUSTOMER_IDS_GOOGLE:
        print(f" Google Ads customer: {customer_id}")
        query = f"""
            SELECT
              ad_group_ad.ad.id,
              ad_group_ad.ad.name, 
              ad_group_ad.ad.type,
              ad_group_ad.ad.image_ad.image_url,
              ad_group_ad.ad.app_engagement_ad.images,
              ad_group_ad.ad.app_ad.images,
              ad_group_ad.ad.responsive_display_ad.marketing_images,
              ad_group_ad.ad.responsive_display_ad.square_marketing_images
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{data_inicio}' AND '{data_fim}'
              AND metrics.impressions > 1
        """
        response = service.search(customer_id=customer_id, query=query)

        direct = []
        assets = {} 

        for row in response:
            ad      = row.ad_group_ad.ad
            ad_id   = ad.id
            ad_name = ad.name
            ad_type = ad.type_.name
            url     = None

            if ad_type == "IMAGE_AD" and ad.image_ad:
                url = ad.image_ad.image_url
            elif ad_type in ("APP_AD", "APP_ENGAGEMENT_AD"):
                imgs = getattr(ad, ad_type.lower()).images or []
                if imgs:
                    assets[ad_id] = (imgs[0].asset, ad_name)
            elif ad_type == "RESPONSIVE_DISPLAY_AD" and ad.responsive_display_ad:
                imgs = ad.responsive_display_ad.marketing_images or ad.responsive_display_ad.square_marketing_images or []
                if imgs:
                    assets[ad_id] = (imgs[0].asset, ad_name)

            if url:
                direct.append({
                    "platform":    "Google Ads",
                    "customer_id": customer_id,
                    "ad_id":       ad_id,
                    "nome_anuncio": ad_name,
                    "final_url":   url
                })

        if assets:
            names = {v[0] for v in assets.values()}
            in_clause = ", ".join(f'"{n}"' for n in names)
            q_assets = f"""
                SELECT
                  asset.resource_name,
                  asset.image_asset.full_size.url
                FROM asset
                WHERE asset.resource_name IN ({in_clause})
            """
            resp_assets = service.search(customer_id=customer_id, query=q_assets)
            asset_urls = {
                row.asset.resource_name: row.asset.image_asset.full_size.url
                for row in resp_assets
                if row.asset.image_asset and row.asset.image_asset.full_size
            }
            for ad_id, (res, ad_name) in assets.items():
                url = asset_urls.get(res)
                if url:
                    direct.append({
                        "platform":    "Google Ads",
                        "customer_id": customer_id,
                        "ad_id":       ad_id,
                        "nome_anuncio": ad_name,
                        "final_url":   url
                    })

        results.extend([r for r in direct if r["final_url"]])

    return pd.DataFrame(results)

def get_meta_ads_with_images() -> pd.DataFrame:
    conf_path = CONFIG_META_PATH / "meta.yaml"
    with open(conf_path, "r") as f:
        meta_cfg = yaml.safe_load(f)
    token = meta_cfg.get("access_token")
    FacebookAdsApi.init(access_token=token)

    creative_map = {}
    for account_id in META_ACCOUNTS:
        print(f" Meta account (criativos): {account_id}")
        try:
            account = AdAccount(account_id)
            imgs = _run_meta_with_limit_backoff(
                lambda limit: account.get_ad_images(
                    fields=["hash", "permalink_url"],
                    params={"limit": limit},
                ),
                META_PAGE_LIMIT,
            )
            hash2url = {i["hash"]: i["permalink_url"] for i in imgs}
            creatives = _run_meta_with_limit_backoff(
                lambda limit: account.get_ad_creatives(
                    fields=["id", "image_hash", "object_story_spec"],
                    params={"limit": limit},
                ),
                META_PAGE_LIMIT,
            )
            for cr in creatives:
                cid = cr.get("id")
                hashes = {cr.get("image_hash")}
                spec   = cr.get("object_story_spec", {})
                ld     = spec.get("link_data", {})
                pd_data= spec.get("photo_data", {})
                if ld.get("image_hash"):       hashes.add(ld["image_hash"])
                if pd_data.get("image_hash"): hashes.add(pd_data["image_hash"])
                for child in ld.get("child_attachments", []):
                    if child.get("image_hash"):
                        hashes.add(child["image_hash"])
                for h in hashes:
                    url = hash2url.get(h)
                    if url:
                        creative_map[cid] = url
        except FacebookRequestError as e:
            print(f"   Erro em {account_id}: {e.api_error_message()}")

    out = []
    for account_id in META_ACCOUNTS:
        print(f" Meta account (ads): {account_id}")
        try:
            # ALTERAÇÃO: Adicionado 'name' para buscar o nome do anúncio
            account = AdAccount(account_id)
            ads = _run_meta_with_limit_backoff(
                lambda limit: account.get_ads(
                    fields=["id", "name", "creative"],
                    params={"limit": limit},
                ),
                META_PAGE_LIMIT,
            )
            for ad in ads:
                aid = ad.get("id")
                ad_name = ad.get("name") # ALTERAÇÃO: Captura o nome do anúncio
                cr  = ad.get("creative", {}).get("id")
                url = creative_map.get(cr)
                if url:
                    # ALTERAÇÃO: Adicionado 'nome_anuncio' ao dicionário
                    out.append({
                        "platform":     "Meta",
                        "account_id":   account_id,
                        "ad_id":        aid,
                        "nome_anuncio": ad_name,
                        "final_url":    url
                    })
        except FacebookRequestError as e:
            print(f"   Erro em {account_id}: {e.api_error_message()}" )

    df = pd.DataFrame(out)
    if "account_id" in df.columns:
        df.rename(columns={"account_id": "customer_id"}, inplace=True)
    return df


def get_assets_ads(data_inicio: str, data_fim: str, plataforma: str = "all") -> pd.DataFrame:
    google_df = pd.DataFrame()
    meta_df = pd.DataFrame()

    if plataforma in ("all", "google"):
        print("\n--- INICIANDO COLETA DO GOOGLE ADS ---\n")
        google_df = get_google_ads_creatives(data_inicio, data_fim)
    
    if plataforma in ("all", "meta"):
        print("\n--- INICIANDO COLETA DA META ADS ---\n")
        meta_df = get_meta_ads_with_images()

    return pd.concat([google_df, meta_df], ignore_index=True)


def append_and_deduplicate(new_df: pd.DataFrame, output_file: Path) -> None:
    if output_file.exists():
        hist = pd.read_csv(output_file, dtype={"ad_id": "string"}, low_memory=False)
        df   = pd.concat([hist, new_df], ignore_index=True)
    else:
        df = new_df.copy()

    df.drop_duplicates(subset=["platform","ad_id","final_url"], inplace=True)
    df.to_csv(output_file, index=False)
    print(f"CSV salvo: {output_file}")


def join_creatives_with_resumo() -> None:
    # Lê os arquivos de performance e de criativos
    df_res = pd.read_csv(RESUMO_FILE, dtype={"id_anuncio": "string"}, low_memory=False)
    df_cre = pd.read_csv(CREATIVES_CSV, dtype={"ad_id": "string"}, low_memory=False)

    print(f"Linhas em df_res (performance) antes da limpeza: {len(df_res)}")
    print(f"Linhas em df_cre (criativos) antes da limpeza: {len(df_cre)}")

    # Filtra para pegar apenas os dados a nível de "anuncio"
    df_res = df_res[df_res['nivel'] == "anuncio"]

    # --- INÍCIO DA LIMPEZA E PREPARAÇÃO ---
    df_res.dropna(subset=['id_anuncio'], inplace=True)
    df_cre.dropna(subset=['ad_id'], inplace=True)

    print(f"Linhas em df_res após remover nulos: {len(df_res)}")
    print(f"Linhas em df_cre após remover nulos: {len(df_cre)}")
    
    if df_res.empty or df_cre.empty:
        print("Um dos DataFrames ficou vazio após a limpeza dos IDs. Não é possível fazer o merge.")
        df_res['final_url'] = DEFAULT_URL
        df_res['nome_anuncio'] = "Nome não encontrado"
        df_res.to_csv(ANUNCIOS_IMAGENS_FILE, index=False)
        print(f"Arquivo de anúncios salvo (sem merges).")
        return

    df_res['id_anuncio'] = df_res['id_anuncio'].str.strip()
    df_cre['ad_id'] = df_cre['ad_id'].str.strip()

    # ALTERAÇÃO: Remove a coluna 'nome_anuncio' de df_res (se existir) para evitar duplicidade
    if 'nome_anuncio' in df_res.columns:
        df_res.drop(columns=['nome_anuncio'], inplace=True)

    # --- FIM DA LIMPEZA ---
    
    # Faz o merge para adicionar as informações do criativo ao DataFrame de performance
    df_fin = df_res.merge(
        df_cre[['ad_id', 'final_url', 'nome_anuncio']], # Seleciona as colunas de df_cre
        left_on='id_anuncio',
        right_on='ad_id',
        how='left'
    )

    # --- INÍCIO DA LIMPEZA FINAL ---
    
    # ALTERAÇÃO: Remove a coluna 'ad_id' que é redundante após o merge
    df_fin.drop(columns=['ad_id'], inplace=True)
    
    # Preenche valores nulos para URL e nome do anúncio (caso não encontre correspondência)
    df_fin['final_url'] = df_fin['final_url'].fillna(DEFAULT_URL)
    df_fin['nome_anuncio'] = df_fin['nome_anuncio'].fillna("Nome não encontrado")
    
    # --- FIM DA LIMPEZA FINAL ---
    
    # Salva o arquivo final, garantindo que o índice não seja incluído
    df_fin.to_csv(ANUNCIOS_IMAGENS_FILE, index=False)
    print(f"CSV salvo: {ANUNCIOS_IMAGENS_FILE}")
    print(f"Total de correspondências encontradas: {df_fin['final_url'].notna().sum() - (df_fin['final_url'] == DEFAULT_URL).sum()}")


def main(plataforma: str = "all"):
    ontem     = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    assets_df = get_assets_ads("2025-01-01", ontem, plataforma=plataforma)
    
    if assets_df.empty:
        print("Nenhum dado de criativo foi coletado. Encerrando.")
        return

    append_and_deduplicate(assets_df, CREATIVES_CSV)
    join_creatives_with_resumo()


if __name__ == "__main__":
    main(plataforma="all")
