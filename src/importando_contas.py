# -*- coding: utf-8 -*-
"""
Checa na API do Meta (Facebook Marketing API) as ad accounts do usuário e,
se a conta configurada em META_TARGET_ACCOUNT existir/acessível, coleta um head de dados
e grava em OUTPUT_PATH no padrão meta_ads_data_{conta}.parquet.

Requisitos: facebook_business, polars, PyYAML, e seu meta.yaml em CONFIG_META_PATH.
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import yaml
import polars as pl

# Reutiliza config/caminhos do seu projeto:
from src.utils_do_pai import OUTPUT_PATH, CONFIG_META_PATH

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount

ALVO = os.environ.get("META_TARGET_ACCOUNT", "")   # conta que você quer validar

def init_meta_api():
    cfg = CONFIG_META_PATH / "meta.yaml"
    if not cfg.exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {cfg}")
    segredos = yaml.safe_load(open(cfg, "r", encoding="utf-8"))
    access_token = segredos.get("access_token")
    app_id = segredos.get("app_id")
    app_secret = segredos.get("app_secret")
    if not all([access_token, app_id, app_secret]):
        raise RuntimeError("meta.yaml precisa conter app_id, app_secret e access_token.")
    FacebookAdsApi.init(app_id=app_id, app_secret=app_secret, access_token=access_token)

def listar_ad_accounts():
    """Retorna lista de dicts com id, name, status, currency, business."""
    me = User(fbid="me")
    fields = [
        "id", "name", "account_status", "currency",
        "business", "owner", "timezone_name"
    ]
    contas = []
    for acc in me.get_ad_accounts(fields=fields):
        contas.append({
            "id": acc.get("id"),
            "name": acc.get("name"),
            "account_status": acc.get("account_status"),
            "currency": acc.get("currency"),
            "business": (acc.get("business") or {}).get("name"),
            # "owner": (acc.get("owner") or {}).get("name"),
            "timezone_name": acc.get("timezone_name"),
        })
    return contas

def head_da_conta(conta_id: str, dias: int = 7) -> pl.DataFrame:
    """
    Faz uma consulta simples de insights em nível de 'account' para os últimos N dias
    e retorna um DataFrame pequeno (head). Também grava no padrão do projeto.
    """
    # Janela de datas curta para teste:
    fim = datetime.utcnow().date()
    ini = fim - timedelta(days=dias-1)

    conta = AdAccount(conta_id)
    # Campos leves no nível 'account'
    fields = ["account_id", "date_start", "date_stop", "impressions", "clicks", "spend", "actions"]
    params = dict(time_increment=1, level="account", time_range={"since": str(ini), "until": str(fim)})

    insights = list(conta.get_insights(fields=fields, params=params))

    # Normaliza em um DF polars:
    rows = []
    for it in insights:
        actions = it.get("actions") or []
        # Flatten de 2 métricas comuns para facilitar visualização:
        convs = {a.get("action_type"): a.get("value") for a in actions if isinstance(a, dict)}
        rows.append({
            "nivel": "conta",
            "conta_id": it.get("account_id"),
            "data": it.get("date_start"),
            "impressions": it.get("impressions"),
            "clicks": it.get("clicks"),
            "spend": it.get("spend"),
            "purchase": convs.get("purchase"),
            "lead": convs.get("lead"),
            "add_to_cart": convs.get("add_to_cart"),
        })

    df = pl.DataFrame(rows) if rows else pl.DataFrame(schema={
        "nivel": pl.Utf8, "conta_id": pl.Utf8, "data": pl.Utf8,
        "impressions": pl.Utf8, "clicks": pl.Utf8, "spend": pl.Utf8,
        "purchase": pl.Utf8, "lead": pl.Utf8, "add_to_cart": pl.Utf8
    })

    # Garante diretório e grava no padrão do projeto:
    out_dir = Path(OUTPUT_PATH)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"meta_ads_data_{conta_id}.parquet"
    df.write_parquet(out_file)

    print(f"\nArquivo gravado: {out_file}")
    print(f"Tamanho: {out_file.stat().st_size} bytes")
    return df

def main():
    if not ALVO:
        raise SystemExit("Set META_TARGET_ACCOUNT before running this diagnostic.")
    init_meta_api()

    # 1) lista contas via API
    contas = listar_ad_accounts()
    print("\n=== AD ACCOUNTS DO TOKEN ===")
    for c in contas:
        print(f"- {c['id']} | {c['name']} | status={c['account_status']} | tz={c['timezone_name']} | business={c['business']}")

    ids = {c["id"] for c in contas}
    if ALVO not in ids:
        print(f"\n⚠️ A conta {ALVO} **NÃO** apareceu para este token. "
              f"Verifique permissões (acesso ao Business, role, token correto/escopo).")
        return

    # 2) coleta um head de dados da conta-alvo e grava no OUTPUT_PATH
    print(f"\n✅ Conta {ALVO} encontrada. Gerando head de dados (últimos 7 dias, nível account)…")
    df_head = head_da_conta(ALVO, dias=7)

    print("\n=== HEAD (até 10 linhas) ===")
    print(df_head.head(10))

if __name__ == "__main__":
    main()
