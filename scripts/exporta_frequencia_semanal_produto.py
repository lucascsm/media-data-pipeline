from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.account_catalog import (  # noqa: E402
    ACCOUNT_CATALOG,
    normalize_account_id,
    normalize_product,
)


SUPPORTED_CHANNELS = ("google", "meta", "tiktok")
ALL_PRODUCTS_ALIASES = {"todos", "todo", "all", "*"}
CSV_COLUMNS = (
    "semana_ref",
    "semana",
    "nome_semana",
    "data_inicio_semana",
    "data_fim_semana",
    "veiculo",
    "produto",
    "id_cliente",
    "impressoes",
    "alcance",
    "frequencia",
    "status_metrica",
    "origem_metrica",
    "observacao",
)


@dataclass(frozen=True)
class Semana:
    inicio: date
    fim: date

    def as_record(self) -> dict[str, str]:
        return {
            "semana_ref": self.inicio.isoformat(),
            "semana": self.inicio.strftime("%d/%m/%Y"),
            "nome_semana": (
                f"{self.inicio.strftime('%d/%m/%Y')} - "
                f"{self.fim.strftime('%d/%m/%Y')}"
            ),
            "data_inicio_semana": self.inicio.isoformat(),
            "data_fim_semana": self.fim.isoformat(),
        }


def _parse_date(value: str) -> date:
    text = str(value).strip()
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Data invalida: {value!r}. Use YYYY-MM-DD ou DD/MM/YYYY."
    )


def _build_weeks(start: date, end: date) -> list[Semana]:
    if start > end:
        raise ValueError("data_inicio nao pode ser maior que data_fim.")
    if start.weekday() != 0:
        raise ValueError("data_inicio deve ser uma segunda-feira.")
    if end.weekday() != 6:
        raise ValueError("data_fim deve ser um domingo.")
    if ((end - start).days + 1) % 7 != 0:
        raise ValueError("O periodo deve conter apenas semanas completas de segunda a domingo.")

    weeks: list[Semana] = []
    current = start
    while current <= end:
        weeks.append(Semana(inicio=current, fim=current + timedelta(days=6)))
        current += timedelta(days=7)
    return weeks


def _parse_channels(value: str) -> list[str]:
    channels = []
    for raw_channel in str(value).split(","):
        channel = raw_channel.strip().lower()
        if not channel:
            continue
        if channel not in SUPPORTED_CHANNELS:
            raise argparse.ArgumentTypeError(
                f"Veiculo invalido: {channel}. Use: {', '.join(SUPPORTED_CHANNELS)}."
            )
        if channel not in channels:
            channels.append(channel)
    if not channels:
        raise argparse.ArgumentTypeError("Informe pelo menos um veiculo.")
    return channels


def _safe_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", ascii_value.lower()).strip("_") or "produto"


def _all_products_selected(product: str) -> bool:
    return str(product).strip().lower() in ALL_PRODUCTS_ALIASES


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {path}")

    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dependencia PyYAML ausente. Instale as dependencias com "
            "python -m pip install -r requirements.txt"
        ) from exc

    with path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}


def _require_config(config: dict[str, Any], required_keys: tuple[str, ...], label: str) -> None:
    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        raise RuntimeError(
            f"Configuracao {label} incompleta. Chaves ausentes: {', '.join(missing)}."
        )


def _to_float(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    parsed = _to_float(value)
    return int(parsed) if parsed is not None else None


def _empty_metrics(status: str, source: str, note: str) -> dict[str, Any]:
    return {
        "impressoes": None,
        "alcance": None,
        "frequencia": None,
        "status_metrica": status,
        "origem_metrica": source,
        "observacao": note,
    }


def _metrics_record(
    impressions: Any,
    reach: Any,
    frequency: Any,
    source: str,
    *,
    status: str = "ok",
    note: str = "",
) -> dict[str, Any]:
    parsed_impressions = _to_int(impressions)
    parsed_reach = _to_int(reach)
    parsed_frequency = _to_float(frequency)

    if not parsed_impressions:
        return _empty_metrics("sem_dados", source, "A API nao retornou impressoes no periodo.")
    if not parsed_reach:
        return _empty_metrics(
            "sem_alcance",
            source,
            "A API retornou impressoes, mas nao retornou alcance deduplicado.",
        ) | {"impressoes": parsed_impressions}

    calculated_frequency = parsed_impressions / parsed_reach
    if parsed_frequency is None:
        parsed_frequency = calculated_frequency
        note = "; ".join(
            part
            for part in (
                note,
                "Frequencia calculada como impressoes/alcance da mesma resposta agregada.",
            )
            if part
        )

    tolerance = max(0.01, abs(parsed_frequency) * 0.01)
    if abs(parsed_frequency - calculated_frequency) > tolerance:
        status = "atencao"
        note = "; ".join(
            part
            for part in (
                note,
                "Frequencia retornada difere de impressoes/alcance em mais de 1%.",
            )
            if part
        )

    return {
        "impressoes": parsed_impressions,
        "alcance": parsed_reach,
        "frequencia": parsed_frequency,
        "status_metrica": status,
        "origem_metrica": source,
        "observacao": note,
    }


def _extract_meta(account_id: str, week: Semana, config_dir: Path) -> dict[str, Any]:
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dependencia facebook-business ausente. Instale as dependencias do projeto."
        ) from exc

    # Reuse only the retry policy. Do not use the date-splitting fallback because
    # reach from smaller periods cannot be combined into a deduplicated week.
    from src.metads import executar_com_retry

    config = _load_yaml(config_dir / "meta.yaml")
    _require_config(config, ("app_id", "app_secret", "access_token"), "Meta")
    FacebookAdsApi.init(
        app_id=config["app_id"],
        app_secret=config["app_secret"],
        access_token=config["access_token"],
    )

    params = {
        "time_range": {"since": week.inicio.isoformat(), "until": week.fim.isoformat()},
        "time_increment": "all_days",
        "level": "account",
        "limit": 100,
    }
    insights = executar_com_retry(
        lambda: AdAccount(account_id).get_insights(
            fields=["impressions", "reach", "frequency"],
            params=params,
        )
    )
    if not insights:
        return _empty_metrics(
            "sem_dados",
            "meta:level=account,time_increment=all_days",
            "A API nao retornou linha para a conta no periodo.",
        )
    if len(insights) != 1:
        raise RuntimeError(
            f"Meta retornou {len(insights)} linhas no nivel account; esperado: 1."
        )

    row = insights[0]
    return _metrics_record(
        row.get("impressions"),
        row.get("reach"),
        row.get("frequency"),
        "meta:level=account,time_increment=all_days",
    )


def _extract_tiktok(account_id: str, week: Semana, config_dir: Path) -> dict[str, Any]:
    from src.tiktok import TIKTOK_API_BASE, _fetch_integrated_paginated

    config = _load_yaml(config_dir / "tiktok.yaml")
    _require_config(config, ("access_token",), "TikTok")
    headers = {"Access-Token": config["access_token"], "Content-Type": "application/json"}
    params = {
        "advertiser_id": account_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_ADVERTISER",
        "dimensions": json.dumps(["advertiser_id"]),
        "metrics": json.dumps(["impressions", "reach", "frequency"]),
        "start_date": week.inicio.isoformat(),
        "end_date": week.fim.isoformat(),
        "page": 1,
        "page_size": 1000,
    }
    body = _fetch_integrated_paginated(
        endpoint=f"{TIKTOK_API_BASE}/report/integrated/get/",
        headers=headers,
        base_params=params,
        context_label="Frequencia semanal TikTok AUCTION_ADVERTISER",
    )
    if body.get("code") != 0:
        raise RuntimeError(
            f"TikTok code={body.get('code')}, message={body.get('message') or 'sem mensagem'}"
        )

    rows = ((body.get("data") or {}).get("list") or [])
    if not rows:
        return _empty_metrics(
            "sem_dados",
            "tiktok:data_level=AUCTION_ADVERTISER",
            "A API nao retornou linha para o advertiser no periodo.",
        )
    if len(rows) != 1:
        raise RuntimeError(
            f"TikTok retornou {len(rows)} linhas no nivel advertiser; esperado: 1."
        )

    metrics = rows[0].get("metrics") or {}
    return _metrics_record(
        metrics.get("impressions"),
        metrics.get("reach"),
        metrics.get("frequency"),
        "tiktok:data_level=AUCTION_ADVERTISER",
    )


def _google_enum_name(value: Any) -> str:
    name = getattr(value, "name", None)
    if name:
        return str(name)
    text = str(value)
    return text.rsplit(".", 1)[-1]


def _extract_google(account_id: str, week: Semana, config_dir: Path) -> dict[str, Any]:
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Dependencia google-ads ausente. Instale as dependencias do projeto."
        ) from exc

    from src.google_ads import _run_with_google_ads_retry

    config_path = config_dir / "google.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_path}")

    client = GoogleAdsClient.load_from_storage(str(config_path))
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          campaign.advertising_channel_type,
          metrics.impressions,
          metrics.unique_users,
          metrics.average_impression_frequency_per_user
        FROM campaign
        WHERE segments.date BETWEEN '{week.inicio.isoformat()}' AND '{week.fim.isoformat()}'
    """

    def _fetch_summary() -> tuple[Any, dict[str, int]]:
        request = client.get_type("SearchGoogleAdsRequest")
        request.customer_id = account_id
        request.query = query
        if "search_settings" in {
            field.name for field in request._pb.DESCRIPTOR.fields
        }:
            # Current API versions expose the summary switch inside
            # SearchSettings instead of SummaryRowSettingEnum.
            request.search_settings.return_summary_row = True
        else:
            request.summary_row_setting = (
                client.enums.SummaryRowSettingEnum.SUMMARY_ROW_WITH_RESULTS
            )

        pager = service.search(request=request)
        summary_row = None
        impressions_by_type: dict[str, int] = {}
        for page in pager.pages:
            for result in page.results:
                impressions = int(result.metrics.impressions)
                if not impressions:
                    continue
                campaign_type = _google_enum_name(
                    result.campaign.advertising_channel_type
                )
                impressions_by_type[campaign_type] = (
                    impressions_by_type.get(campaign_type, 0) + impressions
                )
            candidate = getattr(page, "summary_row", None)
            if candidate is not None:
                summary_row = candidate
        return summary_row, impressions_by_type

    summary_row, impressions_by_type = _run_with_google_ads_retry(
        _fetch_summary,
        context=(
            f"frequencia semanal por cliente customer={account_id} "
            f"periodo={week.inicio.isoformat()}..{week.fim.isoformat()}"
        ),
    )
    if summary_row is None:
        return _empty_metrics(
            "sem_dados",
            "google:campaign_summary_row",
            "A API nao retornou summary_row para a conta no periodo.",
        )

    active_types = sorted(
        campaign_type
        for campaign_type, impressions in impressions_by_type.items()
        if impressions > 0
    )
    status = "ok"
    note = ""
    if "SEARCH" in active_types:
        status = "atencao"
        note = (
            "Conta com campanhas SEARCH. Validar a cobertura de unique_users antes de "
            "tratar o total Google como frequencia oficial do produto."
        )
    if active_types:
        note = "; ".join(
            part for part in (note, f"Tipos com impressoes: {', '.join(active_types)}.") if part
        )

    metrics = summary_row.metrics
    return _metrics_record(
        metrics.impressions,
        metrics.unique_users,
        metrics.average_impression_frequency_per_user,
        "google:campaign_summary_row",
        status=status,
        note=note,
    )


EXTRACTORS: dict[str, Callable[[str, Semana, Path], dict[str, Any]]] = {
    "google": _extract_google,
    "meta": _extract_meta,
    "tiktok": _extract_tiktok,
}


def _select_accounts(product: str, channels: list[str]) -> list[dict[str, str]]:
    if _all_products_selected(product):
        accounts = [
            item for item in ACCOUNT_CATALOG if item["platform"] in channels
        ]
        found_channels = {item["platform"] for item in accounts}
        missing_channels = [channel for channel in channels if channel not in found_channels]
        if missing_channels:
            raise ValueError(
                f"Nenhuma conta ativa no catalogo para: {', '.join(missing_channels)}."
            )
        return accounts

    normalized_product = normalize_product(product)
    accounts = [
        item
        for item in ACCOUNT_CATALOG
        if item["platform"] in channels
        and normalize_product(item["product"]) == normalized_product
    ]
    found_channels = {item["platform"] for item in accounts}
    missing_channels = [channel for channel in channels if channel not in found_channels]
    if missing_channels:
        raise ValueError(
            f"Produto {normalized_product!r} sem conta ativa no catalogo para: "
            f"{', '.join(missing_channels)}."
        )
    return accounts


def _base_record(week: Semana, account: dict[str, str]) -> dict[str, Any]:
    return {
        **week.as_record(),
        "veiculo": account["platform"],
        "produto": account["product"],
        "id_cliente": normalize_account_id(account["account_id"]),
    }


def _validate_unique_grain(rows: list[dict[str, Any]]) -> None:
    seen: set[tuple[str, str, str, str]] = set()
    duplicates: list[tuple[str, str, str, str]] = []
    for row in rows:
        key = (
            str(row["semana_ref"]),
            str(row["veiculo"]),
            str(row["produto"]),
            str(row["id_cliente"]),
        )
        if key in seen:
            duplicates.append(key)
        seen.add(key)
    if duplicates:
        raise RuntimeError(f"Chaves duplicadas no resultado: {duplicates}")


def _format_decimal(value: Any, decimal_separator: str) -> str:
    if value is None:
        return ""
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return text.replace(".", decimal_separator)


def _write_csv(rows: list[dict[str, Any]], output_path: Path, excel_pt_br: bool) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    delimiter = ";" if excel_pt_br else ","
    decimal_separator = "," if excel_pt_br else "."

    with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            output_row = {column: row.get(column, "") for column in CSV_COLUMNS}
            output_row["impressoes"] = (
                "" if row.get("impressoes") is None else str(int(row["impressoes"]))
            )
            output_row["alcance"] = (
                "" if row.get("alcance") is None else str(int(row["alcance"]))
            )
            output_row["frequencia"] = _format_decimal(
                row.get("frequencia"), decimal_separator
            )
            writer.writerow(output_row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exporta alcance e frequencia semanal deduplicados no nivel de id_cliente, "
            "sem agregar linhas de campanha."
        )
    )
    parser.add_argument("--data-inicio", required=True, type=_parse_date)
    parser.add_argument("--data-fim", required=True, type=_parse_date)
    parser.add_argument(
        "--produto",
        default="Produto C",
        help="Nome do produto ou 'todos' para exportar todas as contas ativas.",
    )
    parser.add_argument(
        "--veiculos",
        "--canais",
        dest="veiculos",
        default="google,meta,tiktok",
        type=_parse_channels,
        help="Lista separada por virgula. Padrao: google,meta,tiktok.",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=REPO_ROOT / "config",
        help="Diretorio local com google.yaml, meta.yaml e tiktok.yaml.",
    )
    parser.add_argument(
        "--saida",
        type=Path,
        help="Caminho do CSV. Se omitido, grava em output/ dentro do repositorio.",
    )
    parser.add_argument(
        "--csv-standard",
        action="store_true",
        help="Usa virgula como separador e ponto decimal. O padrao e Excel pt-BR.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        weeks = _build_weeks(args.data_inicio, args.data_fim)
        accounts = _select_accounts(args.produto, args.veiculos)
    except ValueError as exc:
        print(f"Erro de parametros: {exc}", file=sys.stderr)
        return 2

    all_products = _all_products_selected(args.produto)
    normalized_product = normalize_product(args.produto) or args.produto
    output_path = args.saida
    if output_path is None:
        output_prefix = (
            "frequencia_semanal_todos_produtos"
            if all_products
            else f"frequencia_semanal_produto_{_safe_slug(normalized_product)}"
        )
        output_path = REPO_ROOT / "output" / (
            f"{output_prefix}_"
            f"{args.data_inicio.strftime('%Y%m%d')}_{args.data_fim.strftime('%Y%m%d')}.csv"
        )
    elif not output_path.is_absolute():
        output_path = Path.cwd() / output_path

    rows: list[dict[str, Any]] = []
    failures = 0
    for week in weeks:
        for account in accounts:
            platform = account["platform"]
            account_id = account["account_id"]
            print(
                f"[{platform}] {account['product']} / {account_id}: "
                f"{week.inicio.isoformat()} a {week.fim.isoformat()}"
            )
            record = _base_record(week, account)
            try:
                record.update(EXTRACTORS[platform](account_id, week, args.config_dir))
            except Exception as exc:
                failures += 1
                print(f"  Falha: {type(exc).__name__}: {exc}", file=sys.stderr)
                record.update(
                    _empty_metrics(
                        "erro",
                        f"{platform}:api_periodo_id_cliente",
                        "Falha na consulta; consulte a mensagem exibida no terminal.",
                    )
                )
            rows.append(record)

    _validate_unique_grain(rows)
    rows.sort(
        key=lambda row: (
            row["semana_ref"],
            row["veiculo"],
            row["produto"],
            row["id_cliente"],
        )
    )
    _write_csv(rows, output_path, excel_pt_br=not args.csv_standard)

    print(f"CSV gravado em: {output_path.resolve()}")
    print(f"Linhas: {len(rows)} | Falhas: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
