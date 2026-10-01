from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.account_catalog import list_accounts


DEFAULT_GRAPH_VERSION = "v23.0"
DEFAULT_TARGET_SUCCESSES = 500
DEFAULT_SLEEP_SECONDS = 1.0
DEFAULT_MAX_ERROR_RATE = 0.10
DEFAULT_MAX_CONSECUTIVE_FAILURES = 5


@dataclass(frozen=True)
class MarketingApiCall:
    label: str
    path: str
    params: dict[str, Any]


def _candidate_config_paths() -> list[Path]:
    env_path = os.environ.get("META_CONFIG_PATH")
    repo_root = Path(__file__).resolve().parents[1]
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            Path("/app/config/meta.yaml"),
            repo_root / "config" / "meta.yaml",
            Path("config") / "meta.yaml",
        ]
    )
    return candidates


def resolve_config_path(config_path: str | None) -> Path:
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de config nao encontrado: {path}")
        return path

    for path in _candidate_config_paths():
        if path.exists():
            return path

    searched = ", ".join(str(path) for path in _candidate_config_paths())
    raise FileNotFoundError(f"meta.yaml nao encontrado. Caminhos testados: {searched}")


def load_meta_config(config_path: Path) -> dict[str, str]:
    raw: dict[str, str] = {}
    with open(config_path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith("#"):
                continue
            if " #" in value:
                value = value.split(" #", 1)[0].strip()
            raw[key] = value.strip('"').strip("'")

    required = ("access_token", "app_id", "app_secret")
    missing = [key for key in required if not raw.get(key)]
    if missing:
        raise ValueError(f"Config Meta sem campos obrigatorios: {missing}")

    return {key: str(raw[key]).strip() for key in required}


def normalize_accounts(accounts_arg: str | None) -> list[str]:
    if accounts_arg:
        accounts = [item.strip() for item in accounts_arg.split(",") if item.strip()]
    else:
        accounts = list_accounts("meta")

    if not accounts:
        raise ValueError("Nenhuma conta Meta informada/encontrada.")

    return [account if account.startswith("act_") else f"act_{account}" for account in accounts]


def day_for_round(round_index: int, end_date: date) -> str:
    # Rotate recent dates so insights calls are real but still small.
    offset = (round_index % 14) + 1
    return (end_date - timedelta(days=offset)).strftime("%Y-%m-%d")


def build_calls_for_account(account_id: str, insight_day: str) -> list[MarketingApiCall]:
    insight_range = json.dumps({"since": insight_day, "until": insight_day}, separators=(",", ":"))
    return [
        MarketingApiCall(
            label="ad_account",
            path=account_id,
            params={"fields": "id,name,account_status,currency,timezone_name"},
        ),
        MarketingApiCall(
            label="campaigns",
            path=f"{account_id}/campaigns",
            params={"fields": "id,name,status,objective", "limit": 1},
        ),
        MarketingApiCall(
            label="adsets",
            path=f"{account_id}/adsets",
            params={"fields": "id,name,status,campaign_id,optimization_goal", "limit": 1},
        ),
        MarketingApiCall(
            label="ads",
            path=f"{account_id}/ads",
            params={"fields": "id,name,status,campaign_id,adset_id", "limit": 1},
        ),
        MarketingApiCall(
            label="insights_campaign",
            path=f"{account_id}/insights",
            params={
                "fields": "account_id,campaign_id,impressions,reach,clicks,spend,date_start,date_stop",
                "level": "campaign",
                "time_increment": 1,
                "time_range": insight_range,
                "limit": 1,
            },
        ),
        MarketingApiCall(
            label="insights_ad",
            path=f"{account_id}/insights",
            params={
                "fields": "account_id,ad_id,impressions,reach,clicks,spend,date_start,date_stop",
                "level": "ad",
                "time_increment": 1,
                "time_range": insight_range,
                "limit": 1,
            },
        ),
    ]


def parse_json_text(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except ValueError:
        return {"raw": text[:500]}
    return payload if isinstance(payload, dict) else {"raw": payload}


def error_summary(payload: dict[str, Any]) -> dict[str, Any]:
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return {}
    return {
        "message": error.get("message"),
        "type": error.get("type"),
        "code": error.get("code"),
        "subcode": error.get("error_subcode"),
        "fbtrace_id": error.get("fbtrace_id"),
    }


def is_hard_auth_error(payload: dict[str, Any]) -> bool:
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return False

    message = str(error.get("message") or "").lower()
    code = error.get("code")
    error_type = str(error.get("type") or "")
    return (
        "api access blocked" in message
        or code == 190
        or (code == 200 and "OAuthException" in error_type and "permission" in message)
    )


def execute_call(
    base_url: str,
    access_token: str,
    call: MarketingApiCall,
    timeout_seconds: int,
) -> tuple[bool, int | None, dict[str, Any]]:
    params = dict(call.params)
    params["access_token"] = access_token
    url = f"{base_url.rstrip('/')}/{call.path}?{urlencode(params)}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "media-data-pipeline/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        status_code = int(exc.code)
        text = exc.read().decode("utf-8", errors="replace")

    payload = parse_json_text(text)
    ok = 200 <= status_code < 300 and "error" not in payload
    return ok, status_code, payload


def run_review_calls(args: argparse.Namespace) -> int:
    config_path = resolve_config_path(args.config)
    config = load_meta_config(config_path)
    accounts = normalize_accounts(args.accounts)
    base_url = f"https://graph.facebook.com/{args.graph_version}"
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()

    print(f"[Meta Review] Config: {config_path}")
    print(f"[Meta Review] App ID: {config['app_id']}")
    print(f"[Meta Review] Contas: {accounts}")
    print(f"[Meta Review] Alvo: {args.target_successes} chamadas bem-sucedidas")
    print(f"[Meta Review] Pausa entre chamadas: {args.sleep_seconds}s")

    successes = 0
    failures = 0
    attempts = 0
    consecutive_failures = 0

    started_at = time.time()

    while successes < args.target_successes and attempts < args.max_attempts:
        round_index = attempts // max(1, len(accounts))
        insight_day = day_for_round(round_index, end_date)

        for account_id in accounts:
            for call in build_calls_for_account(account_id, insight_day):
                if successes >= args.target_successes or attempts >= args.max_attempts:
                    break

                attempts += 1
                try:
                    ok, http_status, payload = execute_call(
                        base_url,
                        config["access_token"],
                        call,
                        timeout_seconds=args.timeout_seconds,
                    )
                except (OSError, TimeoutError, URLError) as exc:
                    ok = False
                    http_status = None
                    payload = {"error": {"message": str(exc), "type": exc.__class__.__name__}}

                if ok:
                    successes += 1
                    consecutive_failures = 0
                    print(
                        f"[{attempts:03d}] OK    {call.label:<18} {account_id} "
                        f"sucessos={successes}/{args.target_successes}"
                    )
                else:
                    failures += 1
                    consecutive_failures += 1
                    summary = error_summary(payload)
                    print(
                        f"[{attempts:03d}] ERRO  {call.label:<18} {account_id} "
                        f"http={http_status} detalhes={summary or payload}"
                    )

                    if is_hard_auth_error(payload):
                        print("[Meta Review] Abortando: erro de token/permissao/bloqueio detectado.")
                        return 2

                    if consecutive_failures >= args.max_consecutive_failures:
                        print("[Meta Review] Abortando: falhas consecutivas acima do limite.")
                        return 3

                if attempts >= 20:
                    error_rate = failures / attempts
                    if error_rate > args.max_error_rate:
                        print(
                            "[Meta Review] Abortando: taxa de erro atual "
                            f"{error_rate:.1%} acima do limite {args.max_error_rate:.1%}."
                        )
                        return 4

                if args.sleep_seconds > 0 and successes < args.target_successes:
                    time.sleep(args.sleep_seconds)

    elapsed = time.time() - started_at
    final_error_rate = failures / attempts if attempts else 0.0
    print(
        "[Meta Review] Finalizado: "
        f"tentativas={attempts}, sucessos={successes}, falhas={failures}, "
        f"taxa_erro={final_error_rate:.1%}, duracao={elapsed:.1f}s"
    )

    if successes >= args.target_successes and final_error_rate <= args.max_error_rate:
        print("[Meta Review] Alvo atingido. A Meta pode levar ate 24h para atualizar o contador.")
        return 0

    print("[Meta Review] Alvo nao atingido dentro dos limites configurados.")
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Executa chamadas leves e reais da Meta Marketing API para teste/App Review."
    )
    parser.add_argument("--config", type=str, help="Caminho para meta.yaml. Padrao: auto-detecta.")
    parser.add_argument("--accounts", type=str, help="Lista de contas act_ separadas por virgula.")
    parser.add_argument("--graph-version", default=DEFAULT_GRAPH_VERSION, help="Versao da Graph API.")
    parser.add_argument("--target-successes", type=int, default=DEFAULT_TARGET_SUCCESSES)
    parser.add_argument("--max-attempts", type=int, default=600)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--max-error-rate", type=float, default=DEFAULT_MAX_ERROR_RATE)
    parser.add_argument("--max-consecutive-failures", type=int, default=DEFAULT_MAX_CONSECUTIVE_FAILURES)
    parser.add_argument(
        "--end-date",
        type=str,
        help="Data base YYYY-MM-DD para alternar chamadas de insights. Padrao: hoje.",
    )
    return parser


def main() -> None:
    raise SystemExit(run_review_calls(build_parser().parse_args()))


if __name__ == "__main__":
    main()
