from src.legacy_config import load_legacy_settings
import yaml
import polars as pl
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    Filter,
    FilterExpression,
)

# ---------------------------
# CONFIGURAÇÕES INICIAIS
# ---------------------------
from src.utils_do_pai import GCS_MOUNT
#GCS_MOUNT = Path("/path/to/project")
# Pasta de saída dentro do bucket
OUTPUT_DIR  = GCS_MOUNT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Pasta de configuração dentro do container/app
#ONLINE from src.utils_do_pai import CONFIG_DIR
from src.utils_do_pai import CONFIG_DIR
# Caminho para credenciais GA4
YAML_CREDENTIALS_PATH = CONFIG_DIR / "google.yaml"

# ID da propriedade GA4
GA4_PROPERTY_ID = str(load_legacy_settings()["ga4_property_id"])

# ---------------------------
# CARREGAR CREDENCIAIS DO YAML
# ---------------------------
with open(YAML_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

CLIENT_ID = config["client_id"]
CLIENT_SECRET = config["client_secret"]
REFRESH_TOKEN = config["refresh_token"]

# ---------------------------
# CRIAR E ATUALIZAR CREDENCIAIS OAUTH2
# ---------------------------
creds = Credentials(
    token=None,
    refresh_token=REFRESH_TOKEN,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    token_uri="https://oauth2.googleapis.com/token",
)

try:
    creds.refresh(Request())
except RefreshError as e:
    raise RuntimeError(f"Erro ao atualizar token de acesso: {e}")

client = BetaAnalyticsDataClient(credentials=creds)

# ---------------------------
# MAPEAMENTO URL -> PRODUTO
url_to_produto = load_legacy_settings().get("url_to_product", {})

page_location_filter = FilterExpression(
    filter=Filter(
        field_name="pageLocation",
        in_list_filter=Filter.InListFilter(values=list(url_to_produto.keys()))
    )
)

# ---------------------------
# FUNÇÕES AUXILIARES
# ---------------------------
def ga4_response_to_df(response) -> pl.DataFrame:
    dim_names = [h.name for h in response.dimension_headers]
    met_names = [h.name for h in response.metric_headers]
    columns = dim_names + met_names
    data = []
    for row in response.rows:
        dim_vals = list(row.dimension_values)
        met_vals = list(row.metric_values)
        values = [d.value for d in dim_vals + met_vals]
        data.append(dict(zip(columns, values)))
    return pl.DataFrame(data)

def add_produto_column(df: pl.DataFrame) -> pl.DataFrame:
    if "pageLocation" not in df.columns:
        return df.with_columns(pl.lit("Indefinido").alias("produto"))
    produto_values = [url_to_produto.get(url, "Indefinido") for url in df["pageLocation"].to_list()]
    return df.with_columns(pl.Series("produto", produto_values))

def format_date_column(df: pl.DataFrame) -> pl.DataFrame:
    if "date" not in df.columns:
        return df
    return df.with_columns(
        pl.when(pl.col("date").str.contains("-"))
          .then(pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False))
          .otherwise(pl.col("date").str.strptime(pl.Date, format="%Y%m%d", strict=False))
          .alias("date")
    )

def to_string_df(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([pl.col(col).cast(pl.Utf8).alias(col) for col in df.columns])

def _safe_int_column(expr: pl.Expr) -> pl.Expr:
    return (
        expr.cast(pl.Float64, strict=False)
        .fill_nan(None)
        .round(0)
        .cast(pl.Int64)
    )

def _safe_float_column(expr: pl.Expr) -> pl.Expr:
    return expr.cast(pl.Float64, strict=False)

def append_to_excel(file_path: Path, new_df: pl.DataFrame) -> None:
    new_df_str = to_string_df(new_df)

    if file_path.exists():
        old_pd = pd.read_csv(file_path)
        old_df = pl.from_pandas(old_pd)
        old_df_str = to_string_df(old_df)
        combined_df = pl.concat([old_df_str, new_df_str], how="diagonal")
    else:
        combined_df = new_df_str

    cast_exprs = []
    if "sessions" in combined_df.columns:
        cast_exprs.append(_safe_int_column(pl.col("sessions")).alias("sessions"))
    if "totalUsers" in combined_df.columns:
        cast_exprs.append(_safe_int_column(pl.col("totalUsers")).alias("totalUsers"))
    if "bounceRate" in combined_df.columns:
        cast_exprs.append(_safe_float_column(pl.col("bounceRate")).alias("bounceRate"))
    if "engagedSessions" in combined_df.columns:
        cast_exprs.append(_safe_int_column(pl.col("engagedSessions")).alias("engagedSessions"))
    if cast_exprs:
        combined_df = combined_df.with_columns(cast_exprs)

    combined_df = format_date_column(combined_df)

    if "date" in combined_df.columns:
        combined_df = combined_df.with_columns(
            pl.col("date").dt.strftime("%Y-%m-%d").alias("date")
        )

    combined_df.to_pandas().to_csv(file_path, index=False)

# ---------------------------
# MAIN
# ---------------------------
def main(start_date: str = "2025-02-09", end_date: str = "2025-02-15"):
    current_ts = datetime.now().isoformat()

    # --- Requisição 1: Navegação ---
    request_nav = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pageLocation"),
            Dimension(name="sourceMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="bounceRate"),
            Metric(name="engagedSessions"),
        ],
        dimension_filter=page_location_filter
    )
    response_nav = client.run_report(request_nav)
    df_nav = ga4_response_to_df(response_nav)
    df_nav = add_produto_column(df_nav).pipe(format_date_column).with_columns(pl.lit(current_ts).alias("timestamp"))
    nav_excel_path = OUTPUT_DIR / "navigation_report.csv"
    append_to_excel(nav_excel_path, df_nav)
    print(f"Relatório de navegação salvo em: {nav_excel_path}")

    # --- Requisição 2: Perfil ---
    request_profile = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pageLocation"),
            Dimension(name="userGender"),
            Dimension(name="userAgeBracket"),
            Dimension(name="brandingInterest"),
            Dimension(name="platformDeviceCategory")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="bounceRate"),
            Metric(name="engagedSessions"),
        ],
        dimension_filter=page_location_filter
    )
    response_profile = client.run_report(request_profile)
    df_profile = ga4_response_to_df(response_profile)
    df_profile = add_produto_column(df_profile).pipe(format_date_column).with_columns(pl.lit(current_ts).alias("timestamp"))
    profile_excel_path = OUTPUT_DIR / "profile_report.csv"
    append_to_excel(profile_excel_path, df_profile)
    print(f"Relatório de perfil salvo em: {profile_excel_path}")

if __name__ == '__main__':
    main("2025-07-01", "2025-07-04")
