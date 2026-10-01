"""Plan SQL publication; never call the data-refresh orchestration automatically."""
from __future__ import annotations
import argparse
import json
import re
import subprocess
from pathlib import Path
from render_sql import ROOT, render_tree


def publication_paths():
    base = "nova_arquitetura_big_query/"
    files = [base + "camada_CONFIG/cria_config.sql"]
    files += [base + "camada_SOR/" + name for name in (
        "raw_google.sql", "raw_meta.sql", "raw_tiktok.sql", "raw_kwai.sql",
        "raw_alcance_semanal.sql", "raw_alcance_semanal_grupo_anuncio.sql")]
    files += [base + "camada_SOT/" + name for name in (
        "cria_google_particionada.sql", "cria_google_conversions_particionada.sql", "cria_meta_particionada.sql",
        "cria_tiktok_particionada.sql", "cria_kwai_particionada.sql", "cria_alcance_semanal_particionada.sql")]
    files += [base + "camada_SOT/procedures/" + name for name in (
        "sp_atualiza_particionada_google.sql", "sp_atualiza_particionada_google_conversions.sql",
        "sp_atualiza_particionada_meta.sql", "sp_atualiza_particionada_tiktok.sql", "sp_atualiza_particionada_kwai.sql",
        "sp_atualiza_particionada_alcance_semanal.sql", "sp_atualiza_particionada_alcance_semanal_grupo_anuncio.sql")]
    files += [base + "camada_SPEC/procedures/" + name for name in (
        "sp_cria_sumarizada_detalhe.sql", "sp_cria_sumarizada_detalhe_eventos.sql", "sp_cria_semanal.sql",
        "sp_cria_semanal_grupo_anuncio.sql", "sp_cria_mensal.sql", "sp_cria_mensal_anuncio.sql")]
    return files + [base + "orquestracao/sp_run_pipeline_midia.sql"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/cloud.example.json")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    output = ROOT / "rendered_sql"
    render_tree(config, output)
    files = publication_paths()
    if not args.apply:
        print(json.dumps({"mode": "SIMULACAO", "publication_order": files, "calls_refresh": False}, indent=2))
        return
    if any("REPLACE_" in config[key] for key in ("project_id", "data_bucket")):
        raise ValueError("Configure your own project and data bucket before applying")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", config["project_id"]):
        raise ValueError("Invalid project identifier")
    for key in ("dataset_raw", "dataset_work", "dataset_spec", "dataset_config", "dataset_reports"):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", config[key]):
            raise ValueError("Invalid dataset identifier")
    cli = ["bq", f'--project_id={config["project_id"]}', f'--location={config["bigquery_location"]}', "query", "--use_legacy_sql=false"]
    schemas = "\n".join(f'CREATE SCHEMA IF NOT EXISTS `{config["project_id"]}.{config[key]}`;' for key in
                        ("dataset_raw", "dataset_work", "dataset_spec", "dataset_config", "dataset_reports"))
    subprocess.run(cli, input=schemas, text=True, check=True)
    for name in files:
        subprocess.run(cli, input=(output / name).read_text(encoding="utf-8"), text=True, check=True)
    print("SQL publication complete. Review and run orquestracao/roda_pipeline.sql separately.")


if __name__ == "__main__":
    main()
