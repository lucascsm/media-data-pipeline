"""Render all SQL templates locally; this script never accesses BigQuery."""
from __future__ import annotations
import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def render_tree(config: dict, output: Path) -> int:
    values = {
        "PROJECT_ID": config["project_id"], "DATA_BUCKET": config["data_bucket"],
        "ASSETS_BUCKET": config["assets_bucket"],
        "DATASET_RAW": config["dataset_raw"], "DATASET_WORK": config["dataset_work"],
        "DATASET_SPEC": config["dataset_spec"], "DATASET_CONFIG": config["dataset_config"],
        "DATASET_REPORTS": config["dataset_reports"],
        "VIEW_ALIAS": config.get("view_alias", "demo_search_alias"),
        "VIEW_CANONICAL": config.get("view_canonical", "demo-search"),
        "SPECIAL_PRODUCT": config.get("special_product", "Produto C"),
        "SPECIAL_PRODUCT_NORMALIZED": config.get("special_product_normalized", "produto c"),
        "DIAGNOSTIC_WEEK": config.get("diagnostic_week", "2025-01-06"),
        "DIAGNOSTIC_WEEK_LABEL": config.get("diagnostic_week_label", "06/01/2025"),
        "DIAGNOSTIC_START": config.get("diagnostic_start", "2025-01-01"),
        "DIAGNOSTIC_END": config.get("diagnostic_end", "2025-01-14"),
        "RECOVERY_CUTOFF": config.get("recovery_cutoff", "2025-01-06"),
    }
    for key, value in values.items():
        if not isinstance(value, str) or not value or any(c in value for c in ("'", '"', "`", "\n", "\r", "\\", "$")):
            raise ValueError(f"Unsafe SQL template value: {key}")
        if key.startswith("DATASET_") and not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", value):
            raise ValueError(f"Invalid dataset identifier: {key}")
    if output.resolve() == ROOT or ROOT in output.resolve().parents and output.resolve().relative_to(ROOT).parts[0] != "rendered_sql":
        raise ValueError("Inside the repository, render only under rendered_sql/")
    count = 0
    source_paths = list((ROOT / "nova_arquitetura_big_query").rglob("*.sql"))
    source_paths += list((ROOT / "docs").glob("*.sql"))
    source_paths += [p for p in (ROOT / "fluxo_antigo").iterdir() if p.is_file() and (p.suffix == ".sql" or p.suffix == "")]
    for path in source_paths:
        text = path.read_text(encoding="utf-8")
        def replace(match):
            name = match.group(1)
            if name not in values:
                raise ValueError(f"Unknown template parameter: {name}")
            return values[name]
        rendered = re.sub(r"\$\{([A-Z_0-9]+)\}", replace, text)
        destination = output / path.relative_to(ROOT)
        if destination.suffix == "":
            destination = destination.with_suffix(".sql")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rendered, encoding="utf-8")
        count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/cloud.example.json")
    parser.add_argument("--output", type=Path, default=ROOT / "rendered_sql")
    args = parser.parse_args()
    count = render_tree(json.loads(args.config.read_text(encoding="utf-8-sig")), args.output)
    print(f"Rendered {count} SQL files locally. No SQL was published or executed.")


if __name__ == "__main__":
    main()
