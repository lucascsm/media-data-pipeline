from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml


def align_and_concat_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Align schemas before concatenating Polars DataFrames."""
    valid_frames = [frame for frame in frames if frame is not None]
    if not valid_frames:
        return pl.DataFrame()

    all_columns = sorted({column for frame in valid_frames for column in frame.columns})
    common_types: dict[str, pl.DataType] = {}
    for column in all_columns:
        dtypes = [frame.schema[column] for frame in valid_frames if column in frame.columns]
        first_dtype = dtypes[0] if dtypes else pl.Utf8
        common_types[column] = first_dtype if all(dtype == first_dtype for dtype in dtypes) else pl.Utf8

    aligned_frames = []
    for frame in valid_frames:
        normalized = frame
        for column in all_columns:
            dtype = common_types[column]
            if column in normalized.columns:
                normalized = normalized.with_columns(pl.col(column).cast(dtype).alias(column))
            else:
                normalized = normalized.with_columns(pl.lit(None).cast(dtype).alias(column))
        aligned_frames.append(normalized.select(all_columns))

    return pl.concat(aligned_frames, how="vertical")


def load_yaml_config(path: str | Path) -> dict:
    """Load a UTF-8 YAML file into a Python dictionary."""
    with open(Path(path), "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}


def resolve_selected_ids(selected_ids: list[str] | None, default_ids: list[str]) -> list[str]:
    """Return the requested identifiers or fall back to the default catalog."""
    if not selected_ids:
        return list(default_ids)

    resolved_ids = [str(item).strip() for item in selected_ids if str(item).strip()]
    return resolved_ids if resolved_ids else list(default_ids)
