from __future__ import annotations

import polars as pl


def to_long_event_frame(
    df: pl.DataFrame,
    id_columns: list[str],
    *,
    event_column: str = "evento",
    value_column: str = "valor",
    drop_zero_values: bool = True,
) -> pl.DataFrame:
    existing_id_columns = [column for column in id_columns if column in df.columns]
    value_columns = [column for column in df.columns if column not in existing_id_columns]

    if not value_columns:
        return pl.DataFrame(schema={column: df.schema[column] for column in existing_id_columns})

    df_numeric = df.with_columns([
        pl.col(column).cast(pl.Float64, strict=False).alias(column)
        for column in value_columns
    ])

    df_long = df_numeric.melt(
        id_vars=existing_id_columns,
        value_vars=value_columns,
        variable_name=event_column,
        value_name=value_column,
    )

    filters = [
        pl.col(value_column).is_not_null(),
        pl.col(value_column).is_not_nan(),
    ]
    if drop_zero_values:
        filters.append(pl.col(value_column) != 0)

    return df_long.filter(pl.all_horizontal(filters))

