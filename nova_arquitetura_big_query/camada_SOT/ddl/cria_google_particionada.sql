-- DDL de apoio para garantir a tabela particionada de Google na camada SOT.
-- Mantem o schema da origem e adiciona a coluna de particao data_date.

DECLARE v_source_columns STRING;

SET v_source_columns = (
    SELECT STRING_AGG(
        FORMAT('`%s` %s', column_name, data_type),
        ',\n'
        ORDER BY ordinal_position
    )
    FROM `${PROJECT_ID}.${DATASET_RAW}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'all_google'
);

EXECUTE IMMEDIATE FORMAT(
    """
    CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario` (
        %s,
        `data_date` DATE
    )
    PARTITION BY data_date
    """,
    v_source_columns
);
