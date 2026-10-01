CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_WORK}.sp_atualiza_particionada_google_conversions`(
    p_start_date DATE,
    p_end_date DATE
)
BEGIN
    DECLARE v_insert_columns STRING;
    DECLARE v_select_columns STRING;
    
    ASSERT p_start_date IS NOT NULL AS 'p_start_date nao pode ser NULL';
    ASSERT p_end_date IS NOT NULL AS 'p_end_date nao pode ser NULL';
    ASSERT p_start_date <= p_end_date AS 'p_start_date deve ser menor ou igual a p_end_date';

    -- Remove a janela antes de recarregar os dados.
    DELETE FROM `${PROJECT_ID}.${DATASET_WORK}.google_conversions_all_partitioned_diario`
    WHERE data_date BETWEEN p_start_date AND p_end_date;



    -- Colunas explicitas obtidas do schema da tabela destino.
    SET v_insert_columns = (
        SELECT STRING_AGG(FORMAT('`%s`', column_name), ', ' ORDER BY ordinal_position)
        FROM `${PROJECT_ID}.${DATASET_WORK}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'google_conversions_all_partitioned_diario'
    );

    SET v_select_columns = (
        SELECT STRING_AGG(
            CASE
                WHEN column_name = 'data_date' THEN "DATE(SAFE.PARSE_DATETIME('%F', src.data)) AS data_date"
                ELSE FORMAT('src.`%s`', column_name)
            END,
            ', '
            ORDER BY ordinal_position
        )
        FROM `${PROJECT_ID}.${DATASET_WORK}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'google_conversions_all_partitioned_diario'
    );

    EXECUTE IMMEDIATE FORMAT(
        """
        INSERT INTO `${PROJECT_ID}.${DATASET_WORK}.google_conversions_all_partitioned_diario` (%s)
        SELECT %s
        FROM `${PROJECT_ID}.${DATASET_RAW}.all_google_conversions` AS src
        WHERE DATE(SAFE.PARSE_DATETIME('%%F', src.data)) BETWEEN @start_date AND @end_date
        """,
        v_insert_columns,
        v_select_columns
    )
    USING p_start_date AS start_date, p_end_date AS end_date;
END;
