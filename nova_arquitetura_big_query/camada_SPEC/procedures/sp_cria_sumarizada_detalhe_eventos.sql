CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_SPEC}.sp_cria_sumarizada_detalhe_eventos`(
    p_start_date DATE,
    p_end_date DATE
)
BEGIN
    ASSERT p_start_date IS NOT NULL AS 'p_start_date nao pode ser NULL';
    ASSERT p_end_date IS NOT NULL AS 'p_end_date nao pode ser NULL';
    ASSERT p_start_date <= p_end_date AS 'p_start_date deve ser menor ou igual a p_end_date';

    -- Dependencias: a view de eventos e media_all_detalhe devem estar atualizadas.
    CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
    PARTITION BY data_date
    CLUSTER BY veiculo, canal, visao AS
    SELECT *
    FROM `${PROJECT_ID}.${DATASET_SPEC}.vw_media_all_detalhe_eventos`
    WHERE FALSE;

    DELETE FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
    WHERE data_date BETWEEN p_start_date AND p_end_date;

    -- Reclassify historical Google rows outside the incremental window.
    UPDATE `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
    SET veiculo = CASE
        WHEN STARTS_WITH(LOWER(TRIM(COALESCE(visao, ''))), 'demand gen')
            THEN 'google demand gen'
        WHEN STRPOS(LOWER(COALESCE(visao, '')), 'app') > 0
            THEN 'google app'
        ELSE 'google Search'
    END
    WHERE LOWER(COALESCE(veiculo, '')) IN (
        'google',
        'google app',
        'google search',
        'google demand gen'
    );

    INSERT INTO `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` (
        data,
        nome_campanha,
        visao,
        canal,
        veiculo,
        evento,
        conversoes_diretas,
        data_date
    )
    SELECT
        data,
        nome_campanha,
        visao,
        canal,
        veiculo,
        evento,
        conversoes_diretas,
        data_date
    FROM `${PROJECT_ID}.${DATASET_SPEC}.vw_media_all_detalhe_eventos`
    WHERE data_date BETWEEN p_start_date AND p_end_date;
END;
