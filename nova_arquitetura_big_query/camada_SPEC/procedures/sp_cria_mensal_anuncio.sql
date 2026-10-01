CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_SPEC}.sp_cria_mensal_anuncio`(
    p_start_date DATE,
    p_end_date DATE
)
BEGIN
    ASSERT p_start_date IS NOT NULL AS 'p_start_date nao pode ser NULL';
    ASSERT p_end_date IS NOT NULL AS 'p_end_date nao pode ser NULL';
    ASSERT p_start_date <= p_end_date AS 'p_start_date deve ser menor ou igual a p_end_date';

    -- Dependencia: `media_all_detalhe` deve estar atualizada para a mesma janela.
    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio`
    ADD COLUMN IF NOT EXISTS conversoes_diretas FLOAT64;

    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio`
    ADD COLUMN IF NOT EXISTS conversoes_cross FLOAT64;

    DELETE FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio`
    WHERE data_date BETWEEN p_start_date AND p_end_date;

    INSERT INTO `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio` (
        data,
        produto,
        veiculo,
        anuncio,
        visao,
        custo,
        impressoes,
        cliques,
        parcela_impr,
        conversoes,
        conversoes_diretas,
        conversoes_cross,
        data_date
    )
    SELECT
        data,
        produto,
        veiculo,
        anuncio,
        visao,
        SUM(COALESCE(investimento, 0)) AS custo,
        SUM(COALESCE(impressoes, 0)) AS impressoes,
        SUM(COALESCE(cliques, 0)) AS cliques,
        CAST(0 AS FLOAT64) AS parcela_impr,
        SUM(COALESCE(conversoes, 0)) AS conversoes,
        SUM(COALESCE(conversoes, 0)) AS conversoes_diretas,
        SUM(COALESCE(conversoes_cross, 0)) AS conversoes_cross,
        data_date
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE
        LOWER(COALESCE(nivel, '')) = 'anuncio'
        AND LOWER(COALESCE(veiculo, '')) IN ('meta', 'tiktok', 'kwai')
        AND data_date BETWEEN p_start_date AND p_end_date
    GROUP BY
        data,
        produto,
        veiculo,
        anuncio,
        visao,
        data_date;
END;
