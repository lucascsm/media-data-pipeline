CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_SPEC}.sp_cria_mensal`(
    p_start_date DATE,
    p_end_date DATE
)
BEGIN
    ASSERT p_start_date IS NOT NULL AS 'p_start_date nao pode ser NULL';
    ASSERT p_end_date IS NOT NULL AS 'p_end_date nao pode ser NULL';
    ASSERT p_start_date <= p_end_date AS 'p_start_date deve ser menor ou igual a p_end_date';

    -- Dependencia: `media_all_detalhe` deve estar atualizada para a mesma janela.
    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    ADD COLUMN IF NOT EXISTS frequencia FLOAT64;

    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    ADD COLUMN IF NOT EXISTS conversoes_diretas FLOAT64;

    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    ADD COLUMN IF NOT EXISTS conversoes_cross FLOAT64;

    ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    ADD COLUMN IF NOT EXISTS modalidade STRING;

    DELETE FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    WHERE data_date BETWEEN p_start_date AND p_end_date;

    -- Reclassify historical Google rows outside the incremental window.
    UPDATE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
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

    -- Refresh historical modalidade from the current mapping.
    UPDATE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo` t
    SET modalidade = LOWER(COALESCE(
        (
            SELECT DISTINCT NULLIF(TRIM(cfg.modalidade), '')
            FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos` cfg
            WHERE NULLIF(TRIM(cfg.produto), '') = t.produto
                AND LOWER(NULLIF(TRIM(cfg.veiculo), '')) = CASE WHEN LOWER(t.veiculo) IN ('google app', 'google search', 'google demand gen') THEN 'google' ELSE LOWER(t.veiculo) END
                AND NULLIF(TRIM(cfg.visao), '') = t.visao
                AND NULLIF(TRIM(cfg.modalidade), '') IS NOT NULL
        ),
        CASE
            WHEN LOWER(TRIM(COALESCE(t.produto, ''))) = LOWER('${SPECIAL_PRODUCT}') THEN 'N\u00E3o identificado'
            ELSE 'N\u00E3o se aplica'
        END
    ))
    WHERE TRUE;

    INSERT INTO `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo` (
        data,
        produto,
        veiculo,
        nome_grupo_anuncio,
        visao,
        modalidade,
        custo,
        impressoes,
        cliques,
        parcela_impr,
        frequencia,
        conversoes,
        conversoes_diretas,
        conversoes_cross,
        data_date
    )
    WITH base_classificada AS (
        SELECT
            data,
            produto,
            CASE
                WHEN LOWER(COALESCE(veiculo, '')) = 'google'
                    AND STARTS_WITH(LOWER(TRIM(COALESCE(visao, ''))), 'demand gen')
                    THEN 'google demand gen'
                WHEN LOWER(COALESCE(veiculo, '')) = 'google'
                    AND STRPOS(LOWER(COALESCE(visao, '')), 'app') > 0
                    THEN 'google app'
                WHEN LOWER(COALESCE(veiculo, '')) = 'google' THEN 'google Search'
                ELSE veiculo
            END AS veiculo,
            CASE
                WHEN LOWER(COALESCE(veiculo, '')) = 'google' THEN grupo_anuncio
                ELSE campanha
            END AS nome_grupo_anuncio,
            visao,
            -- Scalar lookup keeps repeated keyword/event rows from multiplying metrics.
            LOWER(COALESCE(
                (
                    SELECT DISTINCT NULLIF(TRIM(cfg.modalidade), '')
                    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos` cfg
                    WHERE NULLIF(TRIM(cfg.produto), '') = d.produto
                        AND LOWER(NULLIF(TRIM(cfg.veiculo), '')) = LOWER(d.veiculo)
                        AND NULLIF(TRIM(cfg.visao), '') = d.visao
                        AND NULLIF(TRIM(cfg.modalidade), '') IS NOT NULL
                ),
                CASE
                    WHEN LOWER(TRIM(COALESCE(d.produto, ''))) = LOWER('${SPECIAL_PRODUCT}') THEN 'N\u00E3o identificado'
                    ELSE 'N\u00E3o se aplica'
                END
            )) AS modalidade,
            investimento,
            impressoes,
            cliques,
            alcance,
            conversoes,
            conversoes_cross,
            data_date
        FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe` d
        WHERE
            (
                (LOWER(COALESCE(veiculo, '')) = 'google' AND LOWER(COALESCE(nivel, '')) = 'grupo_anuncio')
                OR (LOWER(COALESCE(veiculo, '')) IN ('meta', 'tiktok', 'kwai') AND LOWER(COALESCE(nivel, '')) = 'campanha')
            )
            AND data_date BETWEEN p_start_date AND p_end_date
    )
    SELECT
        data,
        produto,
        veiculo,
        nome_grupo_anuncio,
        visao,
        modalidade,
        SUM(COALESCE(investimento, 0)) AS custo,
        SUM(COALESCE(impressoes, 0)) AS impressoes,
        SUM(COALESCE(cliques, 0)) AS cliques,
        CAST(0 AS FLOAT64) AS parcela_impr,
        SAFE_DIVIDE(
            SUM(COALESCE(impressoes, 0)),
            NULLIF(SUM(COALESCE(SAFE_CAST(alcance AS FLOAT64), 0)), 0)
        ) AS frequencia,
        SUM(COALESCE(conversoes, 0)) AS conversoes,
        SUM(COALESCE(conversoes, 0)) AS conversoes_diretas,
        SUM(COALESCE(conversoes_cross, 0)) AS conversoes_cross,
        data_date
    FROM base_classificada
    GROUP BY
        data,
        produto,
        veiculo,
        nome_grupo_anuncio,
        visao,
        modalidade,
        data_date;
END;
