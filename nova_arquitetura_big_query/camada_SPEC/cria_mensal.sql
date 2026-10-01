CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
PARTITION BY data_date AS
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
        COALESCE(
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
        ) AS modalidade,
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
FROM
    base_classificada
GROUP BY
    data,
    produto,
    veiculo,
    nome_grupo_anuncio,
    visao,
    modalidade,
    data_date
;
