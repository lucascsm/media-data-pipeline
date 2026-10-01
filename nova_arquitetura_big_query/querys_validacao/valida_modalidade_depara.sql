-- Read-only checks. The first query must return no rows.
-- Run the second query after publishing and executing the updated procedures.
SELECT
    TRIM(produto) AS produto,
    LOWER(TRIM(veiculo)) AS veiculo,
    TRIM(visao) AS visao,
    ARRAY_AGG(DISTINCT NULLIF(TRIM(modalidade), '') IGNORE NULLS) AS modalidades
FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT NULLIF(TRIM(modalidade), '')) > 1;

WITH config AS (
    SELECT DISTINCT
        TRIM(produto) AS produto,
        LOWER(TRIM(veiculo)) AS veiculo,
        TRIM(visao) AS visao,
        NULLIF(TRIM(modalidade), '') AS modalidade
    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
    WHERE NULLIF(TRIM(modalidade), '') IS NOT NULL
),
publicado AS (
    SELECT 'media_all_semanal' AS origem, produto, veiculo, visao, modalidade
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
    UNION ALL
    SELECT 'media_all_resumo', produto, veiculo, visao, modalidade
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
    UNION ALL
    SELECT 'media_all_semanal_grupo_anuncio', produto, veiculo, visao, modalidade
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal_grupo_anuncio`
    UNION ALL
    SELECT 'calcula_reprocessamento', produto, plataforma, visao, modalidade
    FROM `${PROJECT_ID}.${DATASET_SPEC}.calcula_reprocessamento`
),
comparacao AS (
    SELECT
        p.*,
        (
            SELECT DISTINCT cfg.modalidade
            FROM config cfg
            WHERE cfg.produto = p.produto
                AND cfg.veiculo = CASE
                    WHEN LOWER(p.veiculo) IN ('google app', 'google search', 'google demand gen') THEN 'google'
                    ELSE LOWER(p.veiculo)
                END
                AND CASE
                    WHEN p.origem = 'calcula_reprocessamento'
                        AND cfg.visao = '${VIEW_ALIAS}'
                        THEN '${VIEW_CANONICAL}'
                    ELSE cfg.visao
                END = p.visao
        ) AS modalidade_config
    FROM publicado p
)
SELECT
    origem,
    COUNT(*) AS linhas,
    COUNTIF(modalidade_config IS NULL) AS linhas_sem_correspondencia,
    COUNTIF(
        LOWER(modalidade) IS DISTINCT FROM LOWER(COALESCE(
            modalidade_config,
            CASE
                WHEN LOWER(TRIM(produto)) = '${SPECIAL_PRODUCT_NORMALIZED}' THEN 'Não identificado'
                ELSE 'Não se aplica'
            END
        ))
    ) AS divergencias_modalidade
FROM comparacao
GROUP BY origem
ORDER BY origem;
