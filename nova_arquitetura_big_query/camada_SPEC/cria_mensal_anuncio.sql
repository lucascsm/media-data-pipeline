CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio`
PARTITION BY data_date AS
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
FROM
    `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
WHERE
    LOWER(COALESCE(nivel, '')) = 'anuncio'
    AND LOWER(COALESCE(veiculo, '')) IN ('meta', 'tiktok', 'kwai')
GROUP BY
    data,
    produto,
    veiculo,
    anuncio,
    visao,
    data_date
;
