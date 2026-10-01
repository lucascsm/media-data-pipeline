WITH semanal_grupo AS (
    SELECT
        data_date AS semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        SUM(custo) AS custo,
        SUM(impressoes) AS impressoes,
        SUM(cliques) AS cliques,
        SUM(conversoes) AS conversoes
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal_grupo_anuncio`
    WHERE STARTS_WITH(nome_semana, '2-')
    GROUP BY
        semana_ref,
        produto,
        veiculo
),
detalhe_grupo AS (
    SELECT
        DATE_TRUNC(DATE(data_date), WEEK(MONDAY)) AS semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        SUM(COALESCE(investimento, 0)) AS custo,
        SUM(COALESCE(impressoes, 0)) AS impressoes,
        SUM(COALESCE(cliques, 0)) AS cliques,
        SUM(COALESCE(conversoes, 0)) AS conversoes
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
    GROUP BY
        semana_ref,
        produto,
        veiculo
)
SELECT
    COALESCE(s.semana_ref, d.semana_ref) AS semana_ref,
    COALESCE(s.produto, d.produto) AS produto,
    COALESCE(s.veiculo, d.veiculo) AS veiculo,
    COALESCE(s.custo, 0) AS custo_semanal_grupo,
    COALESCE(d.custo, 0) AS custo_detalhe_grupo,
    COALESCE(s.custo, 0) - COALESCE(d.custo, 0) AS diff_custo,
    COALESCE(s.impressoes, 0) AS impressoes_semanal_grupo,
    COALESCE(d.impressoes, 0) AS impressoes_detalhe_grupo,
    COALESCE(s.impressoes, 0) - COALESCE(d.impressoes, 0) AS diff_impressoes,
    COALESCE(s.cliques, 0) AS cliques_semanal_grupo,
    COALESCE(d.cliques, 0) AS cliques_detalhe_grupo,
    COALESCE(s.cliques, 0) - COALESCE(d.cliques, 0) AS diff_cliques,
    COALESCE(s.conversoes, 0) AS conversoes_semanal_grupo,
    COALESCE(d.conversoes, 0) AS conversoes_detalhe_grupo,
    COALESCE(s.conversoes, 0) - COALESCE(d.conversoes, 0) AS diff_conversoes
FROM semanal_grupo s
FULL OUTER JOIN detalhe_grupo d
    ON s.semana_ref = d.semana_ref
    AND s.produto = d.produto
    AND s.veiculo = d.veiculo
ORDER BY
    semana_ref DESC,
    produto,
    veiculo
;
