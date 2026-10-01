CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal_grupo_anuncio`
PARTITION BY data_date AS
WITH campanha_ref AS (
    SELECT
        DATE(data_date) AS data_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha,
        ARRAY_AGG(NULLIF(TRIM(CAST(campanha AS STRING)), '') IGNORE NULLS ORDER BY campanha LIMIT 1)[SAFE_OFFSET(0)] AS nome_campanha_ref,
        ARRAY_AGG(NULLIF(TRIM(CAST(visao AS STRING)), '') IGNORE NULLS ORDER BY visao LIMIT 1)[SAFE_OFFSET(0)] AS visao_ref
    FROM
        `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE
        LOWER(COALESCE(nivel, '')) = 'campanha'
        AND COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) IS NOT NULL
    GROUP BY
        data_ref,
        produto,
        veiculo,
        id_campanha
),
base_detalhe_grupo_raw AS (
    SELECT
        DATE(data_date) AS data_ref,
        DATE_TRUNC(DATE(data_date), WEEK(MONDAY)) AS semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
        NULLIF(TRIM(CAST(campanha AS STRING)), '') AS nome_campanha,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha,
        NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '') AS nome_grupo_anuncio,
        COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        ) AS id_grupo_anuncio,
        COALESCE(investimento, 0) AS custo,
        COALESCE(impressoes, 0) AS impressoes,
            COALESCE(cliques, 0) AS cliques,
            COALESCE(SAFE_CAST(parc_impr AS FLOAT64), 0) AS parc_impr,
        COALESCE(conversoes, 0) AS conversoes,
        COALESCE(conversoes, 0) AS conversoes_diretas,
        COALESCE(conversoes_cross, 0) AS conversoes_cross
    FROM
        `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE
        LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
        AND (
            NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '') IS NOT NULL
            OR NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '') IS NOT NULL
        )
),
base_detalhe_grupo AS (
    SELECT
        g.semana_ref,
        g.produto,
        g.veiculo,
        CASE
            WHEN LOWER(COALESCE(g.visao, '')) IN ('', 'outro', 'outros')
                THEN COALESCE(c.visao_ref, g.visao)
            ELSE g.visao
        END AS visao,
        COALESCE(g.nome_campanha, c.nome_campanha_ref) AS nome_campanha,
        g.id_campanha,
        g.nome_grupo_anuncio,
        g.id_grupo_anuncio,
        g.custo,
        g.impressoes,
            g.cliques,
            g.parc_impr,
            g.conversoes,
            g.conversoes_diretas,
            g.conversoes_cross
    FROM
        base_detalhe_grupo_raw g
        LEFT JOIN campanha_ref c ON c.data_ref = g.data_ref
        AND c.produto = g.produto
        AND c.veiculo = g.veiculo
        AND c.id_campanha = g.id_campanha
),
base_semanal_chaves AS (
    SELECT
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha,
        id_campanha,
        nome_grupo_anuncio,
        id_grupo_anuncio,
        SUM(custo) AS custo,
        SUM(impressoes) AS impressoes,
        SUM(cliques) AS cliques,
        SUM(conversoes) AS conversoes,
        SUM(conversoes_diretas) AS conversoes_diretas,
        SUM(conversoes_cross) AS conversoes_cross,
        COALESCE(
            SAFE_DIVIDE(
                SUM(parc_impr * impressoes),
                NULLIF(SUM(impressoes), 0)
            ),
            0
        ) AS parcela_impr
    FROM
        base_detalhe_grupo
    GROUP BY
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha,
        id_campanha,
        nome_grupo_anuncio,
        id_grupo_anuncio
),
semanas AS (
    SELECT DISTINCT
        semana_ref AS semana_atual
    FROM
        base_semanal_chaves
),
chaves_semana AS (
    SELECT DISTINCT
        s.semana_atual,
        b.produto,
        b.veiculo,
        b.visao,
        b.nome_campanha,
        b.id_campanha,
        b.nome_grupo_anuncio,
        b.id_grupo_anuncio
    FROM
        semanas s
        JOIN base_semanal_chaves b ON b.semana_ref IN (
            s.semana_atual,
            DATE_SUB(s.semana_atual, INTERVAL 7 DAY)
        )
),
linhas_comparativas AS (
    SELECT
        c.semana_atual,
        ord,
        c.produto,
        c.veiculo,
        c.visao,
        c.nome_campanha,
        c.id_campanha,
        c.nome_grupo_anuncio,
        c.id_grupo_anuncio,
        CASE
            WHEN ord = 1 THEN DATE_SUB(c.semana_atual, INTERVAL 7 DAY)
            ELSE c.semana_atual
        END AS semana_metricas
    FROM
        chaves_semana c
        CROSS JOIN UNNEST([1, 2]) AS ord
),
base_comparativa AS (
    SELECT
        l.semana_atual,
        FORMAT_DATE('%d/%m/%Y', l.semana_atual) AS semana,
        FORMAT(
            '%d- %s - %s',
            l.ord,
            FORMAT_DATE('%d/%m/%Y', l.semana_metricas),
            FORMAT_DATE('%d/%m/%Y', DATE_ADD(l.semana_metricas, INTERVAL 6 DAY))
        ) AS nome_semana,
        l.produto,
        CASE
            WHEN l.veiculo = 'google'
                AND STARTS_WITH(LOWER(TRIM(COALESCE(l.visao, ''))), 'demand gen')
                THEN 'google demand gen'
            WHEN l.veiculo = 'google'
                AND STRPOS(LOWER(COALESCE(l.visao, '')), 'app') > 0
                THEN 'google app'
            WHEN l.veiculo = 'google' THEN 'google Search'
            ELSE l.veiculo
        END AS veiculo,
        l.visao,
        -- Scalar lookup keeps repeated keyword/event rows from multiplying metrics.
        LOWER(COALESCE(
            (
                SELECT DISTINCT NULLIF(TRIM(cfg.modalidade), '')
                FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos` cfg
                WHERE NULLIF(TRIM(cfg.produto), '') = l.produto
                    AND LOWER(NULLIF(TRIM(cfg.veiculo), '')) = LOWER(l.veiculo)
                    AND NULLIF(TRIM(cfg.visao), '') = l.visao
                    AND NULLIF(TRIM(cfg.modalidade), '') IS NOT NULL
            ),
            CASE
                WHEN LOWER(TRIM(COALESCE(l.produto, ''))) = LOWER('${SPECIAL_PRODUCT}') THEN 'N\u00E3o identificado'
                ELSE 'N\u00E3o se aplica'
            END
        )) AS modalidade,
        l.nome_campanha,
        l.nome_grupo_anuncio,
        COALESCE(b.custo, 0) AS custo,
        COALESCE(b.impressoes, 0) AS impressoes,
        COALESCE(b.cliques, 0) AS cliques,
        COALESCE(b.parcela_impr, 0) AS parcela_impr,
        COALESCE(b.conversoes, 0) AS conversoes,
        COALESCE(b.conversoes_diretas, 0) AS conversoes_diretas,
        COALESCE(b.conversoes_cross, 0) AS conversoes_cross
    FROM
        linhas_comparativas l
        LEFT JOIN base_semanal_chaves b ON b.semana_ref = l.semana_metricas
        AND b.produto = l.produto
        AND b.veiculo = l.veiculo
        AND b.visao = l.visao
        AND COALESCE(b.nome_campanha, '__sem_nome_campanha__') = COALESCE(l.nome_campanha, '__sem_nome_campanha__')
        AND COALESCE(b.id_campanha, '__sem_id_campanha__') = COALESCE(l.id_campanha, '__sem_id_campanha__')
        AND COALESCE(b.nome_grupo_anuncio, '__sem_nome_grupo__') = COALESCE(l.nome_grupo_anuncio, '__sem_nome_grupo__')
        AND COALESCE(b.id_grupo_anuncio, '__sem_id_grupo__') = COALESCE(l.id_grupo_anuncio, '__sem_id_grupo__')
)
SELECT
    semana,
    nome_semana,
    produto,
    veiculo,
    visao,
    modalidade,
    nome_campanha,
    nome_grupo_anuncio,
    SUM(custo) AS custo,
    SUM(impressoes) AS impressoes,
    SUM(cliques) AS cliques,
    COALESCE(SAFE_DIVIDE(SUM(parcela_impr * impressoes), NULLIF(SUM(impressoes), 0)), 0) AS parcela_impr,
    CAST(NULL AS FLOAT64) AS alcance,
    CAST(NULL AS FLOAT64) AS frequencia,
    SUM(conversoes) AS conversoes,
    SUM(conversoes_diretas) AS conversoes_diretas,
    SUM(conversoes_cross) AS conversoes_cross,
    SAFE_DIVIDE(SUM(cliques), NULLIF(SUM(impressoes), 0)) AS ctr,
    SAFE_DIVIDE(SUM(custo), NULLIF(SUM(conversoes), 0)) AS cpa,
    SAFE_DIVIDE(SUM(conversoes), NULLIF(SUM(cliques), 0)) AS taxa_conversao,
    semana_atual AS data_date
FROM
    base_comparativa
GROUP BY
    semana_atual,
    semana,
    nome_semana,
    produto,
    veiculo,
    visao,
    modalidade,
    nome_campanha,
    nome_grupo_anuncio
;
