CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
PARTITION BY data_date AS
WITH base_detalhe_semanal AS (
    SELECT
        DATE(data) AS data,
        DATE_TRUNC(DATE(data_date), WEEK(MONDAY)) AS semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        visao,
        campanha AS nome_campanha,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(investimento, 0)
        END AS custo,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(impressoes, 0)
        END AS impressoes,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(cliques, 0)
        END AS cliques,
        COALESCE(SAFE_CAST(parc_impr AS FLOAT64), 0) AS parc_impr,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha'
                THEN COALESCE(SAFE_CAST(parc_impr AS FLOAT64), 0) * COALESCE(impressoes, 0)
            WHEN LOWER(veiculo) IN ('meta', 'tiktok', 'kwai')
                THEN COALESCE(SAFE_CAST(parc_impr AS FLOAT64), 0) * COALESCE(impressoes, 0)
            ELSE 0
        END AS parc_impr_ponderada,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha'
                THEN COALESCE(impressoes, 0)
            WHEN LOWER(veiculo) IN ('meta', 'tiktok', 'kwai')
                THEN COALESCE(impressoes, 0)
            ELSE 0
        END AS parc_impr_impressoes,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(conversoes, 0)
        END AS conversoes,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(conversoes, 0)
        END AS conversoes_diretas,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(conversoes_cross, 0)
        END AS conversoes_cross
    FROM
        `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE
        (LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) IN ('grupo_anuncio', 'campanha'))
        OR

        (LOWER(veiculo) IN ('meta', 'tiktok', 'kwai') AND LOWER(COALESCE(nivel, '')) = 'campanha')
),
alcance_semanal AS (
    SELECT
        semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha,
        MAX(SAFE_CAST(alcance AS FLOAT64)) AS alcance
    FROM
        `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned`
    GROUP BY
        semana_ref,
        produto,
        veiculo,
        id_campanha
),
base_semanal AS (
    SELECT
        b.semana_ref,
        b.produto,
        b.veiculo,
        b.visao,
        b.nome_campanha,
        b.id_campanha,
        SUM(b.custo) AS custo,
        SUM(b.impressoes) AS impressoes,
        SUM(b.cliques) AS cliques,
        SUM(b.conversoes) AS conversoes,
        SUM(b.conversoes_diretas) AS conversoes_diretas,
        SUM(b.conversoes_cross) AS conversoes_cross,
        SAFE_DIVIDE(SUM(b.parc_impr_ponderada), NULLIF(SUM(b.parc_impr_impressoes), 0)) AS parcela_impr
    FROM
        base_detalhe_semanal b
    GROUP BY
        b.semana_ref,
        b.produto,
        b.veiculo,
        b.visao,
        b.nome_campanha,
        b.id_campanha
),
base_semanal_chaves AS (
    SELECT
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha,
        id_campanha,
        SUM(custo) AS custo,
        SUM(impressoes) AS impressoes,
        SUM(cliques) AS cliques,
        SUM(conversoes) AS conversoes,
        SUM(conversoes_diretas) AS conversoes_diretas,
        SUM(conversoes_cross) AS conversoes_cross,
        SAFE_DIVIDE(SUM(parcela_impr * impressoes), NULLIF(SUM(impressoes), 0)) AS parcela_impr
    FROM
        base_semanal
    GROUP BY
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha,
        id_campanha
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
        b.id_campanha
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
        COALESCE(
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
        ) AS modalidade,
        l.nome_campanha,
        COALESCE(b.custo, 0) AS custo,
        COALESCE(b.impressoes, 0) AS impressoes,
        COALESCE(b.cliques, 0) AS cliques,
        COALESCE(b.parcela_impr, 0) AS parcela_impr,
        CASE WHEN b.semana_ref IS NOT NULL THEN a.alcance END AS alcance,
        COALESCE(b.conversoes, 0) AS conversoes,
        COALESCE(b.conversoes_diretas, 0) AS conversoes_diretas,
        COALESCE(b.conversoes_cross, 0) AS conversoes_cross
    FROM
        linhas_comparativas l
        LEFT JOIN base_semanal_chaves b ON b.semana_ref = l.semana_metricas
        AND b.produto = l.produto
        AND b.veiculo = l.veiculo
        AND b.visao = l.visao
        AND b.nome_campanha = l.nome_campanha
        AND COALESCE(b.id_campanha, '__sem_id_campanha__') = COALESCE(l.id_campanha, '__sem_id_campanha__')
        LEFT JOIN alcance_semanal a ON a.semana_ref = l.semana_metricas
        AND a.produto = l.produto
        AND a.veiculo = l.veiculo
        AND COALESCE(a.id_campanha, '__sem_id_campanha__') = COALESCE(l.id_campanha, '__sem_id_campanha__')
)
SELECT
    semana,
    nome_semana,
    produto,
    veiculo,
    visao,
    modalidade,
    nome_campanha,
    SUM(custo) AS custo,
    SUM(impressoes) AS impressoes,
    SUM(cliques) AS cliques,
    COALESCE(SAFE_DIVIDE(SUM(parcela_impr * impressoes), NULLIF(SUM(impressoes), 0)), 0) AS parcela_impr,
    SUM(alcance) AS alcance,
    SAFE_DIVIDE(
        SUM(CASE WHEN alcance IS NOT NULL THEN impressoes ELSE 0 END),
        NULLIF(SUM(alcance), 0)
    ) AS frequencia,
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
    nome_campanha
;
