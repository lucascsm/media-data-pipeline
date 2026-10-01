-- Backfill pontual da tabela semanal apagada.
-- Mantem somente particoes semanais ate ${RECOVERY_CUTOFF}, sem atualizar datas posteriores.
-- A base diaria tambem fica travada no corte para nao recalcular o periodo seguinte.
DECLARE v_cutoff_date DATE DEFAULT DATE '${RECOVERY_CUTOFF}';

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
            WHEN LOWER(veiculo) IN ('meta', 'tiktok')
                THEN COALESCE(SAFE_CAST(parc_impr AS FLOAT64), 0) * COALESCE(impressoes, 0)
            ELSE 0
        END AS parc_impr_ponderada,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha'
                THEN COALESCE(impressoes, 0)
            WHEN LOWER(veiculo) IN ('meta', 'tiktok')
                THEN COALESCE(impressoes, 0)
            ELSE 0
        END AS parc_impr_impressoes,
        CASE
            WHEN LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) = 'campanha' THEN 0
            ELSE COALESCE(conversoes, 0)
        END AS conversoes
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date <= v_cutoff_date
        AND (
            (LOWER(veiculo) = 'google' AND LOWER(COALESCE(nivel, '')) IN ('grupo_anuncio', 'campanha'))
            OR (LOWER(veiculo) IN ('meta', 'tiktok') AND LOWER(COALESCE(nivel, '')) = 'campanha')
        )
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
    FROM `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned`
    WHERE semana_ref <= v_cutoff_date
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
        SAFE_DIVIDE(SUM(b.parc_impr_ponderada), NULLIF(SUM(b.parc_impr_impressoes), 0)) AS parcela_impr,
        MAX(a.alcance) AS alcance,
        SAFE_DIVIDE(
            SUM(CASE WHEN a.alcance IS NOT NULL THEN b.impressoes ELSE 0 END),
            NULLIF(MAX(a.alcance), 0)
        ) AS frequencia
    FROM base_detalhe_semanal b
    LEFT JOIN alcance_semanal a
        ON a.semana_ref = b.semana_ref
        AND a.produto = b.produto
        AND a.veiculo = b.veiculo
        AND a.id_campanha = b.id_campanha
    GROUP BY
        b.semana_ref,
        b.produto,
        b.veiculo,
        b.visao,
        b.nome_campanha,
        b.id_campanha
),
base_semanal_final AS (
    SELECT
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha,
        SUM(custo) AS custo,
        SUM(impressoes) AS impressoes,
        SUM(cliques) AS cliques,
        SUM(conversoes) AS conversoes,
        SAFE_DIVIDE(SUM(parcela_impr * impressoes), NULLIF(SUM(impressoes), 0)) AS parcela_impr,
        SAFE_DIVIDE(
            SUM(CASE WHEN alcance IS NOT NULL THEN impressoes ELSE 0 END),
            NULLIF(SUM(alcance), 0)
        ) AS frequencia
    FROM base_semanal
    GROUP BY
        semana_ref,
        produto,
        veiculo,
        visao,
        nome_campanha
),
semanas AS (
    SELECT DISTINCT
        semana_ref AS semana_atual
    FROM base_semanal_final
    WHERE semana_ref <= v_cutoff_date
),
chaves_semana AS (
    SELECT DISTINCT
        s.semana_atual,
        b.produto,
        b.veiculo,
        b.visao,
        b.nome_campanha
    FROM semanas s
    JOIN base_semanal_final b
        ON b.semana_ref IN (s.semana_atual, DATE_SUB(s.semana_atual, INTERVAL 7 DAY))
),
linhas_comparativas AS (
    SELECT
        c.semana_atual,
        ord,
        c.produto,
        c.veiculo,
        c.visao,
        c.nome_campanha,
        CASE
            WHEN ord = 1 THEN DATE_SUB(c.semana_atual, INTERVAL 7 DAY)
            ELSE c.semana_atual
        END AS semana_metricas
    FROM chaves_semana c
    CROSS JOIN UNNEST([1, 2]) AS ord
)
SELECT
    FORMAT_DATE('%d/%m/%Y', l.semana_atual) AS semana,
    FORMAT(
        '%d- %s - %s',
        l.ord,
        FORMAT_DATE('%d/%m/%Y', l.semana_metricas),
        FORMAT_DATE('%d/%m/%Y', DATE_ADD(l.semana_metricas, INTERVAL 6 DAY))
    ) AS nome_semana,
    l.produto,
    l.veiculo,
    l.visao,
    l.nome_campanha,
    COALESCE(b.custo, 0) AS custo,
    COALESCE(b.impressoes, 0) AS impressoes,
    COALESCE(b.cliques, 0) AS cliques,
    COALESCE(b.parcela_impr, 0) AS parcela_impr,
    b.frequencia AS frequencia,
    COALESCE(b.conversoes, 0) AS conversoes,
    SAFE_DIVIDE(COALESCE(b.cliques, 0), NULLIF(COALESCE(b.impressoes, 0), 0)) AS ctr,
    SAFE_DIVIDE(COALESCE(b.custo, 0), NULLIF(COALESCE(b.conversoes, 0), 0)) AS cpa,
    SAFE_DIVIDE(COALESCE(b.conversoes, 0), NULLIF(COALESCE(b.cliques, 0), 0)) AS taxa_conversao,
    l.semana_atual AS data_date
FROM linhas_comparativas l
LEFT JOIN base_semanal_final b
    ON b.semana_ref = l.semana_metricas
    AND b.produto = l.produto
    AND b.veiculo = l.veiculo
    AND b.visao = l.visao
    AND b.nome_campanha = l.nome_campanha
WHERE l.semana_atual <= v_cutoff_date;
