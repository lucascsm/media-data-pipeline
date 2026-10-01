-- Diagnostico da divergencia entre a arquitetura antiga e a nova.
-- Ajuste as datas/produto no bloco params e execute os SELECTs separadamente no BigQuery.

WITH params AS (
  SELECT
    DATE '${DIAGNOSTIC_WEEK}' AS semana_ref,
    '${SPECIAL_PRODUCT}' AS produto_ref
)

-- 1) Comparacao semanal por canal.
SELECT
  'novo' AS processo,
  nome_semana,
  produto,
  veiculo AS canal,
  SUM(custo) AS custo,
  SUM(impressoes) AS impressoes,
  SUM(cliques) AS cliques,
  SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
WHERE semana = (SELECT FORMAT_DATE('%d/%m/%Y', semana_ref) FROM params)
  AND produto = (SELECT produto_ref FROM params)
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
  'antigo' AS processo,
  nome_semana,
  produto,
  plataforma AS canal,
  SUM(custo) AS custo,
  SUM(impressoes) AS impressoes,
  SUM(clique) AS cliques,
  SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_REPORTS}.resumo_semanal_pbi`
WHERE PARSE_DATE('%d/%m/%Y', semana) = (SELECT semana_ref FROM params)
  AND produto = (SELECT produto_ref FROM params)
GROUP BY 1, 2, 3, 4
ORDER BY nome_semana, produto, canal, processo;

-- 2) Quebra da base nova antes da semanal.
WITH params AS (
  SELECT
    DATE '${DIAGNOSTIC_START}' AS data_ini,
    DATE '${DIAGNOSTIC_END}' AS data_fim,
    '${SPECIAL_PRODUCT}' AS produto_ref
)
SELECT
  veiculo,
  nivel,
  visao,
  SUM(investimento) AS custo,
  SUM(impressoes) AS impressoes,
  SUM(cliques) AS cliques,
  SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
WHERE data_date BETWEEN (SELECT data_ini FROM params) AND (SELECT data_fim FROM params)
  AND produto = (SELECT produto_ref FROM params)
GROUP BY veiculo, nivel, visao
ORDER BY veiculo, nivel, visao;

-- 3) Drill especifico para Meta de Produto C.
-- Serve para validar se eventos equivalentes estao sendo somados separadamente na SPEC nova.
WITH params AS (
  SELECT
    DATE '${DIAGNOSTIC_START}' AS data_ini,
    DATE '${DIAGNOSTIC_END}' AS data_fim,
    '${SPECIAL_PRODUCT}' AS produto_ref
),
base AS (
  SELECT
    DATE(data) AS data_ref,
    produto,
    nivel,
    nome_campanha,
    nome_grupo_anuncio,
    nome_anuncio,
    LOWER(COALESCE(CAST(evento AS STRING), '')) AS evento,
    SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS valor
  FROM `${PROJECT_ID}.${DATASET_WORK}.meta_all_partitioned_diario`
  WHERE data_date BETWEEN (SELECT data_ini FROM params) AND (SELECT data_fim FROM params)
    AND produto = (SELECT produto_ref FROM params)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
  CASE
    WHEN evento IN ('app_custom_event_fb_mobile_add_to_wishlist', 'add_to_wishlist', 'omni_add_to_wishlist')
      THEN 'alias_add_to_wishlist'
    WHEN evento IN ('start_trial_mobile_app', 'start_trial_total')
      THEN 'alias_start_trial'
    ELSE evento
  END AS grupo_evento,
  evento,
  SUM(valor) AS valor_total
FROM base
GROUP BY 1, 2
ORDER BY valor_total DESC, grupo_evento, evento;

-- 4) Google: campanha do legado vs grupo_anuncio da arquitetura nova.
WITH params AS (
  SELECT
    DATE '${DIAGNOSTIC_START}' AS data_ini,
    DATE '${DIAGNOSTIC_END}' AS data_fim,
    '${SPECIAL_PRODUCT}' AS produto_ref
)
SELECT
  'campaign_level' AS granularidade,
  SUM(COALESCE(SAFE_CAST(cliques AS FLOAT64), 0)) AS cliques,
  SUM(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0)) AS impressoes
FROM `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario`
WHERE data_date BETWEEN (SELECT data_ini FROM params) AND (SELECT data_fim FROM params)
  AND produto = (SELECT produto_ref FROM params)
  AND LOWER(COALESCE(nivel, '')) = 'campanha'

UNION ALL

SELECT
  'grupo_anuncio' AS granularidade,
  SUM(COALESCE(SAFE_CAST(cliques AS FLOAT64), 0)) AS cliques,
  SUM(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0)) AS impressoes
FROM `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario`
WHERE data_date BETWEEN (SELECT data_ini FROM params) AND (SELECT data_fim FROM params)
  AND produto = (SELECT produto_ref FROM params)
  AND LOWER(COALESCE(nivel, '')) = 'grupo_anuncio';
