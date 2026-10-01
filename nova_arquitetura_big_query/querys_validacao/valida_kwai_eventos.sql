DECLARE v_data_inicio DATE DEFAULT DATE '2026-05-04';
DECLARE v_data_fim DATE DEFAULT DATE '2026-05-10';
DECLARE v_semana STRING DEFAULT '04/05/2026';

-- 1) Eventos disponiveis no SOT por nivel.
SELECT
    'sot_eventos' AS bloco,
    nivel,
    evento,
    SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS valor,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
GROUP BY
    nivel,
    evento
ORDER BY
    nivel,
    valor DESC;

-- 2) Diagnostico no raw: sem filtro de evento multiplica performance se o parquet antigo
-- ainda tiver spend repetido nas linhas de evento.
SELECT
    'raw_spend_diagnostico' AS bloco,
    nivel,
    SUM(COALESCE(SAFE_CAST(spend AS FLOAT64), 0)) AS spend_sem_filtro_evento,
    SUM(
        CASE
            WHEN LOWER(COALESCE(CAST(evento AS STRING), '')) = 'result'
                THEN COALESCE(SAFE_CAST(spend AS FLOAT64), 0)
            ELSE 0
        END
    ) AS spend_result,
    COUNT(1) AS linhas,
    COUNTIF(LOWER(COALESCE(CAST(evento AS STRING), '')) = 'result') AS linhas_result
FROM `${PROJECT_ID}.${DATASET_RAW}.all_kwai`
WHERE DATE(SAFE.PARSE_DATETIME('%F', data)) BETWEEN v_data_inicio AND v_data_fim
GROUP BY
    nivel
ORDER BY
    nivel;

-- 3) Result base por nivel, para bater com a metrica Result da plataforma.
SELECT
    'sot_result' AS bloco,
    nivel,
    SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS result_evento,
    SUM(COALESCE(SAFE_CAST(result AS FLOAT64), 0)) AS result_coluna,
    SUM(COALESCE(SAFE_CAST(spend AS FLOAT64), 0)) AS investimento,
    SUM(COALESCE(SAFE_CAST(impressoes AS INT64), 0)) AS impressoes,
    SUM(COALESCE(SAFE_CAST(cliques AS INT64), 0)) AS cliques,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(CAST(evento AS STRING), '')) = 'result'
GROUP BY
    nivel
ORDER BY
    nivel;

-- 4) Loan Disbursal por nivel. Este e o bloco correto para validar loanDisbursal.
SELECT
    'sot_loan_disbursal' AS bloco,
    nivel,
    SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS loan_disbursal,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(CAST(evento AS STRING), '')) = 'loandisbursal'
GROUP BY
    nivel
ORDER BY
    nivel;

-- 5) Linhas por dia. A semana 2026-05-04 a 2026-05-10 deveria ter mais que 5 linhas de campanha.
SELECT
    'sot_result_por_dia' AS bloco,
    data_date,
    nivel,
    SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS result_evento,
    SUM(COALESCE(SAFE_CAST(spend AS FLOAT64), 0)) AS investimento,
    SUM(COALESCE(SAFE_CAST(impressoes AS INT64), 0)) AS impressoes,
    SUM(COALESCE(SAFE_CAST(cliques AS INT64), 0)) AS cliques,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(CAST(evento AS STRING), '')) = 'result'
GROUP BY
    data_date,
    nivel
ORDER BY
    data_date,
    nivel;

-- 6) Detalhe final por nivel, depois do de/para de eventos.
SELECT
    'spec_detalhe' AS bloco,
    nivel,
    SUM(COALESCE(investimento, 0)) AS investimento,
    SUM(COALESCE(impressoes, 0)) AS impressoes,
    SUM(COALESCE(cliques, 0)) AS cliques,
    SUM(COALESCE(conversoes, 0)) AS conversoes,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(veiculo, '')) = 'kwai'
GROUP BY
    nivel
ORDER BY
    nivel;

-- 7) Semanal final. Use a mesma semana exibida no PBI/plataforma.
SELECT
    'spec_semanal' AS bloco,
    nome_semana,
    produto,
    SUM(COALESCE(custo, 0)) AS custo,
    SUM(COALESCE(impressoes, 0)) AS impressoes,
    SUM(COALESCE(cliques, 0)) AS cliques,
    SUM(COALESCE(conversoes, 0)) AS conversoes,
    SUM(COALESCE(alcance, 0)) AS alcance,
    SAFE_DIVIDE(SUM(COALESCE(impressoes, 0)), NULLIF(SUM(COALESCE(alcance, 0)), 0)) AS frequencia
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
WHERE semana = v_semana
    AND LOWER(COALESCE(veiculo, '')) = 'kwai'
GROUP BY
    nome_semana,
    produto
ORDER BY
    nome_semana,
    produto;

-- 8) Diagnostico de explosao: total sem nivel versus total de campanha.
SELECT
    'spec_detalhe_total_sem_nivel' AS bloco,
    SUM(COALESCE(investimento, 0)) AS investimento,
    SUM(COALESCE(impressoes, 0)) AS impressoes,
    SUM(COALESCE(cliques, 0)) AS cliques,
    SUM(COALESCE(conversoes, 0)) AS conversoes,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(veiculo, '')) = 'kwai'
UNION ALL
SELECT
    'spec_detalhe_campanha' AS bloco,
    SUM(COALESCE(investimento, 0)) AS investimento,
    SUM(COALESCE(impressoes, 0)) AS impressoes,
    SUM(COALESCE(cliques, 0)) AS cliques,
    SUM(COALESCE(conversoes, 0)) AS conversoes,
    COUNT(1) AS linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
WHERE data_date BETWEEN v_data_inicio AND v_data_fim
    AND LOWER(COALESCE(veiculo, '')) = 'kwai'
    AND LOWER(COALESCE(nivel, '')) = 'campanha';
