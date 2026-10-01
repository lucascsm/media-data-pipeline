DECLARE p_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY);
DECLARE p_end_date DATE DEFAULT CURRENT_DATE();

-- Reconstroi a regra de referencia diretamente da WORK, dentro da mesma
-- janela usada pela SPEC. Google anuncio fica fora porque nome_anuncio nao
-- esta preenchido nessa origem.
CREATE TEMP TABLE nomes_origem AS
SELECT
    data_date,
    produto,
    'meta' AS veiculo,
    LOWER(TRIM(nivel)) AS nivel,
    COALESCE(
        NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
        '__sem_id_cliente__'
    ) AS id_cliente_norm,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        )
        WHEN 'grupo_anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        )
        WHEN 'anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
        )
    END AS id_entidade,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN NULLIF(TRIM(CAST(nome_campanha AS STRING)), '')
        WHEN 'grupo_anuncio' THEN NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '')
        WHEN 'anuncio' THEN NULLIF(TRIM(CAST(nome_anuncio AS STRING)), '')
    END AS nome
FROM `${PROJECT_ID}.${DATASET_WORK}.meta_all_partitioned_diario`
WHERE data_date BETWEEN p_start_date AND p_end_date
    AND LOWER(TRIM(nivel)) IN ('campanha', 'grupo_anuncio', 'anuncio')

UNION ALL

SELECT
    data_date,
    produto,
    'tiktok' AS veiculo,
    LOWER(TRIM(nivel)) AS nivel,
    COALESCE(
        NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
        '__sem_id_cliente__'
    ) AS id_cliente_norm,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        )
        WHEN 'grupo_anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        )
        WHEN 'anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
        )
    END AS id_entidade,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN NULLIF(TRIM(CAST(nome_campanha AS STRING)), '')
        WHEN 'grupo_anuncio' THEN NULLIF(TRIM(CAST(adgroup_name AS STRING)), '')
        WHEN 'anuncio' THEN NULLIF(TRIM(CAST(ad_name AS STRING)), '')
    END AS nome
FROM `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
WHERE data_date BETWEEN p_start_date AND p_end_date
    AND LOWER(TRIM(nivel)) IN ('campanha', 'grupo_anuncio', 'anuncio')

UNION ALL

SELECT
    data_date,
    produto,
    'kwai' AS veiculo,
    LOWER(TRIM(nivel)) AS nivel,
    COALESCE(
        NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
        '__sem_id_cliente__'
    ) AS id_cliente_norm,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        )
        WHEN 'grupo_anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        )
        WHEN 'anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
        )
    END AS id_entidade,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN NULLIF(TRIM(CAST(nome_campanha AS STRING)), '')
        WHEN 'grupo_anuncio' THEN NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '')
        WHEN 'anuncio' THEN NULLIF(TRIM(CAST(anuncio AS STRING)), '')
    END AS nome
FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
WHERE data_date BETWEEN p_start_date AND p_end_date
    AND LOWER(TRIM(nivel)) IN ('campanha', 'grupo_anuncio', 'anuncio')

UNION ALL

SELECT
    data_date,
    produto,
    'google' AS veiculo,
    LOWER(TRIM(nivel)) AS nivel,
    COALESCE(
        NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
        '__sem_id_cliente__'
    ) AS id_cliente_norm,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        )
        WHEN 'grupo_anuncio' THEN COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        )
    END AS id_entidade,
    CASE LOWER(TRIM(nivel))
        WHEN 'campanha' THEN NULLIF(TRIM(CAST(nome_campanha AS STRING)), '')
        WHEN 'grupo_anuncio' THEN NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '')
    END AS nome
FROM `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario`
WHERE data_date BETWEEN p_start_date AND p_end_date
    AND LOWER(TRIM(nivel)) IN ('campanha', 'grupo_anuncio');

CREATE TEMP TABLE nomes_esperados AS
SELECT
    produto,
    veiculo,
    nivel,
    id_cliente_norm,
    id_entidade,
    ARRAY_AGG(
        nome IGNORE NULLS
        ORDER BY data_date DESC, nome DESC
        LIMIT 1
    )[SAFE_OFFSET(0)] AS nome_esperado
FROM nomes_origem
WHERE id_entidade IS NOT NULL
GROUP BY
    produto,
    veiculo,
    nivel,
    id_cliente_norm,
    id_entidade;

-- 1) Deve retornar zero linhas. Evita que um mesmo ID tenha dois nomes atuais
-- por estar associado a mais de uma conta no mesmo produto.
SELECT
    produto,
    veiculo,
    nivel,
    id_entidade,
    COUNT(DISTINCT nome_esperado) AS qtd_nomes_esperados,
    ARRAY_AGG(DISTINCT nome_esperado IGNORE NULLS ORDER BY nome_esperado) AS nomes_esperados
FROM nomes_esperados
GROUP BY
    produto,
    veiculo,
    nivel,
    id_entidade
HAVING COUNT(DISTINCT nome_esperado) > 1;

-- 2) Deve retornar zero linhas depois do reprocessamento. Confere se o nome
-- publicado no nivel da entidade e o ultimo nome nao vazio da WORK.
WITH esperado AS (
    SELECT
        produto,
        veiculo,
        nivel,
        id_entidade,
        ARRAY_AGG(
            nome_esperado IGNORE NULLS
            ORDER BY nome_esperado DESC
            LIMIT 1
        )[SAFE_OFFSET(0)] AS nome_esperado
    FROM nomes_esperados
    GROUP BY
        produto,
        veiculo,
        nivel,
        id_entidade
),
observado AS (
    SELECT
        produto,
        LOWER(veiculo) AS veiculo,
        LOWER(nivel) AS nivel,
        CASE LOWER(nivel)
            WHEN 'campanha' THEN COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            )
            WHEN 'grupo_anuncio' THEN COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            )
            WHEN 'anuncio' THEN COALESCE(
                CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
            )
        END AS id_entidade,
        CASE LOWER(nivel)
            WHEN 'campanha' THEN NULLIF(TRIM(CAST(campanha AS STRING)), '')
            WHEN 'grupo_anuncio' THEN NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '')
            WHEN 'anuncio' THEN NULLIF(TRIM(CAST(anuncio AS STRING)), '')
        END AS nome_observado
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN p_start_date AND p_end_date
        AND LOWER(nivel) IN ('campanha', 'grupo_anuncio', 'anuncio')
        AND NOT (LOWER(veiculo) = 'google' AND LOWER(nivel) = 'anuncio')
)
SELECT
    o.produto,
    o.veiculo,
    o.nivel,
    o.id_entidade,
    o.nome_observado,
    e.nome_esperado
FROM observado o
INNER JOIN esperado e
    USING (produto, veiculo, nivel, id_entidade)
WHERE o.nome_observado IS DISTINCT FROM e.nome_esperado
GROUP BY
    o.produto,
    o.veiculo,
    o.nivel,
    o.id_entidade,
    o.nome_observado,
    e.nome_esperado
ORDER BY
    o.veiculo,
    o.nivel,
    o.produto,
    o.id_entidade;

-- 3) Deve retornar zero linhas. Verifica a propagacao do mesmo nome para
-- linhas de campanha, grupo e anuncio que carregam o mesmo ID.
WITH nomes_publicados AS (
    SELECT
        produto,
        LOWER(veiculo) AS veiculo,
        'campanha' AS nivel,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_entidade,
        NULLIF(TRIM(CAST(campanha AS STRING)), '') AS nome
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN p_start_date AND p_end_date

    UNION ALL

    SELECT
        produto,
        LOWER(veiculo),
        'grupo_anuncio',
        COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        ),
        NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '')
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN p_start_date AND p_end_date

    UNION ALL

    SELECT
        produto,
        LOWER(veiculo),
        'anuncio',
        COALESCE(
            CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
        ),
        NULLIF(TRIM(CAST(anuncio AS STRING)), '')
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN p_start_date AND p_end_date
        AND LOWER(veiculo) != 'google'
)
SELECT
    produto,
    veiculo,
    nivel,
    id_entidade,
    COUNT(DISTINCT nome) AS qtd_nomes,
    ARRAY_AGG(DISTINCT nome IGNORE NULLS ORDER BY nome LIMIT 10) AS nomes
FROM nomes_publicados
WHERE id_entidade IS NOT NULL
GROUP BY
    produto,
    veiculo,
    nivel,
    id_entidade
HAVING COUNT(DISTINCT nome) > 1
ORDER BY
    veiculo,
    nivel,
    produto,
    id_entidade;
