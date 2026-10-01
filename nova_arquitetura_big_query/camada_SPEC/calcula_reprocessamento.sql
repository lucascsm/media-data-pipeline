CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.calcula_reprocessamento` AS
WITH depara_visao_eventos AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS produto,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
        CASE
            WHEN NULLIF(TRIM(CAST(visao AS STRING)), '') = '${VIEW_ALIAS}'
                THEN '${VIEW_CANONICAL}'
            ELSE NULLIF(TRIM(CAST(visao AS STRING)), '')
        END AS visao,
        NULLIF(TRIM(CAST(valor AS STRING)), '') AS valor,
        NULLIF(TRIM(modalidade), '') AS modalidade,
        SAFE_CAST(prioridade AS INT64) AS prioridade,
        LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) AS tipo_linha
    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
),
visoes_config AS (
    SELECT
        produto,
        veiculo,
        visao,
        prioridade,
        ARRAY_AGG(DISTINCT valor ORDER BY valor) AS palavras_chave
    FROM depara_visao_eventos
    WHERE tipo_linha = 'palavra'
        AND produto IS NOT NULL
        AND veiculo IS NOT NULL
        AND visao IS NOT NULL
        AND valor IS NOT NULL
        AND prioridade IS NOT NULL
    GROUP BY
        produto,
        veiculo,
        visao,
        prioridade
),
base_reprocessamento AS (
    SELECT
        'reprocessamento' AS origem,
        semana,
        nome_semana,
        produto,
        CASE
            WHEN LOWER(plataforma) IN ('google app', 'google search', 'google demand gen') THEN 'google'
            ELSE LOWER(plataforma)
        END AS plataforma,
        nome_campanha,
        dia_foto,
        SUM(COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0)) AS conversoes,
        SUM(COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0)) AS conversoes_diretas,
        SUM(COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(r), '$.conversoes_cross') AS FLOAT64), 0)) AS conversoes_cross
    FROM `${PROJECT_ID}.${DATASET_SPEC}.reprocessamento_semanal` r
    WHERE nome_campanha IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
base_semanal AS (
    SELECT
        'semanal' AS origem,
        PARSE_DATE('%d/%m/%Y', semana) AS semana,
        nome_semana,
        produto,
        CASE
            WHEN LOWER(veiculo) IN ('google app', 'google search', 'google demand gen') THEN 'google'
            ELSE LOWER(veiculo)
        END AS plataforma,
        nome_campanha,
        CAST(NULL AS DATE) AS dia_foto,
        SUM(COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0)) AS conversoes,
        SUM(COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0)) AS conversoes_diretas,
        SUM(COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(s), '$.conversoes_cross') AS FLOAT64), 0)) AS conversoes_cross
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal` s
    WHERE COALESCE(SAFE_CAST(custo AS FLOAT64), 0) > 0
        AND nome_campanha IS NOT NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
),
base_linhas AS (
    SELECT
        base.*,
        LOWER(COALESCE(CAST(base.nome_campanha AS STRING), '')) AS texto_base,
        TO_JSON_STRING(STRUCT(
            base.origem,
            base.semana,
            base.nome_semana,
            base.produto,
            base.plataforma,
            base.nome_campanha,
            base.dia_foto
        )) AS chave_visao
    FROM (
        SELECT *
        FROM base_reprocessamento

        UNION ALL

        SELECT *
        FROM base_semanal
    ) base
),
visao_matches AS (
    SELECT
        chave_visao,
        visao,
        ROW_NUMBER() OVER (
            PARTITION BY chave_visao
            ORDER BY prioridade
        ) AS rn_visao
    FROM (
        SELECT
            base.chave_visao,
            cfg.visao,
            cfg.prioridade,
            ARRAY_LENGTH(cfg.palavras_chave) AS qtd_palavras,
            COUNTIF(base.texto_base LIKE CONCAT('%', LOWER(palavra), '%')) AS qtd_match
        FROM (
            SELECT DISTINCT
                chave_visao,
                produto,
                plataforma,
                texto_base
            FROM base_linhas
        ) base
        INNER JOIN visoes_config cfg
            ON cfg.produto = base.produto
            AND cfg.veiculo = base.plataforma
        CROSS JOIN UNNEST(cfg.palavras_chave) AS palavra
        GROUP BY
            base.chave_visao,
            cfg.visao,
            cfg.prioridade,
            ARRAY_LENGTH(cfg.palavras_chave)
    )
    WHERE qtd_match = qtd_palavras
),
base_com_visao AS (
    SELECT
        base.origem,
        base.semana,
        base.nome_semana,
        base.produto,
        base.plataforma,
        COALESCE(vm.visao, 'Outro') AS visao,
        base.dia_foto,
        base.conversoes,
        base.conversoes_diretas,
        base.conversoes_cross
    FROM base_linhas base
    LEFT JOIN visao_matches vm
        ON base.chave_visao = vm.chave_visao
        AND vm.rn_visao = 1
),
reprocessamento AS (
    SELECT
        semana,
        nome_semana,
        produto,
        plataforma,
        visao,
        dia_foto,
        SUM(conversoes) AS conversoes,
        SUM(conversoes_diretas) AS conversoes_diretas,
        SUM(conversoes_cross) AS conversoes_cross
    FROM base_com_visao
    WHERE origem = 'reprocessamento'
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
),
sumariza_semanal AS (
    SELECT
        semana,
        nome_semana,
        produto,
        plataforma,
        visao,
        SUM(conversoes) AS conversoes,
        SUM(conversoes_diretas) AS conversoes_diretas,
        SUM(conversoes_cross) AS conversoes_cross
    FROM base_com_visao
    WHERE origem = 'semanal'
    GROUP BY
        1,
        2,
        3,
        4,
        5
)
SELECT
    a.semana,
    a.nome_semana,
    a.produto,
    CASE
        WHEN a.plataforma = 'google'
            AND STARTS_WITH(LOWER(TRIM(COALESCE(a.visao, ''))), 'demand gen')
            THEN 'google demand gen'
        WHEN a.plataforma = 'google'
            AND STRPOS(LOWER(COALESCE(a.visao, '')), 'app') > 0
            THEN 'google app'
        WHEN a.plataforma = 'google' THEN 'google Search'
        ELSE a.plataforma
    END AS plataforma,
    a.visao,
    -- Scalar lookup keeps repeated keyword/event rows from multiplying metrics.
    LOWER(COALESCE(
        (
            SELECT DISTINCT NULLIF(TRIM(cfg.modalidade), '')
            FROM depara_visao_eventos cfg
            WHERE NULLIF(TRIM(cfg.produto), '') = a.produto
                AND LOWER(NULLIF(TRIM(cfg.veiculo), '')) = LOWER(a.plataforma)
                AND NULLIF(TRIM(cfg.visao), '') = a.visao
                AND NULLIF(TRIM(cfg.modalidade), '') IS NOT NULL
        ),
        CASE
            WHEN LOWER(TRIM(COALESCE(a.produto, ''))) = LOWER('${SPECIAL_PRODUCT}') THEN 'N\u00E3o identificado'
            ELSE 'N\u00E3o se aplica'
        END
    )) AS modalidade,
    a.dia_foto,
    a.conversoes AS conversoes_reprocessamento,
    b.conversoes AS conversoes_atuais,
    a.conversoes_diretas AS conversoes_diretas_reprocessamento,
    b.conversoes_diretas AS conversoes_diretas_atuais,
    a.conversoes_cross AS conversoes_cross_reprocessamento,
    b.conversoes_cross AS conversoes_cross_atuais
FROM reprocessamento AS a
INNER JOIN sumariza_semanal AS b
    ON a.semana = b.semana
    AND a.nome_semana = b.nome_semana
    AND a.produto = b.produto
    AND a.plataforma = b.plataforma
    AND a.visao = b.visao;
