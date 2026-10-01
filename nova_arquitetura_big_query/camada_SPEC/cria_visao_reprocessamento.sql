-- Passo 2: cria a view de comparacao semanal ja classificada por visao.
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_REPORTS}.vw_reprocessamento_semanal_com_visao` AS
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
base_reprocessamento_campanha AS (
    SELECT
        'atual' AS origem,
        PARSE_DATE('%d/%m/%Y', semana) AS semana,
        nome_semana,
        produto,
        CASE
            WHEN LOWER(veiculo) IN ('google app', 'google search', 'google demand gen') THEN 'google'
            ELSE LOWER(veiculo)
        END AS plataforma,
        nome_campanha,
        SUM(COALESCE(conversoes, 0)) AS conversoes,
        SUM(COALESCE(conversoes, 0)) AS conversoes_diretas,
        SUM(COALESCE(conversoes_cross, 0)) AS conversoes_cross
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
    WHERE nome_campanha IS NOT NULL
    GROUP BY
        origem,
        semana,
        nome_semana,
        produto,
        plataforma,
        nome_campanha

    UNION ALL

    SELECT
        'foto' AS origem,
        semana,
        nome_semana,
        produto,
        plataforma,
        nome_campanha,
        conversoes,
        conversoes_diretas,
        conversoes_cross
    FROM (
        SELECT
            semana,
            nome_semana,
            produto,
            CASE
                WHEN LOWER(plataforma) IN ('google app', 'google search', 'google demand gen') THEN 'google'
                ELSE LOWER(plataforma)
            END AS plataforma,
            nome_campanha,
            COALESCE(conversoes, 0) AS conversoes,
            COALESCE(conversoes, 0) AS conversoes_diretas,
            COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(r), '$.conversoes_cross') AS FLOAT64), 0) AS conversoes_cross,
            ROW_NUMBER() OVER (
                PARTITION BY
                    semana,
                    nome_semana,
                    produto,
                    LOWER(plataforma),
                    COALESCE(nome_campanha, '')
                ORDER BY
                    dia_foto DESC
            ) AS rn_foto
        FROM `${PROJECT_ID}.${DATASET_REPORTS}.reprocessamento_semanal` r
        WHERE nome_campanha IS NOT NULL
    )
    WHERE rn_foto = 1
),
base_linhas AS (
    SELECT
        base.*,
        LOWER(COALESCE(base.nome_campanha, '')) AS texto_base,
        TO_JSON_STRING(STRUCT(
            base.origem,
            base.semana,
            base.nome_semana,
            base.produto,
            base.plataforma,
            base.nome_campanha
        )) AS chave_visao
    FROM base_reprocessamento_campanha base
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
        base.conversoes,
        base.conversoes_diretas,
        base.conversoes_cross
    FROM base_linhas base
    LEFT JOIN visao_matches vm
        ON base.chave_visao = vm.chave_visao
        AND vm.rn_visao = 1
)
SELECT
    origem,
    semana,
    nome_semana,
    produto,
    plataforma,
    visao,
    SUM(conversoes) AS conversoes,
    SUM(conversoes_diretas) AS conversoes_diretas,
    SUM(conversoes_cross) AS conversoes_cross
FROM base_com_visao
GROUP BY
    origem,
    semana,
    nome_semana,
    produto,
    plataforma,
    visao;
