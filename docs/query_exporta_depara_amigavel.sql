-- Gera uma base de apoio para a aba de eventos do de/para amigavel.
-- Os campos de nome e descricao amigavel ficam vazios para validacao do negocio.

WITH config AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS produto,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS plataforma,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao_dashboard,
        NULLIF(TRIM(CAST(valor AS STRING)), '') AS valor,
        SAFE_CAST(prioridade AS INT64) AS prioridade,
        LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) AS tipo_linha,
        COALESCE(
            LOWER(NULLIF(TRIM(CAST(evento_principal AS STRING)), '')) IN (
                'true',
                '1',
                'sim',
                's',
                'yes',
                'y'
            ),
            FALSE
        ) AS evento_principal
    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
),
grupos_palavras AS (
    SELECT
        produto,
        plataforma,
        visao_dashboard,
        prioridade,
        STRING_AGG(DISTINCT valor, ' E ' ORDER BY valor) AS palavras_do_grupo
    FROM config
    WHERE tipo_linha = 'palavra'
        AND produto IS NOT NULL
        AND plataforma IS NOT NULL
        AND visao_dashboard IS NOT NULL
        AND valor IS NOT NULL
        AND prioridade IS NOT NULL
    GROUP BY
        produto,
        plataforma,
        visao_dashboard,
        prioridade
),
visoes AS (
    SELECT
        produto,
        plataforma,
        visao_dashboard,
        MIN(prioridade) AS primeira_prioridade,
        STRING_AGG(
            FORMAT('P%d: %s', prioridade, palavras_do_grupo),
            ' OU '
            ORDER BY prioridade
        ) AS criterio_classificacao
    FROM grupos_palavras
    GROUP BY
        produto,
        plataforma,
        visao_dashboard
),
eventos AS (
    SELECT
        produto,
        plataforma,
        visao_dashboard,
        valor AS evento_tecnico,
        LOGICAL_OR(evento_principal) AS evento_principal
    FROM config
    WHERE tipo_linha = 'evento'
        AND produto IS NOT NULL
        AND plataforma IS NOT NULL
        AND visao_dashboard IS NOT NULL
        AND valor IS NOT NULL
    GROUP BY
        produto,
        plataforma,
        visao_dashboard,
        evento_tecnico
)
SELECT
    v.produto,
    v.plataforma,
    CASE
        WHEN LOWER(v.visao_dashboard) LIKE 'search-%' THEN 'Pesquisa'
        WHEN LOWER(v.visao_dashboard) LIKE 'app-web-%' THEN 'Web'
        WHEN LOWER(v.visao_dashboard) LIKE 'app-%' THEN 'Aplicativo'
        ELSE 'Outro'
    END AS canal_amigavel,
    CASE
        WHEN LOWER(v.visao_dashboard) LIKE 'app-android-%' THEN 'Android'
        WHEN LOWER(v.visao_dashboard) LIKE 'app-ios-%' THEN 'iOS'
        WHEN LOWER(v.visao_dashboard) LIKE 'app-web-%' THEN 'Web'
        ELSE 'Nao se aplica'
    END AS sistema,
    REGEXP_REPLACE(
        v.visao_dashboard,
        r'(?i)^(app-(android|ios|web|cpc)-|search-)',
        ''
    ) AS jornada_objetivo_base,
    v.visao_dashboard,
    v.criterio_classificacao,
    v.primeira_prioridade,
    COALESCE(
        e.evento_tecnico,
        CASE
            WHEN v.plataforma = 'tiktok' THEN 'result / conversion'
            WHEN v.plataforma = 'kwai' THEN 'result'
            ELSE '(sem evento configurado)'
        END
    ) AS evento_tecnico,
    CASE
        WHEN e.evento_tecnico IS NULL AND v.plataforma = 'tiktok'
            THEN 'Fallback da plataforma: result; se result = 0 e conversion > 0, usa conversion'
        WHEN e.evento_tecnico IS NULL AND v.plataforma = 'kwai'
            THEN 'Fallback da plataforma: result'
        WHEN e.evento_tecnico IS NULL
            THEN 'Sem evento configurado'
        WHEN v.plataforma IN ('google', 'meta') AND e.evento_principal
            THEN 'Principal / direta'
        WHEN v.plataforma IN ('google', 'meta') AND NOT e.evento_principal
            THEN 'Apoio / cross'
        ELSE 'Configurado / direta'
    END AS tipo_no_semanal,
    CASE
        WHEN e.evento_tecnico IS NULL AND v.plataforma IN ('tiktok', 'kwai') THEN 'Sim'
        WHEN e.evento_tecnico IS NULL THEN 'Nao'
        WHEN v.plataforma IN ('google', 'meta') THEN IF(e.evento_principal, 'Sim', 'Nao')
        ELSE 'Sim'
    END AS entra_na_conversao_oficial,
    CAST(NULL AS STRING) AS nome_amigavel_validado,
    CAST(NULL AS STRING) AS descricao_de_negocio,
    CAST(NULL AS STRING) AS etapa_do_funil,
    CAST(NULL AS STRING) AS responsavel_validacao,
    'Pendente de validacao com negocio' AS status_validacao,
    CAST(NULL AS DATE) AS data_validacao
FROM visoes v
LEFT JOIN eventos e
    ON e.produto = v.produto
    AND e.plataforma = v.plataforma
    AND e.visao_dashboard = v.visao_dashboard
ORDER BY
    v.produto,
    v.plataforma,
    v.primeira_prioridade,
    v.visao_dashboard,
    evento_tecnico;
