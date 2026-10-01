CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_SPEC}.vw_media_all_detalhe_eventos` AS
WITH eventos_config_raw AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS produto,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
        NULLIF(TRIM(CAST(valor AS STRING)), '') AS evento_original_config,
        NULLIF(TRIM(CAST(valor_amigavel AS STRING)), '') AS evento_amigavel_config,
        COALESCE(
            LOWER(NULLIF(TRIM(JSON_VALUE(TO_JSON_STRING(d), '$.evento_principal')), '')) IN (
                'true',
                '1',
                'sim',
                's',
                'yes',
                'y'
            ),
            FALSE
        ) AS evento_principal
    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos` d
    WHERE LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) = 'evento'
        AND NULLIF(TRIM(CAST(produto AS STRING)), '') IS NOT NULL
        AND NULLIF(TRIM(CAST(veiculo AS STRING)), '') IS NOT NULL
        AND NULLIF(TRIM(CAST(visao AS STRING)), '') IS NOT NULL
        AND NULLIF(TRIM(CAST(valor AS STRING)), '') IS NOT NULL
),
eventos_config_normalizada AS (
    SELECT
        produto,
        veiculo,
        visao,
        evento_original_config,
        evento_amigavel_config,
        CASE
            WHEN veiculo = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(evento_original_config), '.', '_'), '-', '_')) IN (
                    'app_custom_event_fb_mobile_add_to_wishlist',
                    'add_to_wishlist',
                    'omni_add_to_wishlist'
                ) THEN 'alias_add_to_wishlist'
            WHEN veiculo = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(evento_original_config), '.', '_'), '-', '_')) IN (
                    'start_trial_mobile_app',
                    'start_trial_total'
                ) THEN 'alias_start_trial'
            WHEN veiculo IN ('meta', 'tiktok', 'kwai')
                THEN LOWER(REPLACE(REPLACE(TRIM(evento_original_config), '.', '_'), '-', '_'))
            ELSE evento_original_config
        END AS evento_chave,
        evento_principal
    FROM eventos_config_raw
),
eventos_config AS (
    SELECT
        produto,
        veiculo,
        visao,
        evento_chave,
        ARRAY_AGG(
            DISTINCT evento_original_config IGNORE NULLS
            ORDER BY evento_original_config
            LIMIT 1
        )[SAFE_OFFSET(0)] AS evento_original,
        COALESCE(
            ARRAY_AGG(
                DISTINCT evento_amigavel_config IGNORE NULLS
                ORDER BY evento_amigavel_config
                LIMIT 1
            )[SAFE_OFFSET(0)],
            ARRAY_AGG(
                DISTINCT evento_original_config IGNORE NULLS
                ORDER BY evento_original_config
                LIMIT 1
            )[SAFE_OFFSET(0)]
        ) AS evento,
        LOGICAL_OR(evento_principal) AS evento_principal
    FROM eventos_config_normalizada
    GROUP BY
        produto,
        veiculo,
        visao,
        evento_chave
),
eventos_config_qtd AS (
    SELECT
        produto,
        veiculo,
        visao,
        COUNT(DISTINCT evento_chave) AS qtd_eventos
    FROM eventos_config
    GROUP BY
        produto,
        veiculo,
        visao
),
google_eventos_config_produto AS (
    SELECT
        produto,
        evento_chave,
        ARRAY_AGG(
            DISTINCT evento IGNORE NULLS
            ORDER BY evento
            LIMIT 1
        )[SAFE_OFFSET(0)] AS evento,
        LOGICAL_OR(evento_principal) AS evento_principal
    FROM eventos_config
    WHERE veiculo = 'google'
    GROUP BY
        produto,
        evento_chave
),
dimensoes_campanha_base AS (
    SELECT
        DATE(data) AS data,
        data_date,
        produto,
        LOWER(CAST(veiculo AS STRING)) AS veiculo,
        visao,
        campanha AS nome_campanha,
        COALESCE(SAFE_CAST(investimento AS FLOAT64), 0) AS investimento_linha,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha_norm,
        COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        ) AS id_grupo_anuncio_norm,
        -- O ID mantem o vinculo com os eventos mesmo quando a SPEC substitui
        -- o nome historico pelo nome mais recente da campanha.
        COALESCE(
            CONCAT(
                'id:',
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                )
            ),
            CONCAT(
                'nome:',
                LOWER(NULLIF(TRIM(CAST(campanha AS STRING)), ''))
            )
        ) AS chave_campanha
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE
        (
            (
                LOWER(COALESCE(veiculo, '')) = 'google'
                AND LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
                AND NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '') IS NOT NULL
            )
            OR (
                LOWER(COALESCE(veiculo, '')) IN ('meta', 'tiktok', 'kwai')
                AND LOWER(COALESCE(nivel, '')) = 'campanha'
            )
        )
),
dimensoes_campanha_com_custo_semanal AS (
    SELECT
        *,
        SUM(investimento_linha) OVER (
            PARTITION BY
                DATE_TRUNC(data_date, WEEK(MONDAY)),
                produto,
                veiculo,
                visao,
                nome_campanha
        ) AS investimento_campanha_semana
    FROM dimensoes_campanha_base
),
dimensoes_campanha AS (
    SELECT DISTINCT
        data,
        data_date,
        produto,
        veiculo,
        visao,
        nome_campanha,
        id_campanha_norm,
        id_grupo_anuncio_norm,
        chave_campanha
    FROM dimensoes_campanha_com_custo_semanal
    WHERE
        (
            veiculo = 'google'
            -- Replica o efeito do custo > 0 no semanal: a campanha e filtrada
            -- somente depois de consolidar todos os grupos e dias da semana.
            AND investimento_campanha_semana > 0
        )
        OR (
            veiculo IN ('meta', 'tiktok', 'kwai')
            AND investimento_linha > 0
        )
),
meta_eventos AS (
    SELECT
        data_date,
        produto,
        COALESCE(
            CONCAT(
                'id:',
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                )
            ),
            CONCAT(
                'nome:',
                LOWER(NULLIF(TRIM(CAST(nome_campanha AS STRING)), ''))
            )
        ) AS chave_campanha,
        CASE
            WHEN LOWER(REPLACE(REPLACE(TRIM(CAST(evento AS STRING)), '.', '_'), '-', '_')) IN (
                'app_custom_event_fb_mobile_add_to_wishlist',
                'add_to_wishlist',
                'omni_add_to_wishlist'
            ) THEN 'alias_add_to_wishlist'
            WHEN LOWER(REPLACE(REPLACE(TRIM(CAST(evento AS STRING)), '.', '_'), '-', '_')) IN (
                'start_trial_mobile_app',
                'start_trial_total'
            ) THEN 'alias_start_trial'
            ELSE LOWER(REPLACE(REPLACE(TRIM(CAST(evento AS STRING)), '.', '_'), '-', '_'))
        END AS evento_chave,
        MAX(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_WORK}.meta_all_partitioned_diario`
    WHERE LOWER(COALESCE(CAST(nivel AS STRING), '')) = 'campanha'
        AND NULLIF(TRIM(CAST(evento AS STRING)), '') IS NOT NULL
        AND SAFE_CAST(valor AS FLOAT64) IS NOT NULL
    GROUP BY
        data_date,
        produto,
        chave_campanha,
        evento_chave
),
tiktok_eventos AS (
    SELECT
        data_date,
        produto,
        COALESCE(
            CONCAT(
                'id:',
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                )
            ),
            CONCAT(
                'nome:',
                LOWER(NULLIF(TRIM(CAST(nome_campanha AS STRING)), ''))
            )
        ) AS chave_campanha,
        LOWER(REPLACE(REPLACE(TRIM(CAST(evento AS STRING)), '.', '_'), '-', '_')) AS evento_chave,
        SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
    WHERE LOWER(COALESCE(CAST(nivel AS STRING), '')) = 'campanha'
        AND NULLIF(TRIM(CAST(evento AS STRING)), '') IS NOT NULL
        AND SAFE_CAST(valor AS FLOAT64) IS NOT NULL
    GROUP BY
        data_date,
        produto,
        chave_campanha,
        evento_chave
),
tiktok_result_oficial AS (
    -- Reutiliza a conversao oficial ja protegida em media_all_detalhe. Assim, a
    -- regra result/conversion permanece em um unico ponto do pipeline.
    SELECT
        data_date,
        produto,
        visao,
        COALESCE(
            CONCAT(
                'id:',
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                )
            ),
            CONCAT(
                'nome:',
                LOWER(NULLIF(TRIM(CAST(campanha AS STRING)), ''))
            )
        ) AS chave_campanha,
        SUM(COALESCE(conversoes_diretas, conversoes, 0)) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE LOWER(COALESCE(veiculo, '')) = 'tiktok'
        AND LOWER(COALESCE(nivel, '')) = 'campanha'
        AND COALESCE(SAFE_CAST(investimento AS FLOAT64), 0) > 0
    GROUP BY
        data_date,
        produto,
        visao,
        chave_campanha
),
kwai_eventos AS (
    SELECT
        data_date,
        produto,
        COALESCE(
            CONCAT(
                'id:',
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                )
            ),
            CONCAT(
                'nome:',
                LOWER(NULLIF(TRIM(CAST(nome_campanha AS STRING)), ''))
            )
        ) AS chave_campanha,
        LOWER(REPLACE(REPLACE(TRIM(CAST(evento AS STRING)), '.', '_'), '-', '_')) AS evento_chave,
        SUM(COALESCE(SAFE_CAST(valor AS FLOAT64), 0)) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
    WHERE LOWER(COALESCE(CAST(nivel AS STRING), '')) = 'campanha'
        AND NULLIF(TRIM(CAST(evento AS STRING)), '') IS NOT NULL
        AND SAFE_CAST(valor AS FLOAT64) IS NOT NULL
    GROUP BY
        data_date,
        produto,
        chave_campanha,
        evento_chave
),
eventos_campanha AS (
    SELECT
        data_date,
        produto,
        'meta' AS veiculo,
        chave_campanha,
        evento_chave,
        conversoes_diretas
    FROM meta_eventos

    UNION ALL

    SELECT
        data_date,
        produto,
        'tiktok' AS veiculo,
        chave_campanha,
        evento_chave,
        conversoes_diretas
    FROM tiktok_eventos

    UNION ALL

    SELECT
        data_date,
        produto,
        'kwai' AS veiculo,
        chave_campanha,
        evento_chave,
        conversoes_diretas
    FROM kwai_eventos
),
eventos_campanha_configurados AS (
    SELECT
        d.data,
        d.data_date,
        d.produto AS canal,
        d.veiculo,
        d.nome_campanha,
        d.visao,
        cfg.evento,
        CASE
            WHEN d.veiculo = 'tiktok' AND cfg.evento_chave = 'result'
                THEN COALESCE(result_oficial.conversoes_diretas, 0)
            ELSE COALESCE(e.conversoes_diretas, 0)
        END AS conversoes_diretas
    FROM dimensoes_campanha d
    INNER JOIN eventos_config cfg
        ON cfg.produto = d.produto
        AND cfg.veiculo = d.veiculo
        AND cfg.visao = d.visao
        AND cfg.evento_principal
    LEFT JOIN eventos_campanha e
        ON e.data_date = d.data_date
        AND e.produto = d.produto
        AND e.veiculo = d.veiculo
        AND e.chave_campanha = d.chave_campanha
        AND e.evento_chave = cfg.evento_chave
    LEFT JOIN tiktok_result_oficial result_oficial
        ON result_oficial.data_date = d.data_date
        AND result_oficial.produto = d.produto
        AND result_oficial.visao = d.visao
        AND result_oficial.chave_campanha = d.chave_campanha
    WHERE d.veiculo IN ('meta', 'tiktok', 'kwai')
),
google_eventos AS (
    SELECT
        data_date,
        COALESCE(
            CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
        ) AS id_campanha_norm,
        COALESCE(
            CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
            NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
        ) AS id_grupo_anuncio_norm,
        NULLIF(TRIM(CAST(conversion_event AS STRING)), '') AS evento_chave,
        SUM(
            COALESCE(SAFE_CAST(conversoes_by_conversion_date AS FLOAT64), 0)
            + COALESCE(SAFE_CAST(conversao_view AS FLOAT64), 0)
        ) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_WORK}.google_conversions_all_partitioned_diario`
    WHERE LOWER(COALESCE(CAST(nivel AS STRING), '')) = 'grupo_anuncio'
        AND NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '') IS NOT NULL
        AND NULLIF(TRIM(CAST(conversion_event AS STRING)), '') IS NOT NULL
    GROUP BY
        data_date,
        id_campanha_norm,
        id_grupo_anuncio_norm,
        evento_chave
),
google_eventos_configurados AS (
    SELECT
        d.data,
        d.data_date,
        d.produto AS canal,
        d.veiculo,
        d.nome_campanha,
        d.visao,
        cfg_visao.evento AS evento,
        COALESCE(e.conversoes_diretas, 0) AS conversoes_diretas
    FROM dimensoes_campanha d
    INNER JOIN eventos_config cfg_visao
        ON cfg_visao.produto = d.produto
        AND cfg_visao.veiculo = 'google'
        AND cfg_visao.visao = d.visao
        AND cfg_visao.evento_principal
    LEFT JOIN google_eventos e
        ON e.data_date = d.data_date
        AND e.id_grupo_anuncio_norm = d.id_grupo_anuncio_norm
        AND e.evento_chave = cfg_visao.evento_chave
        AND (
            e.id_campanha_norm IS NULL
            OR d.id_campanha_norm IS NULL
            OR e.id_campanha_norm = d.id_campanha_norm
        )
    WHERE d.veiculo = 'google'

    UNION ALL

    -- Mantem o fallback historico do Google por produto quando a visao nao
    -- possui nenhum evento configurado. Sem uma configuracao por visao, nao ha
    -- grade segura para inventar linhas zeradas; somente eventos observados entram.
    SELECT
        d.data,
        d.data_date,
        d.produto AS canal,
        d.veiculo,
        d.nome_campanha,
        d.visao,
        cfg_produto.evento,
        e.conversoes_diretas
    FROM dimensoes_campanha d
    INNER JOIN google_eventos e
        ON e.data_date = d.data_date
        AND e.id_grupo_anuncio_norm = d.id_grupo_anuncio_norm
        AND (
            e.id_campanha_norm IS NULL
            OR d.id_campanha_norm IS NULL
            OR e.id_campanha_norm = d.id_campanha_norm
        )
    LEFT JOIN eventos_config_qtd cfg_qtd
        ON cfg_qtd.produto = d.produto
        AND cfg_qtd.veiculo = 'google'
        AND cfg_qtd.visao = d.visao
    INNER JOIN google_eventos_config_produto cfg_produto
        ON cfg_produto.produto = d.produto
        AND cfg_produto.evento_chave = e.evento_chave
        AND cfg_produto.evento_principal
    WHERE d.veiculo = 'google'
        AND COALESCE(cfg_qtd.qtd_eventos, 0) = 0
),
tiktok_result_config AS (
    SELECT
        produto,
        visao,
        COALESCE(
            ARRAY_AGG(
                DISTINCT evento_amigavel_config IGNORE NULLS
                ORDER BY evento_amigavel_config
                LIMIT 1
            )[SAFE_OFFSET(0)],
            'result'
        ) AS evento
    FROM eventos_config_normalizada
    WHERE veiculo = 'tiktok'
        AND evento_chave = 'result'
    GROUP BY
        produto,
        visao
),
tiktok_result_fallback AS (
    -- Sem evento direto no de/para, preserva a conversao oficial do detalhe
    -- em uma linha tecnica chamada result.
    SELECT
        DATE(d.data) AS data,
        d.data_date,
        d.produto AS canal,
        LOWER(CAST(d.veiculo AS STRING)) AS veiculo,
        d.campanha AS nome_campanha,
        d.visao,
        COALESCE(cfg_result.evento, 'result') AS evento,
        SUM(COALESCE(d.conversoes_diretas, d.conversoes, 0)) AS conversoes_diretas
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe` d
    LEFT JOIN tiktok_result_config cfg_result
        ON cfg_result.produto = d.produto
        AND cfg_result.visao = d.visao
    WHERE LOWER(COALESCE(d.veiculo, '')) = 'tiktok'
        AND LOWER(COALESCE(d.nivel, '')) = 'campanha'
        AND COALESCE(SAFE_CAST(d.investimento AS FLOAT64), 0) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM eventos_config cfg
            WHERE cfg.produto = d.produto
                AND cfg.veiculo = 'tiktok'
                AND cfg.visao = d.visao
                AND cfg.evento_principal
        )
    GROUP BY
        data,
        d.data_date,
        canal,
        veiculo,
        nome_campanha,
        d.visao,
        evento
),
eventos_unificados AS (
    SELECT * FROM eventos_campanha_configurados

    UNION ALL

    SELECT * FROM google_eventos_configurados

    UNION ALL

    SELECT * FROM tiktok_result_fallback
),
eventos_classificados AS (
    SELECT
        data,
        data_date,
        canal,
        CASE
            WHEN veiculo = 'google'
                AND STARTS_WITH(LOWER(TRIM(COALESCE(visao, ''))), 'demand gen')
                THEN 'google demand gen'
            WHEN veiculo = 'google'
                AND STRPOS(LOWER(COALESCE(visao, '')), 'app') > 0
                THEN 'google app'
            WHEN veiculo = 'google' THEN 'google Search'
            ELSE veiculo
        END AS veiculo,
        nome_campanha,
        visao,
        evento,
        conversoes_diretas
    FROM eventos_unificados
)
SELECT
    data,
    nome_campanha,
    visao,
    canal,
    veiculo,
    evento,
    SUM(COALESCE(conversoes_diretas, 0)) AS conversoes_diretas,
    data_date
FROM eventos_classificados
GROUP BY
    data,
    nome_campanha,
    visao,
    canal,
    veiculo,
    evento,
    data_date;

CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
PARTITION BY data_date
CLUSTER BY veiculo, canal, visao AS
SELECT *
FROM `${PROJECT_ID}.${DATASET_SPEC}.vw_media_all_detalhe_eventos`;
