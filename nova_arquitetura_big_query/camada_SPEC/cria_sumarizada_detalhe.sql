CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
PARTITION BY data_date AS
    WITH depara_visao_eventos AS (
        SELECT
            NULLIF(TRIM(CAST(produto AS STRING)), '') AS produto,
            LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
            NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
            NULLIF(TRIM(CAST(valor AS STRING)), '') AS valor,
            SAFE_CAST(prioridade AS INT64) AS prioridade,
            LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) AS tipo_linha,
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
    eventos_config_raw AS (
        SELECT
            produto,
            veiculo,
            visao,
            valor AS evento,
            LOGICAL_OR(evento_principal) AS evento_principal
        FROM depara_visao_eventos
        WHERE tipo_linha = 'evento'
            AND produto IS NOT NULL
            AND veiculo IS NOT NULL
            AND visao IS NOT NULL
            AND valor IS NOT NULL
        GROUP BY
            produto,
            veiculo,
            visao,
            evento
    ),
    meta_visoes_config AS (
        SELECT
            produto,
            prioridade,
            visao,
            palavras_chave
        FROM visoes_config
        WHERE veiculo = 'meta'
    ),
    meta_eventos_config_raw AS (
        SELECT
            produto,
            visao,
            evento,
            evento_principal
        FROM eventos_config_raw
        WHERE veiculo = 'meta'
    ),
    meta_eventos_config_base AS (
        SELECT DISTINCT
            produto,
            visao,
            LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) AS evento_base,
            evento_principal,
            CASE
                WHEN LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) IN (
                    'app_custom_event_fb_mobile_add_to_wishlist',
                    'add_to_wishlist'
                ) THEN 'alias_add_to_wishlist'
                WHEN LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) = 'start_trial_mobile_app'
                    THEN 'alias_start_trial'
                ELSE LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_'))
            END AS evento_grupo
        FROM meta_eventos_config_raw
    ),
    meta_eventos_config AS (
        SELECT
            produto,
            visao,
            evento_grupo,
            evento,
            LOGICAL_OR(evento_principal) AS evento_principal
        FROM (
            SELECT
                produto,
                visao,
                evento_grupo,
                evento,
                evento_principal
            FROM meta_eventos_config_base,
            UNNEST(
                CASE
                    WHEN evento_grupo = 'alias_add_to_wishlist' THEN [
                        'app_custom_event_fb_mobile_add_to_wishlist',
                        'add_to_wishlist',
                        'omni_add_to_wishlist'
                    ]
                    WHEN evento_grupo = 'alias_start_trial' THEN [
                        'start_trial_mobile_app',
                        'start_trial_total'
                    ]
                    ELSE [evento_base]
                END
            ) AS evento
        )
        GROUP BY
            produto,
            visao,
            evento_grupo,
            evento
    ),
    tiktok_visoes_config AS (
        SELECT
            produto,
            prioridade,
            visao,
            palavras_chave
        FROM visoes_config
        WHERE veiculo = 'tiktok'
    ),
    tiktok_eventos_config_raw AS (
        SELECT
            produto,
            visao,
            evento
        FROM eventos_config_raw
        WHERE veiculo = 'tiktok'
            -- result continua sendo metrica tecnica de fallback. A linha no
            -- de/para existe apenas para nomear o fluxo detalhe_eventos.
            AND LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) != 'result'
    ),
    tiktok_eventos_config AS (
        SELECT DISTINCT
            produto,
            visao,
            LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) AS evento
        FROM tiktok_eventos_config_raw
    ),
    tiktok_eventos_visao AS (
        SELECT
            produto,
            visao,
            COUNT(1) AS qtd_eventos
        FROM tiktok_eventos_config
        GROUP BY
            produto,
            visao
    ),
    kwai_visoes_config AS (
        SELECT
            produto,
            prioridade,
            visao,
            palavras_chave
        FROM visoes_config
        WHERE veiculo = 'kwai'
    ),
    kwai_eventos_config_raw AS (
        SELECT
            produto,
            visao,
            evento
        FROM eventos_config_raw
        WHERE veiculo = 'kwai'
    ),
    kwai_eventos_config AS (
        SELECT DISTINCT
            produto,
            visao,
            LOWER(REPLACE(REPLACE(TRIM(evento), '.', '_'), '-', '_')) AS evento
        FROM kwai_eventos_config_raw
    ),
    kwai_eventos_visao AS (
        SELECT
            produto,
            visao,
            COUNT(1) AS qtd_eventos
        FROM kwai_eventos_config
        GROUP BY
            produto,
            visao
    ),
    meta_base AS (
        SELECT
            *
        FROM `${PROJECT_ID}.${DATASET_WORK}.meta_all_partitioned_diario`
    ),
    -- Padroniza cada hierarquia pelo ultimo nome nao vazio da janela antes de
    -- classificar a visao. RAW e WORK continuam preservando o nome historico.
    meta_campanha_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_campanha_atual
        FROM
            meta_base
        WHERE
            LOWER(COALESCE(nivel, '')) = 'campanha'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_campanha_norm
    ),
    meta_grupo_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_grupo_anuncio_atual
        FROM
            meta_base
        WHERE
            LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_grupo_norm
    ),
    meta_anuncio_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
            ) AS id_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_anuncio AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_anuncio AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_anuncio_atual
        FROM
            meta_base
        WHERE
            LOWER(COALESCE(nivel, '')) = 'anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_anuncio_norm
    ),
    meta_enriquecida AS (
        SELECT
            m.* REPLACE (
                COALESCE(
                    c.nome_campanha_atual,
                    NULLIF(TRIM(CAST(m.nome_campanha AS STRING)), '')
                ) AS nome_campanha,
                COALESCE(
                    g.nome_grupo_anuncio_atual,
                    NULLIF(TRIM(CAST(m.nome_grupo_anuncio AS STRING)), '')
                ) AS nome_grupo_anuncio,
                COALESCE(
                    a.nome_anuncio_atual,
                    NULLIF(TRIM(CAST(m.nome_anuncio AS STRING)), '')
                ) AS nome_anuncio
            )
        FROM (
            SELECT
                mb.*,
                COALESCE(
                    NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                    '__sem_id_cliente__'
                ) AS id_cliente_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ) AS id_grupo_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ) AS id_campanha_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ) AS id_anuncio_norm
            FROM meta_base mb
        ) m
        LEFT JOIN meta_campanha_nome_atual c
            ON m.produto = c.produto
            AND m.id_cliente_norm = c.id_cliente_norm
            AND m.id_campanha_norm = c.id_campanha_norm
        LEFT JOIN meta_grupo_nome_atual g
            ON m.produto = g.produto
            AND m.id_cliente_norm = g.id_cliente_norm
            AND m.id_grupo_norm = g.id_grupo_norm
        LEFT JOIN meta_anuncio_nome_atual a
            ON m.produto = a.produto
            AND m.id_cliente_norm = a.id_cliente_norm
            AND m.id_anuncio_norm = a.id_anuncio_norm
    ),
    meta_rows_visao AS (
        SELECT
            m.*,
            LOWER(COALESCE(CAST(m.nome_campanha AS STRING), '')) AS texto_base,
            TO_JSON_STRING(m) AS chave_visao
        FROM meta_enriquecida m
    ),
    meta_visao_matches AS (
        SELECT
            chave_visao,
            visao,
            ROW_NUMBER() OVER (
                PARTITION BY chave_visao
                ORDER BY prioridade
            ) AS rn_visao
        FROM (
            SELECT
                meta_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave) AS qtd_palavras,
                COUNTIF(meta_row.texto_base LIKE CONCAT('%', LOWER(palavra), '%')) AS qtd_match
            FROM (
                SELECT DISTINCT
                    chave_visao,
                    produto,
                    texto_base
                FROM meta_rows_visao
            ) meta_row
            INNER JOIN meta_visoes_config cfg
                ON cfg.produto = meta_row.produto
            CROSS JOIN UNNEST(cfg.palavras_chave) AS palavra
            GROUP BY
                meta_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave)
        )
        WHERE
            qtd_match = qtd_palavras
    ),
    visoes_meta AS (
        SELECT
            meta_row.* EXCEPT(texto_base, chave_visao),
            COALESCE(mvm.visao, 'Outro') AS visao
        FROM
            meta_rows_visao meta_row
            LEFT JOIN meta_visao_matches mvm
                ON meta_row.chave_visao = mvm.chave_visao
                AND mvm.rn_visao = 1
    ),
    meta_visao_grupo AS (
        SELECT
            DATE(data) AS data_ref,
            produto,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_norm,
            ANY_VALUE(visao) AS visao_grupo
        FROM
            visoes_meta
        WHERE
            LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            data_ref,
            produto,
            id_grupo_norm
    ),
    visoes_meta_ajustada AS (
        SELECT
            vm.* EXCEPT(id_grupo_norm) REPLACE (
                CASE
                    WHEN LOWER(COALESCE(vm.nivel, '')) = 'anuncio' THEN COALESCE(mvg.visao_grupo, vm.visao)
                    ELSE vm.visao
                END AS visao
            )
        FROM (
            SELECT
                v.*
            FROM visoes_meta v
        ) vm
        LEFT JOIN meta_visao_grupo mvg
            ON DATE(vm.data) = mvg.data_ref
            AND vm.produto = mvg.produto
            AND vm.id_grupo_norm = mvg.id_grupo_norm
    ),
    meta_perf_base AS (
        SELECT
            * EXCEPT(rn_perf)
        FROM (
            SELECT
                vm.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        DATE(vm.data),
                        COALESCE(vm.nivel, ''),
                        COALESCE(vm.produto, ''),
                        COALESCE(vm.visao, ''),
                        COALESCE(CAST(vm.nome_campanha AS STRING), ''),
                        COALESCE(CAST(vm.id_campanha AS STRING), ''),
                        COALESCE(CAST(vm.nome_grupo_anuncio AS STRING), ''),
                        COALESCE(CAST(vm.id_grupo_anuncio AS STRING), ''),
                        COALESCE(CAST(vm.nome_anuncio AS STRING), ''),
                        COALESCE(CAST(vm.id_anuncio AS STRING), ''),
                        vm.data_date
                    ORDER BY
                        LOWER(COALESCE(CAST(vm.evento AS STRING), '')),
                        COALESCE(SAFE_CAST(vm.valor AS FLOAT64), 0) DESC
                ) AS rn_perf
            FROM visoes_meta_ajustada vm
        )
        WHERE
            rn_perf = 1
    ),
    meta_perf_final AS (
        SELECT
            DATE(data) AS data,
            nivel,
            produto,
            visao,
            nome_campanha AS campanha,
            id_campanha,
            nome_grupo_anuncio AS grupo_anuncio,
            id_grupo_anuncio,
            nome_anuncio AS anuncio,
            id_anuncio,
            COALESCE(SAFE_CAST(gasto AS FLOAT64), 0) AS investimento,
            COALESCE(SAFE_CAST(impressoes AS INT64), 0) AS impressoes,
            COALESCE(
                CAST(
                    COALESCE(
                        NULLIF(SAFE_CAST(link_click AS FLOAT64), 0),
                        SAFE_CAST(clicks AS FLOAT64),
                        0
                    ) AS INT64
                ),
                0
            ) AS cliques,
            COALESCE(
                SAFE_DIVIDE(
                    COALESCE(
                        NULLIF(SAFE_CAST(link_click AS FLOAT64), 0),
                        SAFE_CAST(clicks AS FLOAT64),
                        0
                    ),
                    NULLIF(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0), 0)
                ),
                0
            ) AS ctr,
            CAST(0 AS FLOAT64) AS parc_impr,
            SAFE_CAST(alcance AS FLOAT64) AS alcance,
            data_date,
            CONCAT(
                CAST(DATE(data) AS STRING),
                '|',
                COALESCE(nivel, ''),
                '|',
                COALESCE(produto, ''),
                '|',
                COALESCE(visao, ''),
                '|',
                COALESCE(CAST(id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(nome_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(nome_anuncio AS STRING), ''),
                '|',
                CAST(data_date AS STRING)
            ) AS chave_linha
        FROM meta_perf_base
    ),
    meta_conversoes_evento AS (
        SELECT
            DATE(vm.data) AS data,
            vm.nivel,
            vm.produto,
            vm.visao,
            vm.nome_campanha AS campanha,
            vm.id_campanha,
            vm.nome_grupo_anuncio AS grupo_anuncio,
            vm.id_grupo_anuncio,
            vm.nome_anuncio AS anuncio,
            vm.id_anuncio,
            vm.data_date,
            mec.evento_grupo,
            mec.evento_principal,
            MAX(COALESCE(SAFE_CAST(vm.valor AS FLOAT64), 0)) AS conversoes_evento,
            CONCAT(
                CAST(DATE(vm.data) AS STRING),
                '|',
                COALESCE(vm.nivel, ''),
                '|',
                COALESCE(vm.produto, ''),
                '|',
                COALESCE(vm.visao, ''),
                '|',
                COALESCE(CAST(vm.id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vm.id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vm.id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vm.nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vm.nome_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vm.nome_anuncio AS STRING), ''),
                '|',
                CAST(vm.data_date AS STRING)
            ) AS chave_linha
        FROM
            visoes_meta_ajustada vm
            INNER JOIN meta_eventos_config mec
                ON vm.produto = mec.produto
                AND vm.visao = mec.visao
                AND LOWER(COALESCE(CAST(vm.evento AS STRING), '')) = mec.evento
        GROUP BY
            data,
            nivel,
            produto,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date,
            chave_linha,
            evento_grupo,
            evento_principal
    ),
    meta_conversoes AS (
        SELECT
            data,
            nivel,
            produto,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date,
            chave_linha,
            SUM(IF(evento_principal, conversoes_evento, 0)) AS conversoes,
            SUM(IF(evento_principal, conversoes_evento, 0)) AS conversoes_diretas,
            SUM(IF(evento_principal, 0, conversoes_evento)) AS conversoes_cross
        FROM
            meta_conversoes_evento
        GROUP BY
            data,
            nivel,
            produto,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date,
            chave_linha
    ),
    final_meta AS(
        SELECT
            mp.data AS data,
            mp.nivel,
            mp.produto,
            'meta' AS veiculo,
            mp.visao,
            mp.campanha,
            mp.id_campanha,
            mp.grupo_anuncio,
            mp.id_grupo_anuncio,
            mp.anuncio,
            mp.id_anuncio,
            mp.investimento,
            mp.impressoes,
            mp.cliques,
            mp.ctr,
            mp.parc_impr,
            mp.alcance,
            COALESCE(mc.conversoes, 0) AS conversoes,
            COALESCE(mc.conversoes_diretas, 0) AS conversoes_diretas,
            COALESCE(mc.conversoes_cross, 0) AS conversoes_cross,
            mp.data_date
        FROM
            meta_perf_final mp
            LEFT JOIN meta_conversoes mc
                ON mp.chave_linha = mc.chave_linha
    ),
    tiktok_base AS (
        SELECT
            *
        FROM
            `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
    ),
    -- A referencia usa a linha do proprio nivel: campanha, grupo ou anuncio.
    tiktok_campanha_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_campanha_atual
        FROM tiktok_base
        WHERE LOWER(COALESCE(nivel, '')) = 'campanha'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_campanha_norm
    ),
    tiktok_grupo_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(adgroup_name AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(adgroup_name AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_grupo_anuncio_atual
        FROM tiktok_base
        WHERE LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_grupo_anuncio_norm
    ),
    tiktok_anuncio_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
            ) AS id_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(ad_name AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(ad_name AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_anuncio_atual
        FROM tiktok_base
        WHERE LOWER(COALESCE(nivel, '')) = 'anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_anuncio_norm
    ),
    tiktok_enriquecida AS (
        SELECT
            t.* REPLACE (
                COALESCE(
                    c.nome_campanha_atual,
                    NULLIF(TRIM(CAST(t.nome_campanha AS STRING)), '')
                ) AS nome_campanha,
                COALESCE(
                    g.nome_grupo_anuncio_atual,
                    NULLIF(TRIM(CAST(t.adgroup_name AS STRING)), '')
                ) AS adgroup_name,
                COALESCE(
                    a.nome_anuncio_atual,
                    NULLIF(TRIM(CAST(t.ad_name AS STRING)), '')
                ) AS ad_name
            )
        FROM (
            SELECT
                tb.*,
                COALESCE(
                    NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                    '__sem_id_cliente__'
                ) AS id_cliente_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ) AS id_campanha_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ) AS id_grupo_anuncio_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ) AS id_anuncio_norm
            FROM tiktok_base tb
        ) t
        LEFT JOIN tiktok_campanha_nome_atual c
            ON t.produto = c.produto
            AND t.id_cliente_norm = c.id_cliente_norm
            AND t.id_campanha_norm = c.id_campanha_norm
        LEFT JOIN tiktok_grupo_nome_atual g
            ON t.produto = g.produto
            AND t.id_cliente_norm = g.id_cliente_norm
            AND t.id_grupo_anuncio_norm = g.id_grupo_anuncio_norm
        LEFT JOIN tiktok_anuncio_nome_atual a
            ON t.produto = a.produto
            AND t.id_cliente_norm = a.id_cliente_norm
            AND t.id_anuncio_norm = a.id_anuncio_norm
    ),
    tiktok_rows_visao AS (
        SELECT
            t.*,
            LOWER(COALESCE(CAST(t.nome_campanha AS STRING), '')) AS texto_base,
            TO_JSON_STRING(t) AS chave_visao
        FROM tiktok_enriquecida t
    ),
    tiktok_visao_matches AS (
        SELECT
            chave_visao,
            visao,
            ROW_NUMBER() OVER (
                PARTITION BY chave_visao
                ORDER BY prioridade
            ) AS rn_visao
        FROM (
            SELECT
                tiktok_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave) AS qtd_palavras,
                COUNTIF(tiktok_row.texto_base LIKE CONCAT('%', LOWER(palavra), '%')) AS qtd_match
            FROM (
                SELECT DISTINCT
                    chave_visao,
                    produto,
                    texto_base
                FROM tiktok_rows_visao
            ) tiktok_row
            INNER JOIN tiktok_visoes_config cfg
                ON cfg.produto = tiktok_row.produto
            CROSS JOIN UNNEST(cfg.palavras_chave) AS palavra
            GROUP BY
                tiktok_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave)
        )
        WHERE
            qtd_match = qtd_palavras
    ),
    visoes_tiktok AS (
        SELECT
            tiktok_row.* EXCEPT(texto_base, chave_visao),
            COALESCE(tvm.visao, 'Outro') AS visao
        FROM
            tiktok_rows_visao tiktok_row
            LEFT JOIN tiktok_visao_matches tvm
                ON tiktok_row.chave_visao = tvm.chave_visao
                AND tvm.rn_visao = 1
    ),
    tiktok_perf_base AS (
        SELECT
            * EXCEPT(rn_perf)
        FROM (
            SELECT
                vt.*,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        DATE(vt.data),
                        COALESCE(vt.nivel, ''),
                        COALESCE(vt.produto, ''),
                        COALESCE(vt.visao, ''),
                        COALESCE(CAST(vt.nome_campanha AS STRING), ''),
                        COALESCE(CAST(vt.id_campanha AS STRING), ''),
                        COALESCE(CAST(vt.adgroup_name AS STRING), ''),
                        COALESCE(CAST(vt.id_grupo_anuncio AS STRING), ''),
                        COALESCE(CAST(vt.ad_name AS STRING), ''),
                        COALESCE(CAST(vt.id_anuncio AS STRING), ''),
                        vt.data_date
                    ORDER BY
                        LOWER(COALESCE(CAST(vt.evento AS STRING), '')),
                        COALESCE(SAFE_CAST(vt.valor AS FLOAT64), 0) DESC
                ) AS rn_perf
            FROM visoes_tiktok vt
        )
        WHERE
            rn_perf = 1
    ),
    tiktok_perf_final AS (
        SELECT
            DATE(data) AS data,
            nivel,
            produto,
            visao,
            nome_campanha AS campanha,
            id_campanha,
            adgroup_name AS grupo_anuncio,
            id_grupo_anuncio,
            ad_name AS anuncio,
            id_anuncio,
            COALESCE(SAFE_CAST(spend AS FLOAT64), 0) AS investimento,
            COALESCE(SAFE_CAST(impressoes AS INT64), 0) AS impressoes,
            COALESCE(SAFE_CAST(cliques AS INT64), 0) AS cliques,
            COALESCE(
                SAFE_DIVIDE(
                    COALESCE(SAFE_CAST(cliques AS FLOAT64), 0),
                    NULLIF(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0), 0)
                ),
                0
            ) AS ctr,
            CAST(0 AS FLOAT64) AS parc_impr,
            SAFE_CAST(alcance AS FLOAT64) AS alcance,
            COALESCE(SAFE_CAST(result AS FLOAT64), 0) AS resultado_base,
            COALESCE(SAFE_CAST(conversion AS FLOAT64), 0) AS conversion_base,
            data_date,
            CONCAT(
                CAST(DATE(data) AS STRING),
                '|',
                COALESCE(nivel, ''),
                '|',
                COALESCE(produto, ''),
                '|',
                COALESCE(visao, ''),
                '|',
                COALESCE(CAST(id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(adgroup_name AS STRING), ''),
                '|',
                COALESCE(CAST(ad_name AS STRING), ''),
                '|',
                CAST(data_date AS STRING)
            ) AS chave_linha
        FROM tiktok_perf_base
    ),
    tiktok_conversoes AS (
        SELECT
            DATE(vt.data) AS data,
            vt.nivel,
            vt.produto,
            vt.visao,
            vt.nome_campanha AS campanha,
            vt.id_campanha,
            vt.adgroup_name AS grupo_anuncio,
            vt.id_grupo_anuncio,
            vt.ad_name AS anuncio,
            vt.id_anuncio,
            vt.data_date,
            SUM(COALESCE(SAFE_CAST(vt.valor AS FLOAT64), 0)) AS conversoes,
            CONCAT(
                CAST(DATE(vt.data) AS STRING),
                '|',
                COALESCE(vt.nivel, ''),
                '|',
                COALESCE(vt.produto, ''),
                '|',
                COALESCE(vt.visao, ''),
                '|',
                COALESCE(CAST(vt.id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vt.id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vt.id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vt.nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vt.adgroup_name AS STRING), ''),
                '|',
                COALESCE(CAST(vt.ad_name AS STRING), ''),
                '|',
                CAST(vt.data_date AS STRING)
            ) AS chave_linha
        FROM
            visoes_tiktok vt
            INNER JOIN tiktok_eventos_config tec
                ON vt.produto = tec.produto
                AND vt.visao = tec.visao
                AND LOWER(COALESCE(CAST(vt.evento AS STRING), '')) = tec.evento
        GROUP BY
            data,
            nivel,
            produto,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date,
            chave_linha
    ),
    final_tiktok AS(
        SELECT
            tp.data AS data,
            tp.nivel,
            tp.produto,
            'tiktok' AS veiculo,
            tp.visao,
            tp.campanha,
            tp.id_campanha,
            tp.grupo_anuncio,
            tp.id_grupo_anuncio,
            tp.anuncio,
            tp.id_anuncio,
            tp.investimento,
            tp.impressoes,
            tp.cliques,
            tp.ctr,
            tp.parc_impr,
            tp.alcance,
            CASE
                WHEN COALESCE(tev.qtd_eventos, 0) > 0 THEN COALESCE(tc.conversoes, 0)
                WHEN COALESCE(tp.resultado_base, 0) = 0 AND COALESCE(tp.conversion_base, 0) > 0 THEN COALESCE(tp.conversion_base, 0)
                ELSE COALESCE(tp.resultado_base, 0)
            END AS conversoes,
            CASE
                WHEN COALESCE(tev.qtd_eventos, 0) > 0 THEN COALESCE(tc.conversoes, 0)
                WHEN COALESCE(tp.resultado_base, 0) = 0 AND COALESCE(tp.conversion_base, 0) > 0 THEN COALESCE(tp.conversion_base, 0)
                ELSE COALESCE(tp.resultado_base, 0)
            END AS conversoes_diretas,
            CAST(0 AS FLOAT64) AS conversoes_cross,
            tp.data_date
        FROM
            tiktok_perf_final tp
            LEFT JOIN tiktok_conversoes tc
                ON tp.chave_linha = tc.chave_linha
            LEFT JOIN tiktok_eventos_visao tev
                ON tp.produto = tev.produto
                AND tp.visao = tev.visao
    ),
    kwai_base AS (
        SELECT
            *
        FROM
            `${PROJECT_ID}.${DATASET_WORK}.kwai_all_partitioned_diario`
    ),
    -- A referencia usa a linha do proprio nivel: campanha, grupo ou anuncio.
    kwai_campanha_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_campanha_atual
        FROM kwai_base
        WHERE LOWER(COALESCE(nivel, '')) = 'campanha'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_campanha_norm
    ),
    kwai_grupo_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(grupo_anuncio AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_grupo_anuncio_atual
        FROM kwai_base
        WHERE LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_grupo_anuncio_norm
    ),
    kwai_anuncio_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
            ) AS id_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(anuncio AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(anuncio AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_anuncio_atual
        FROM kwai_base
        WHERE LOWER(COALESCE(nivel, '')) = 'anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_anuncio_norm
    ),
    kwai_enriquecida AS (
        SELECT
            k.* REPLACE (
                COALESCE(
                    c.nome_campanha_atual,
                    NULLIF(TRIM(CAST(k.nome_campanha AS STRING)), '')
                ) AS nome_campanha,
                COALESCE(
                    g.nome_grupo_anuncio_atual,
                    NULLIF(TRIM(CAST(k.grupo_anuncio AS STRING)), '')
                ) AS grupo_anuncio,
                COALESCE(
                    a.nome_anuncio_atual,
                    NULLIF(TRIM(CAST(k.anuncio AS STRING)), '')
                ) AS anuncio
            )
        FROM (
            SELECT
                kb.*,
                COALESCE(
                    NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                    '__sem_id_cliente__'
                ) AS id_cliente_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ) AS id_campanha_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ) AS id_grupo_anuncio_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
                ) AS id_anuncio_norm
            FROM kwai_base kb
        ) k
        LEFT JOIN kwai_campanha_nome_atual c
            ON k.produto = c.produto
            AND k.id_cliente_norm = c.id_cliente_norm
            AND k.id_campanha_norm = c.id_campanha_norm
        LEFT JOIN kwai_grupo_nome_atual g
            ON k.produto = g.produto
            AND k.id_cliente_norm = g.id_cliente_norm
            AND k.id_grupo_anuncio_norm = g.id_grupo_anuncio_norm
        LEFT JOIN kwai_anuncio_nome_atual a
            ON k.produto = a.produto
            AND k.id_cliente_norm = a.id_cliente_norm
            AND k.id_anuncio_norm = a.id_anuncio_norm
    ),
    kwai_rows_visao AS (
        SELECT
            k.*,
            LOWER(COALESCE(CAST(k.nome_campanha AS STRING), '')) AS texto_base,
            TO_JSON_STRING(k) AS chave_visao
        FROM kwai_enriquecida k
    ),
    kwai_visao_matches AS (
        SELECT
            chave_visao,
            visao,
            ROW_NUMBER() OVER (
                PARTITION BY chave_visao
                ORDER BY prioridade
            ) AS rn_visao
        FROM (
            SELECT
                kwai_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave) AS qtd_palavras,
                COUNTIF(kwai_row.texto_base LIKE CONCAT('%', LOWER(palavra), '%')) AS qtd_match
            FROM (
                SELECT DISTINCT
                    chave_visao,
                    produto,
                    texto_base
                FROM kwai_rows_visao
            ) kwai_row
            INNER JOIN kwai_visoes_config cfg
                ON cfg.produto = kwai_row.produto
            CROSS JOIN UNNEST(cfg.palavras_chave) AS palavra
            GROUP BY
                kwai_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave)
        )
        WHERE
            qtd_match = qtd_palavras
    ),
    visoes_kwai AS (
        SELECT
            kwai_row.* EXCEPT(texto_base, chave_visao),
            COALESCE(kvm.visao, 'Outro') AS visao
        FROM
            kwai_rows_visao kwai_row
            LEFT JOIN kwai_visao_matches kvm
                ON kwai_row.chave_visao = kvm.chave_visao
                AND kvm.rn_visao = 1
    ),
    kwai_perf_base AS (
        SELECT
            data,
            nivel,
            produto,
            visao,
            nome_campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            SUM(COALESCE(SAFE_CAST(spend AS FLOAT64), 0)) AS spend,
            SUM(COALESCE(SAFE_CAST(impressoes AS INT64), 0)) AS impressoes,
            SUM(COALESCE(SAFE_CAST(cliques AS INT64), 0)) AS cliques,
            MAX(SAFE_CAST(alcance AS FLOAT64)) AS alcance,
            SUM(COALESCE(SAFE_CAST(result AS FLOAT64), 0)) AS result,
            SUM(COALESCE(SAFE_CAST(action AS FLOAT64), 0)) AS action,
            data_date
        FROM visoes_kwai
        WHERE
            LOWER(COALESCE(CAST(evento AS STRING), '')) = 'result'
        GROUP BY
            data,
            nivel,
            produto,
            visao,
            nome_campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date
    ),
    kwai_perf_final AS (
        SELECT
            DATE(data) AS data,
            nivel,
            produto,
            visao,
            nome_campanha AS campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            COALESCE(SAFE_CAST(spend AS FLOAT64), 0) AS investimento,
            COALESCE(SAFE_CAST(impressoes AS INT64), 0) AS impressoes,
            COALESCE(SAFE_CAST(cliques AS INT64), 0) AS cliques,
            COALESCE(
                SAFE_DIVIDE(
                    COALESCE(SAFE_CAST(cliques AS FLOAT64), 0),
                    NULLIF(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0), 0)
                ),
                0
            ) AS ctr,
            CAST(0 AS FLOAT64) AS parc_impr,
            SAFE_CAST(alcance AS FLOAT64) AS alcance,
            COALESCE(SAFE_CAST(result AS FLOAT64), 0) AS resultado_base,
            data_date,
            CONCAT(
                CAST(DATE(data) AS STRING),
                '|',
                COALESCE(nivel, ''),
                '|',
                COALESCE(produto, ''),
                '|',
                COALESCE(visao, ''),
                '|',
                COALESCE(CAST(id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(anuncio AS STRING), ''),
                '|',
                CAST(data_date AS STRING)
            ) AS chave_linha
        FROM kwai_perf_base
    ),
    kwai_conversoes AS (
        SELECT
            DATE(vk.data) AS data,
            vk.nivel,
            vk.produto,
            vk.visao,
            vk.nome_campanha AS campanha,
            vk.id_campanha,
            vk.grupo_anuncio,
            vk.id_grupo_anuncio,
            vk.anuncio,
            vk.id_anuncio,
            vk.data_date,
            SUM(COALESCE(SAFE_CAST(vk.valor AS FLOAT64), 0)) AS conversoes,
            CONCAT(
                CAST(DATE(vk.data) AS STRING),
                '|',
                COALESCE(vk.nivel, ''),
                '|',
                COALESCE(vk.produto, ''),
                '|',
                COALESCE(vk.visao, ''),
                '|',
                COALESCE(CAST(vk.id_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vk.id_grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vk.id_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vk.nome_campanha AS STRING), ''),
                '|',
                COALESCE(CAST(vk.grupo_anuncio AS STRING), ''),
                '|',
                COALESCE(CAST(vk.anuncio AS STRING), ''),
                '|',
                CAST(vk.data_date AS STRING)
            ) AS chave_linha
        FROM
            visoes_kwai vk
            INNER JOIN kwai_eventos_config kec
                ON vk.produto = kec.produto
                AND vk.visao = kec.visao
                AND LOWER(COALESCE(CAST(vk.evento AS STRING), '')) = kec.evento
        GROUP BY
            data,
            nivel,
            produto,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            data_date,
            chave_linha
    ),
    final_kwai AS(
        SELECT
            kp.data AS data,
            kp.nivel,
            kp.produto,
            'kwai' AS veiculo,
            kp.visao,
            kp.campanha,
            kp.id_campanha,
            kp.grupo_anuncio,
            kp.id_grupo_anuncio,
            kp.anuncio,
            kp.id_anuncio,
            kp.investimento,
            kp.impressoes,
            kp.cliques,
            kp.ctr,
            kp.parc_impr,
            kp.alcance,
            CASE
                WHEN COALESCE(kev.qtd_eventos, 0) > 0 THEN COALESCE(kc.conversoes, 0)
                ELSE COALESCE(kp.resultado_base, 0)
            END AS conversoes,
            CASE
                WHEN COALESCE(kev.qtd_eventos, 0) > 0 THEN COALESCE(kc.conversoes, 0)
                ELSE COALESCE(kp.resultado_base, 0)
            END AS conversoes_diretas,
            CAST(0 AS FLOAT64) AS conversoes_cross,
            kp.data_date
        FROM
            kwai_perf_final kp
            LEFT JOIN kwai_conversoes kc
                ON kp.chave_linha = kc.chave_linha
            LEFT JOIN kwai_eventos_visao kev
                ON kp.produto = kev.produto
                AND kp.visao = kev.visao
    ),
    google_visoes_config AS (
        SELECT
            produto,
            prioridade,
            visao,
            palavras_chave
        FROM visoes_config
        WHERE veiculo = 'google'
    ),
    google_eventos_config_raw AS (
        SELECT
            produto,
            visao,
            evento,
            evento_principal
        FROM eventos_config_raw
        WHERE veiculo = 'google'
    ),
    google_eventos_config AS (
        SELECT
            produto,
            visao,
            evento,
            LOGICAL_OR(evento_principal) AS evento_principal
        FROM google_eventos_config_raw
        GROUP BY
            produto,
            visao,
            evento
    ),
    google_eventos_produto AS (
        SELECT
            produto,
            evento,
            LOGICAL_OR(evento_principal) AS evento_principal
        FROM
            google_eventos_config
        GROUP BY
            produto,
            evento
    ),
    google_eventos_visao AS (
        SELECT
            produto,
            visao,
            COUNT(1) AS qtd_eventos
        FROM
            google_eventos_config
        GROUP BY
            produto,
            visao
    ),
    google_base AS (
        SELECT
            *
        FROM
            `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario`
    ),
    -- Google nao publica nome_anuncio na WORK atual; por isso somente campanha
    -- e grupo sao padronizados neste fluxo.
    google_campanha_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_campanha AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_campanha_atual
        FROM google_base
        WHERE LOWER(COALESCE(nivel, '')) = 'campanha'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_campanha_norm
    ),
    google_grupo_nome_atual AS (
        SELECT
            produto,
            COALESCE(
                NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                '__sem_id_cliente__'
            ) AS id_cliente_norm,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio_norm,
            ARRAY_AGG(
                NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '') IGNORE NULLS
                ORDER BY
                    data_date DESC,
                    NULLIF(TRIM(CAST(nome_grupo_anuncio AS STRING)), '') DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] AS nome_grupo_anuncio_atual
        FROM google_base
        WHERE LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            produto,
            id_cliente_norm,
            id_grupo_anuncio_norm
    ),
    google_enriquecida AS (
        SELECT
            g.* REPLACE (
                COALESCE(
                    c.nome_campanha_atual,
                    NULLIF(TRIM(CAST(g.nome_campanha AS STRING)), '')
                ) AS nome_campanha,
                COALESCE(
                    ga.nome_grupo_anuncio_atual,
                    NULLIF(TRIM(CAST(g.nome_grupo_anuncio AS STRING)), '')
                ) AS nome_grupo_anuncio
            )
        FROM (
            SELECT
                gb.*,
                COALESCE(
                    NULLIF(TRIM(CAST(id_cliente AS STRING)), ''),
                    '__sem_id_cliente__'
                ) AS id_cliente_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
                ) AS id_campanha_norm,
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ) AS id_grupo_anuncio_norm
            FROM google_base gb
        ) g
        LEFT JOIN google_campanha_nome_atual c
            ON g.produto = c.produto
            AND g.id_cliente_norm = c.id_cliente_norm
            AND g.id_campanha_norm = c.id_campanha_norm
        LEFT JOIN google_grupo_nome_atual ga
            ON g.produto = ga.produto
            AND g.id_cliente_norm = ga.id_cliente_norm
            AND g.id_grupo_anuncio_norm = ga.id_grupo_anuncio_norm
    ),
    google_perf_detalhe AS (
        SELECT
            DATE(data) AS data,
            nivel,
            CAST(id_cliente AS STRING) AS id_cliente,
            produto,
            'google' AS veiculo,
            nome_campanha AS campanha,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha,
            nome_grupo_anuncio AS grupo_anuncio,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio,
            nome_anuncio AS anuncio,
            COALESCE(
                CAST(SAFE_CAST(id_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_anuncio AS STRING)), '')
            ) AS id_anuncio,
            SUM(COALESCE(SAFE_CAST(custo AS FLOAT64), 0)) AS investimento,
            SUM(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0)) AS impressoes,
            SUM(COALESCE(SAFE_CAST(cliques AS FLOAT64), 0)) AS cliques,
            AVG(COALESCE(SAFE_CAST(search_impression_share AS FLOAT64), 0)) AS parc_impr,
            AVG(SAFE_CAST(alcance AS FLOAT64)) AS alcance,
            SAFE_DIVIDE(
                SUM(COALESCE(SAFE_CAST(cliques AS FLOAT64), 0)),
                NULLIF(SUM(COALESCE(SAFE_CAST(impressoes AS FLOAT64), 0)), 0)
            ) AS ctr,
            data_date
        FROM
            google_enriquecida
        GROUP BY
            data,
            nivel,
            id_cliente,
            produto,
            nome_campanha,
            id_campanha,
            nome_grupo_anuncio,
            id_grupo_anuncio,
            nome_anuncio,
            id_anuncio,
            data_date
    ),
    google_rows_visao AS (
        SELECT
            p.*,
            LOWER(COALESCE(p.campanha, '')) AS texto_base,
            TO_JSON_STRING(p) AS chave_visao
        FROM google_perf_detalhe p
    ),
    google_visao_matches AS (
        SELECT
            chave_visao,
            visao,
            ROW_NUMBER() OVER (
                PARTITION BY chave_visao
                ORDER BY prioridade
            ) AS rn_visao
        FROM (
            SELECT
                google_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave) AS qtd_palavras,
                COUNTIF(google_row.texto_base LIKE CONCAT('%', LOWER(palavra), '%')) AS qtd_match
            FROM (
                SELECT DISTINCT
                    chave_visao,
                    produto,
                    texto_base
                FROM google_rows_visao
            ) google_row
            INNER JOIN google_visoes_config cfg
                ON cfg.produto = google_row.produto
            CROSS JOIN UNNEST(cfg.palavras_chave) AS palavra
            GROUP BY
                google_row.chave_visao,
                cfg.visao,
                cfg.prioridade,
                ARRAY_LENGTH(cfg.palavras_chave)
        )
        WHERE
            qtd_match = qtd_palavras
    ),
    google_perf_classificada AS (
        SELECT
            p.data,
            p.nivel,
            p.id_cliente,
            p.produto,
            p.veiculo,
            COALESCE(gvm.visao, 'Outro') AS visao,
            p.campanha,
            p.id_campanha,
            p.grupo_anuncio,
            p.id_grupo_anuncio,
            p.anuncio,
            p.id_anuncio,
            p.investimento,
            p.impressoes,
            p.cliques,
            p.ctr,
            p.parc_impr,
            p.alcance,
            p.data_date
        FROM
            google_rows_visao p
            LEFT JOIN google_visao_matches gvm
                ON p.chave_visao = gvm.chave_visao
                AND gvm.rn_visao = 1
    ),
    google_grupo_base AS (
        SELECT
            data,
            id_cliente,
            id_grupo_anuncio,
            produto,
            visao,
            nivel,
            campanha,
            anuncio,
            id_campanha,
            id_anuncio,
            ROW_NUMBER() OVER (
                PARTITION BY data,
                id_cliente,
                id_grupo_anuncio
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(nivel, '')) = 'grupo_anuncio' THEN 0
                        WHEN LOWER(COALESCE(nivel, '')) = 'anuncio' THEN 1
                        WHEN LOWER(COALESCE(nivel, '')) = 'campanha' THEN 2
                        ELSE 3
                    END,
                    COALESCE(id_campanha, ''),
                    COALESCE(id_anuncio, ''),
                    COALESCE(campanha, ''),
                    COALESCE(anuncio, '')
            ) AS rn_grupo
        FROM
            google_perf_classificada
        WHERE
            NULLIF(TRIM(id_grupo_anuncio), '') IS NOT NULL
    ),
    google_grupo_visao AS (
        SELECT
            data,
            id_cliente,
            id_grupo_anuncio,
            produto,
            visao
        FROM
            google_grupo_base
        WHERE
            rn_grupo = 1
    ),
    google_conversoes_evento AS (
        SELECT
            DATE(data) AS data,
            CAST(id_cliente AS STRING) AS id_cliente,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio,
            conversion_event AS evento,
            SUM(COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0)) AS conversoes_clique,
            SUM(COALESCE(SAFE_CAST(conversoes_by_conversion_date AS FLOAT64), 0)) AS conversoes_momento,
            SUM(COALESCE(SAFE_CAST(conversao_view AS FLOAT64), 0)) AS conversao_view,
            SUM(COALESCE(SAFE_CAST(todas_conversoes AS FLOAT64), 0)) AS todas_conversoes,
            SUM(COALESCE(SAFE_CAST(todas_conversions_by_conversion_date AS FLOAT64), 0)) AS todas_conversoes_momento
        FROM
            `${PROJECT_ID}.${DATASET_WORK}.google_conversions_all_partitioned_diario`
        WHERE
            LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            AND NULLIF(
                COALESCE(
                    CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                    NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
                ),
                ''
            ) IS NOT NULL
        GROUP BY
            data,
            id_cliente,
            id_grupo_anuncio,
            evento
    ),
    google_conversoes_evento_filtrada AS (
        SELECT
            gv.data,
            gv.id_cliente,
            gv.id_grupo_anuncio,
            gv.produto,
            gv.visao,
            gce.evento,
            gce.conversoes_clique,
            gce.conversoes_momento,
            gce.conversao_view,
            gce.todas_conversoes,
            gce.todas_conversoes_momento,
            COALESCE(
                gec.evento_principal,
                CASE
                    WHEN COALESCE(gev.qtd_eventos, 0) = 0 THEN gep.evento_principal
                    ELSE FALSE
                END,
                FALSE
            ) AS evento_principal
        FROM
            google_grupo_visao gv
            LEFT JOIN google_conversoes_evento gce ON gv.data = gce.data
            AND gv.id_cliente = gce.id_cliente
            AND gv.id_grupo_anuncio = gce.id_grupo_anuncio
            LEFT JOIN google_eventos_visao gev ON gv.produto = gev.produto
            AND gv.visao = gev.visao
            LEFT JOIN google_eventos_produto gep ON gv.produto = gep.produto
            AND gce.evento = gep.evento
            LEFT JOIN google_eventos_config gec ON gv.produto = gec.produto
            AND gv.visao = gec.visao
            AND gce.evento = gec.evento
        WHERE
            gce.evento IS NULL
            OR (
                gep.evento IS NOT NULL
                AND (
                    COALESCE(gev.qtd_eventos, 0) = 0
                    OR gec.evento IS NOT NULL
                )
            )
    ),
    google_conversoes_por_grupo AS (
        SELECT
            data,
            id_cliente,
            id_grupo_anuncio,
            produto,
            visao,
            SUM(
                CASE
                    WHEN evento_principal
                        THEN COALESCE(conversoes_momento, 0) + COALESCE(conversao_view, 0)
                    ELSE 0
                END
            ) AS conversoes_diretas_grupo,
            SUM(
                CASE
                    WHEN NOT evento_principal
                        THEN GREATEST(0, COALESCE(todas_conversoes_momento, 0) - COALESCE(conversao_view, 0))
                    ELSE 0
                END
            ) AS conversoes_cross_grupo,
            SUM(
                CASE
                    WHEN evento_principal
                        THEN COALESCE(conversoes_momento, 0) + COALESCE(conversao_view, 0)
                    ELSE 0
                END
            ) AS conversoes_grupo
        FROM
            google_conversoes_evento_filtrada
        GROUP BY
            data,
            id_cliente,
            id_grupo_anuncio,
            produto,
            visao
    ),
    google_detalhe_com_join AS (
        -- Join controlado: base detalhada (1..N por grupo) x conversao agregada (1 por grupo).
        SELECT
            p.data,
            p.nivel,
            p.id_cliente,
            p.produto,
            p.veiculo,
            p.visao,
            p.campanha,
            p.id_campanha,
            p.grupo_anuncio,
            p.id_grupo_anuncio,
            p.anuncio,
            p.id_anuncio,
            p.investimento,
            p.impressoes,
            p.cliques,
            p.ctr,
            p.parc_impr,
            p.alcance,
            p.data_date,
            COALESCE(c.conversoes_diretas_grupo, 0) AS conversoes_diretas_grupo,
            COALESCE(c.conversoes_cross_grupo, 0) AS conversoes_cross_grupo,
            COALESCE(c.conversoes_diretas_grupo, 0) AS conversoes_grupo,
            ROW_NUMBER() OVER (
                PARTITION BY p.data,
                p.id_cliente,
                p.id_grupo_anuncio
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(p.nivel, '')) = 'grupo_anuncio' THEN 0
                        WHEN LOWER(COALESCE(p.nivel, '')) = 'anuncio' THEN 1
                        WHEN LOWER(COALESCE(p.nivel, '')) = 'campanha' THEN 2
                        ELSE 3
                    END,
                    COALESCE(p.id_campanha, ''),
                    COALESCE(p.id_anuncio, ''),
                    COALESCE(p.campanha, ''),
                    COALESCE(p.anuncio, '')
            ) AS rn_ancora_grupo
        FROM
            google_perf_classificada p
            LEFT JOIN google_conversoes_por_grupo c ON p.data = c.data
            AND p.id_cliente = c.id_cliente
            AND p.id_grupo_anuncio = c.id_grupo_anuncio
    ),
    google_unificado AS (
        SELECT
            data,
            nivel,
            produto,
            veiculo,
            visao,
            campanha,
            id_campanha,
            grupo_anuncio,
            id_grupo_anuncio,
            anuncio,
            id_anuncio,
            CAST(investimento AS FLOAT64) AS investimento,
            CAST(impressoes AS FLOAT64) AS impressoes,
            CAST(cliques AS FLOAT64) AS cliques,
            CAST(ctr AS FLOAT64) AS ctr,
            CAST(parc_impr AS FLOAT64) AS parc_impr,
            CAST(alcance AS FLOAT64) AS alcance,
            CAST(
                CASE
                    WHEN NULLIF(TRIM(id_grupo_anuncio), '') IS NOT NULL
                    AND rn_ancora_grupo = 1 THEN conversoes_grupo
                    ELSE 0
                END AS FLOAT64
            ) AS conversoes,
            CAST(
                CASE
                    WHEN NULLIF(TRIM(id_grupo_anuncio), '') IS NOT NULL
                    AND rn_ancora_grupo = 1 THEN conversoes_diretas_grupo
                    ELSE 0
                END AS FLOAT64
            ) AS conversoes_diretas,
            CAST(
                CASE
                    WHEN NULLIF(TRIM(id_grupo_anuncio), '') IS NOT NULL
                    AND rn_ancora_grupo = 1 THEN conversoes_cross_grupo
                    ELSE 0
                END AS FLOAT64
            ) AS conversoes_cross,
            data_date
        FROM
            google_detalhe_com_join
    ),
    final_google AS (
    SELECT
        data,
        nivel,
        produto,
        veiculo,
        visao,
        campanha,
        id_campanha,
        grupo_anuncio,
        id_grupo_anuncio,
        anuncio,
        id_anuncio,
        SUM(COALESCE(investimento, 0)) AS investimento,
        SUM(COALESCE(impressoes, 0)) AS impressoes,
        SUM(COALESCE(cliques, 0)) AS cliques,
        SAFE_DIVIDE(
            SUM(COALESCE(cliques, 0)),
            NULLIF(SUM(COALESCE(impressoes, 0)), 0)
        ) AS CTR,
        SUM(COALESCE(parc_impr, 0)) AS parc_impr,
        AVG(alcance) AS alcance,
        SUM(COALESCE(conversoes, 0)) AS conversoes,
        SUM(COALESCE(conversoes, 0)) AS conversoes_diretas,
        SUM(COALESCE(conversoes_cross, 0)) AS conversoes_cross,

        data_date
    FROM
        google_unificado
    GROUP BY
        data,
        nivel,
        produto,
        veiculo,
        visao,
        campanha,
        id_campanha,
        grupo_anuncio,
        id_grupo_anuncio,
        anuncio,
        id_anuncio,
        data_date
    )

    SELECT * FROM final_google
    UNION ALL
    SELECT * FROM final_meta
    UNION ALL
    SELECT * FROM final_tiktok
    UNION ALL
    SELECT * FROM final_kwai
;





