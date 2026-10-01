DECLARE p_start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
DECLARE p_end_date DATE DEFAULT CURRENT_DATE();

-- 1) Volume diario, incluindo a quantidade de linhas preenchidas com zero.
SELECT
    data_date,
    veiculo,
    COUNT(*) AS qtd_linhas,
    COUNTIF(conversoes_diretas = 0) AS qtd_linhas_zero,
    COUNTIF(conversoes_diretas IS NULL) AS qtd_linhas_nulas,
    COUNT(DISTINCT evento) AS qtd_eventos,
    SUM(conversoes_diretas) AS conversoes_diretas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
WHERE data_date BETWEEN p_start_date AND p_end_date
GROUP BY
    data_date,
    veiculo
ORDER BY
    data_date DESC,
    veiculo;

-- 2) Deve retornar zero linhas: a granularidade final precisa ser unica.
SELECT
    data,
    nome_campanha,
    visao,
    canal,
    veiculo,
    evento,
    data_date,
    COUNT(*) AS qtd_linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
WHERE data_date BETWEEN p_start_date AND p_end_date
GROUP BY
    data,
    nome_campanha,
    visao,
    canal,
    veiculo,
    evento,
    data_date
HAVING COUNT(*) > 1
ORDER BY qtd_linhas DESC;

-- 3) Deve retornar zero linhas. Para Google, replica o filtro do semanal:
-- custo positivo na campanha/semana. Os demais veiculos continuam exigindo
-- custo positivo na campanha/dia.
WITH linhas_candidatas AS (
    SELECT
        DATE(data) AS data,
        data_date,
        produto AS canal,
        LOWER(CAST(veiculo AS STRING)) AS veiculo,
        visao,
        campanha AS nome_campanha,
        COALESCE(SAFE_CAST(investimento AS FLOAT64), 0) AS investimento_linha
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN DATE_TRUNC(p_start_date, WEEK(MONDAY))
        AND DATE_ADD(DATE_TRUNC(p_end_date, WEEK(MONDAY)), INTERVAL 6 DAY)
        AND (
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
linhas_com_custo_semanal AS (
    SELECT
        *,
        SUM(investimento_linha) OVER (
            PARTITION BY
                DATE_TRUNC(data_date, WEEK(MONDAY)),
                canal,
                veiculo,
                visao,
                nome_campanha
        ) AS investimento_campanha_semana
    FROM linhas_candidatas
),
campanhas_elegiveis AS (
    SELECT DISTINCT
        data,
        data_date,
        canal,
        veiculo,
        visao,
        nome_campanha
    FROM linhas_com_custo_semanal
    WHERE
        (veiculo = 'google' AND investimento_campanha_semana > 0)
        OR (veiculo IN ('meta', 'tiktok', 'kwai') AND investimento_linha > 0)
)
SELECT
    e.data,
    e.data_date,
    e.canal,
    e.veiculo,
    e.visao,
    e.nome_campanha,
    COUNT(*) AS qtd_linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` e
LEFT JOIN campanhas_elegiveis c
    ON c.data = e.data
    AND c.data_date = e.data_date
    AND c.canal = e.canal
    AND c.veiculo = e.veiculo
    AND c.visao IS NOT DISTINCT FROM e.visao
    AND c.nome_campanha IS NOT DISTINCT FROM e.nome_campanha
WHERE e.data_date BETWEEN p_start_date AND p_end_date
    AND c.data_date IS NULL
GROUP BY
    e.data,
    e.data_date,
    e.canal,
    e.veiculo,
    e.visao,
    e.nome_campanha
ORDER BY
    e.data_date DESC,
    e.veiculo,
    e.canal,
    e.visao,
    e.nome_campanha;

-- 4) Qualidade do de/para. Ausencia de nome amigavel deve retornar zero linhas.
-- Mais de um nome amigavel para a mesma chave tecnica precisa ser revisado.
WITH config_normalizada AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS canal,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
        CASE
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_')) IN (
                    'app_custom_event_fb_mobile_add_to_wishlist',
                    'add_to_wishlist',
                    'omni_add_to_wishlist'
                ) THEN 'alias_add_to_wishlist'
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_')) IN (
                    'start_trial_mobile_app',
                    'start_trial_total'
                ) THEN 'alias_start_trial'
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) IN ('meta', 'tiktok', 'kwai')
                THEN LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_'))
            ELSE NULLIF(TRIM(CAST(valor AS STRING)), '')
        END AS evento_chave,
        NULLIF(TRIM(CAST(valor_amigavel AS STRING)), '') AS evento_amigavel
    FROM `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
    WHERE LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) = 'evento'
        AND COALESCE(
            LOWER(NULLIF(TRIM(CAST(evento_principal AS STRING)), '')) IN (
                'true',
                '1',
                'sim',
                's',
                'yes',
                'y'
            ),
            FALSE
        )
)
SELECT
    canal,
    veiculo,
    visao,
    evento_chave,
    COUNTIF(evento_amigavel IS NULL) AS qtd_sem_nome_amigavel,
    COUNT(DISTINCT evento_amigavel) AS qtd_nomes_amigaveis,
    STRING_AGG(DISTINCT evento_amigavel, ' | ' ORDER BY evento_amigavel) AS nomes_amigaveis
FROM config_normalizada
GROUP BY
    canal,
    veiculo,
    visao,
    evento_chave
HAVING COUNTIF(evento_amigavel IS NULL) > 0
    OR COUNT(DISTINCT evento_amigavel) > 1
ORDER BY
    canal,
    veiculo,
    visao,
    evento_chave;

-- 5) Deve retornar zero linhas: o campo evento publicado precisa usar o nome
-- amigavel principal. As duas excecoes preservam os fallbacks tecnicos atuais.
WITH config_raw AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS canal,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
        NULLIF(TRIM(CAST(valor AS STRING)), '') AS evento_original,
        NULLIF(TRIM(CAST(valor_amigavel AS STRING)), '') AS evento_amigavel,
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
    WHERE LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) = 'evento'
),
config_escopo AS (
    SELECT
        canal,
        veiculo,
        visao,
        COUNT(*) AS qtd_eventos_config,
        COUNTIF(evento_principal) AS qtd_eventos_principais
    FROM config_raw
    GROUP BY
        canal,
        veiculo,
        visao
),
config_amigavel_principal AS (
    SELECT DISTINCT
        canal,
        veiculo,
        visao,
        evento_amigavel AS evento
    FROM config_raw
    WHERE evento_principal
        AND evento_amigavel IS NOT NULL
),
google_amigavel_produto AS (
    SELECT DISTINCT
        canal,
        evento_amigavel AS evento
    FROM config_raw
    WHERE veiculo = 'google'
        AND evento_principal
        AND evento_amigavel IS NOT NULL
)
SELECT
    e.canal,
    e.veiculo,
    e.visao,
    e.evento,
    COUNT(*) AS qtd_linhas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` e
LEFT JOIN config_amigavel_principal cfg
    ON cfg.canal = e.canal
    AND cfg.veiculo = e.veiculo
    AND cfg.visao = e.visao
    AND cfg.evento = e.evento
LEFT JOIN config_escopo escopo
    ON escopo.canal = e.canal
    AND escopo.veiculo = e.veiculo
    AND escopo.visao = e.visao
LEFT JOIN google_amigavel_produto cfg_google
    ON cfg_google.canal = e.canal
    AND cfg_google.evento = e.evento
WHERE e.data_date BETWEEN p_start_date AND p_end_date
    AND cfg.evento IS NULL
    AND NOT (
        e.veiculo = 'google'
        AND COALESCE(escopo.qtd_eventos_config, 0) = 0
        AND cfg_google.evento IS NOT NULL
    )
    AND NOT (
        e.veiculo = 'tiktok'
        AND COALESCE(escopo.qtd_eventos_principais, 0) = 0
        AND e.evento = 'result'
    )
GROUP BY
    e.canal,
    e.veiculo,
    e.visao,
    e.evento
ORDER BY qtd_linhas DESC;

-- 6) Deve retornar zero linhas: toda campanha ativa no dia precisa ter uma
-- linha para cada evento principal aplicavel, inclusive quando o valor for zero.
WITH config_normalizada AS (
    SELECT
        NULLIF(TRIM(CAST(produto AS STRING)), '') AS canal,
        LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) AS veiculo,
        NULLIF(TRIM(CAST(visao AS STRING)), '') AS visao,
        CASE
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_')) IN (
                    'app_custom_event_fb_mobile_add_to_wishlist',
                    'add_to_wishlist',
                    'omni_add_to_wishlist'
                ) THEN 'alias_add_to_wishlist'
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) = 'meta'
                AND LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_')) IN (
                    'start_trial_mobile_app',
                    'start_trial_total'
                ) THEN 'alias_start_trial'
            WHEN LOWER(NULLIF(TRIM(CAST(veiculo AS STRING)), '')) IN ('meta', 'tiktok', 'kwai')
                THEN LOWER(REPLACE(REPLACE(TRIM(CAST(valor AS STRING)), '.', '_'), '-', '_'))
            ELSE NULLIF(TRIM(CAST(valor AS STRING)), '')
        END AS evento_chave,
        NULLIF(TRIM(CAST(valor AS STRING)), '') AS evento_original,
        NULLIF(TRIM(CAST(valor_amigavel AS STRING)), '') AS evento_amigavel,
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
    WHERE LOWER(NULLIF(TRIM(CAST(tipo_linha AS STRING)), '')) = 'evento'
),
config_principal AS (
    SELECT
        canal,
        veiculo,
        visao,
        evento_chave,
        COALESCE(
            ARRAY_AGG(
                DISTINCT evento_amigavel IGNORE NULLS
                ORDER BY evento_amigavel
                LIMIT 1
            )[SAFE_OFFSET(0)],
            ARRAY_AGG(
                DISTINCT evento_original IGNORE NULLS
                ORDER BY evento_original
                LIMIT 1
            )[SAFE_OFFSET(0)]
        ) AS evento
    FROM config_normalizada
    GROUP BY
        canal,
        veiculo,
        visao,
        evento_chave
    HAVING LOGICAL_OR(evento_principal)
),
dimensoes_campanha_base AS (
    SELECT
        DATE(data) AS data,
        data_date,
        produto AS canal,
        LOWER(CAST(veiculo AS STRING)) AS veiculo,
        visao,
        campanha AS nome_campanha,
        COALESCE(SAFE_CAST(investimento AS FLOAT64), 0) AS investimento_linha
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN DATE_TRUNC(p_start_date, WEEK(MONDAY))
        AND DATE_ADD(DATE_TRUNC(p_end_date, WEEK(MONDAY)), INTERVAL 6 DAY)
        AND (
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
                canal,
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
        canal,
        veiculo,
        visao,
        nome_campanha
    FROM dimensoes_campanha_com_custo_semanal
    WHERE data_date BETWEEN p_start_date AND p_end_date
        AND (
            (veiculo = 'google' AND investimento_campanha_semana > 0)
            OR (veiculo IN ('meta', 'tiktok', 'kwai') AND investimento_linha > 0)
        )
),
grade_esperada AS (
    SELECT DISTINCT
        d.data,
        d.data_date,
        d.canal,
        d.veiculo,
        d.visao,
        d.nome_campanha,
        cfg.evento
    FROM dimensoes_campanha d
    INNER JOIN config_principal cfg
        ON cfg.canal = d.canal
        AND cfg.veiculo = d.veiculo
        AND cfg.visao = d.visao
)
SELECT
    g.data,
    g.data_date,
    g.canal,
    g.veiculo,
    g.visao,
    g.nome_campanha,
    g.evento
FROM grade_esperada g
LEFT JOIN `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` e
    ON e.data = g.data
    AND e.data_date = g.data_date
    AND e.canal = g.canal
    AND e.veiculo = g.veiculo
    AND e.visao = g.visao
    AND e.nome_campanha = g.nome_campanha
    AND e.evento = g.evento
WHERE e.data_date IS NULL
ORDER BY
    g.data_date DESC,
    g.veiculo,
    g.canal,
    g.visao,
    g.nome_campanha,
    g.evento
LIMIT 100;

-- 7) Evidencia das linhas preenchidas com zero. A consulta mostra zeros para
-- eventos que possuem valor positivo em outro dia da mesma janela.
SELECT
    e.data_date,
    e.canal,
    e.veiculo,
    e.visao,
    e.nome_campanha,
    e.evento,
    e.conversoes_diretas
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` e
WHERE e.data_date BETWEEN p_start_date AND p_end_date
    AND e.conversoes_diretas = 0
    AND EXISTS (
        SELECT 1
        FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos` positivo
        WHERE positivo.data_date BETWEEN p_start_date AND p_end_date
            AND positivo.canal = e.canal
            AND positivo.veiculo = e.veiculo
            AND positivo.visao = e.visao
            AND positivo.nome_campanha = e.nome_campanha
            AND positivo.evento = e.evento
            AND positivo.conversoes_diretas > 0
    )
ORDER BY
    e.data_date DESC,
    e.veiculo,
    e.canal,
    e.visao,
    e.nome_campanha,
    e.evento
LIMIT 100;

-- 8) Compara a soma atribuida aos eventos com a conversao direta do detalhe,
-- usando custo por campanha/semana no Google e custo por campanha/dia nos
-- demais veiculos. Deve retornar zero linhas.
WITH eventos AS (
    SELECT
        canal,
        veiculo,
        visao,
        SUM(conversoes_diretas) AS conversoes_eventos
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe_eventos`
    WHERE data_date BETWEEN p_start_date AND p_end_date
    GROUP BY
        canal,
        veiculo,
        visao
),
detalhe_base AS (
    SELECT
        data_date,
        produto AS canal,
        LOWER(veiculo) AS veiculo,
        visao,
        campanha AS nome_campanha,
        COALESCE(SAFE_CAST(investimento AS FLOAT64), 0) AS investimento_linha,
        COALESCE(conversoes_diretas, conversoes, 0) AS conversoes_detalhe
    FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_detalhe`
    WHERE data_date BETWEEN DATE_TRUNC(p_start_date, WEEK(MONDAY))
        AND DATE_ADD(DATE_TRUNC(p_end_date, WEEK(MONDAY)), INTERVAL 6 DAY)
        AND (
            (
                LOWER(COALESCE(veiculo, '')) = 'google'
                AND LOWER(COALESCE(nivel, '')) = 'grupo_anuncio'
            )
            OR (
                LOWER(COALESCE(veiculo, '')) IN ('meta', 'tiktok', 'kwai')
                AND LOWER(COALESCE(nivel, '')) = 'campanha'
            )
        )
),
detalhe_com_custo_semanal AS (
    SELECT
        *,
        SUM(investimento_linha) OVER (
            PARTITION BY
                DATE_TRUNC(data_date, WEEK(MONDAY)),
                canal,
                veiculo,
                visao,
                nome_campanha
        ) AS investimento_campanha_semana
    FROM detalhe_base
),
detalhe AS (
    SELECT
        canal,
        veiculo,
        visao,
        SUM(conversoes_detalhe) AS conversoes_detalhe
    FROM detalhe_com_custo_semanal
    WHERE data_date BETWEEN p_start_date AND p_end_date
        AND (
            (veiculo = 'google' AND investimento_campanha_semana > 0)
            OR (veiculo IN ('meta', 'tiktok', 'kwai') AND investimento_linha > 0)
        )
    GROUP BY
        canal,
        veiculo,
        visao
)
SELECT
    COALESCE(d.canal, e.canal) AS canal,
    COALESCE(d.veiculo, e.veiculo) AS veiculo,
    COALESCE(d.visao, e.visao) AS visao,
    COALESCE(d.conversoes_detalhe, 0) AS conversoes_detalhe,
    COALESCE(e.conversoes_eventos, 0) AS conversoes_eventos,
    COALESCE(e.conversoes_eventos, 0) - COALESCE(d.conversoes_detalhe, 0) AS diferenca
FROM detalhe d
FULL OUTER JOIN eventos e
    ON e.canal = d.canal
    AND e.veiculo = d.veiculo
    AND e.visao = d.visao
WHERE ABS(COALESCE(e.conversoes_eventos, 0) - COALESCE(d.conversoes_detalhe, 0)) > 0.01
ORDER BY
    ABS(diferenca) DESC;
