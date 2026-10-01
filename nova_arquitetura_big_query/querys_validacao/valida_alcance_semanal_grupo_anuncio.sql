-- Cobertura de alcance/frequencia na saida semanal por grupo.
SELECT
    data_date AS semana_ref,
    LOWER(veiculo) AS veiculo,
    COUNT(*) AS linhas,
    COUNTIF(alcance IS NOT NULL) AS linhas_com_alcance,
    COUNTIF(frequencia IS NOT NULL) AS linhas_com_frequencia,
    COUNTIF(
        alcance > 0
        AND ABS(
            frequencia - SAFE_DIVIDE(impressoes, alcance)
        ) > 0.000001
    ) AS divergencias_calculo_frequencia
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal_grupo_anuncio`
WHERE STARTS_WITH(nome_semana, '2-')
    AND data_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 28 DAY)
GROUP BY
    semana_ref,
    veiculo
ORDER BY
    semana_ref DESC,
    veiculo;

-- A chave da WORK deve ser unica para impedir multiplicacao no JOIN da SPEC.
SELECT
    semana_ref,
    LOWER(veiculo) AS veiculo,
    produto,
    id_cliente,
    id_grupo_anuncio,
    COUNT(*) AS quantidade
FROM `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_grupo_anuncio_partitioned`
WHERE semana_ref >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 28 DAY)
GROUP BY
    semana_ref,
    veiculo,
    produto,
    id_cliente,
    id_grupo_anuncio
HAVING COUNT(*) > 1
ORDER BY
    semana_ref DESC,
    veiculo,
    produto,
    id_cliente,
    id_grupo_anuncio;
