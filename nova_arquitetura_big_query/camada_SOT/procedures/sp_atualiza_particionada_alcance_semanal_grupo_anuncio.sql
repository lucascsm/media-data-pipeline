CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_WORK}.sp_atualiza_particionada_alcance_semanal_grupo_anuncio`(
    p_start_date DATE,
    p_end_date DATE
)
BEGIN
    DECLARE v_start_week DATE DEFAULT DATE_TRUNC(p_start_date, WEEK(MONDAY));
    DECLARE v_end_week DATE DEFAULT DATE_TRUNC(p_end_date, WEEK(MONDAY));

    ASSERT p_start_date IS NOT NULL AS 'p_start_date nao pode ser NULL';
    ASSERT p_end_date IS NOT NULL AS 'p_end_date nao pode ser NULL';
    ASSERT p_start_date <= p_end_date AS 'p_start_date deve ser menor ou igual a p_end_date';

    CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_grupo_anuncio_partitioned` (
        semana_ref DATE,
        semana STRING,
        nome_semana STRING,
        data_inicio_semana DATE,
        data_fim_semana DATE,
        veiculo STRING,
        produto STRING,
        id_cliente STRING,
        id_campanha STRING,
        nome_campanha STRING,
        id_grupo_anuncio STRING,
        nome_grupo_anuncio STRING,
        alcance FLOAT64
    )
    PARTITION BY semana_ref;

    DELETE FROM `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_grupo_anuncio_partitioned`
    WHERE semana_ref BETWEEN v_start_week AND v_end_week;

    INSERT INTO `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_grupo_anuncio_partitioned` (
        semana_ref,
        semana,
        nome_semana,
        data_inicio_semana,
        data_fim_semana,
        veiculo,
        produto,
        id_cliente,
        id_campanha,
        nome_campanha,
        id_grupo_anuncio,
        nome_grupo_anuncio,
        alcance
    )
    WITH origem AS (
        SELECT
            SAFE.PARSE_DATE('%F', CAST(semana_ref AS STRING)) AS semana_ref,
            CAST(semana AS STRING) AS semana,
            CAST(nome_semana AS STRING) AS nome_semana,
            SAFE.PARSE_DATE('%F', CAST(data_inicio_semana AS STRING)) AS data_inicio_semana,
            SAFE.PARSE_DATE('%F', CAST(data_fim_semana AS STRING)) AS data_fim_semana,
            LOWER(CAST(veiculo AS STRING)) AS veiculo,
            CAST(produto AS STRING) AS produto,
            CAST(id_cliente AS STRING) AS id_cliente,
            COALESCE(
                CAST(SAFE_CAST(id_campanha AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_campanha AS STRING)), '')
            ) AS id_campanha,
            CAST(nome_campanha AS STRING) AS nome_campanha,
            COALESCE(
                CAST(SAFE_CAST(id_grupo_anuncio AS INT64) AS STRING),
                NULLIF(TRIM(CAST(id_grupo_anuncio AS STRING)), '')
            ) AS id_grupo_anuncio,
            CAST(nome_grupo_anuncio AS STRING) AS nome_grupo_anuncio,
            SAFE_CAST(alcance AS FLOAT64) AS alcance
        FROM `${PROJECT_ID}.${DATASET_RAW}.all_alcance_semanal_grupo_anuncio`
    )
    SELECT
        semana_ref,
        semana,
        nome_semana,
        data_inicio_semana,
        data_fim_semana,
        veiculo,
        produto,
        id_cliente,
        id_campanha,
        nome_campanha,
        id_grupo_anuncio,
        nome_grupo_anuncio,
        alcance
    FROM origem
    WHERE semana_ref BETWEEN v_start_week AND v_end_week
        AND semana_ref IS NOT NULL
        AND veiculo IN ('meta', 'tiktok')
        AND id_grupo_anuncio IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY semana_ref, veiculo, produto, id_cliente, id_grupo_anuncio
        ORDER BY data_fim_semana DESC, nome_grupo_anuncio DESC
    ) = 1;
END;
