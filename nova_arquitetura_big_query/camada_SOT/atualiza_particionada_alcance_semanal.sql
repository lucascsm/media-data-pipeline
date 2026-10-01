DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
DECLARE end_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE start_week DATE DEFAULT DATE_TRUNC(start_date, WEEK(MONDAY));
DECLARE end_week DATE DEFAULT DATE_TRUNC(end_date, WEEK(MONDAY));

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned` (
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
    alcance FLOAT64
)
PARTITION BY semana_ref;

DELETE FROM `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned`
WHERE semana_ref BETWEEN start_week AND end_week;

INSERT INTO `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned` (
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
        SAFE_CAST(alcance AS FLOAT64) AS alcance
    FROM `${PROJECT_ID}.${DATASET_RAW}.all_alcance_semanal`
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
    alcance
FROM origem
WHERE semana_ref BETWEEN start_week AND end_week
    AND semana_ref IS NOT NULL
    AND veiculo IS NOT NULL
    AND id_campanha IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY semana_ref, veiculo, produto, id_cliente, id_campanha
    ORDER BY data_fim_semana DESC, nome_campanha DESC
) = 1;
