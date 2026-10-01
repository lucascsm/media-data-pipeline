CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_WORK}.google_all_partitioned_diario`
PARTITION BY data_date AS
SELECT
    *,
    DATE(SAFE.PARSE_DATETIME('%F', data)) AS data_date
    FROM `${PROJECT_ID}.${DATASET_RAW}.all_google`;