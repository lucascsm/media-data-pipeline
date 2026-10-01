DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY);
DECLARE end_date   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

DELETE FROM `${PROJECT_ID}.${DATASET_WORK}.google_all_conversions_partitioned_diario`
WHERE data_date BETWEEN start_date AND end_date;

INSERT INTO `${PROJECT_ID}.${DATASET_WORK}.google_all_conversions_partitioned_diario`
SELECT
  *,
  DATE(SAFE.PARSE_DATETIME('%F', data)) AS data_date
FROM `${PROJECT_ID}.${DATASET_RAW}.all_google_conversions`
WHERE DATE(SAFE.PARSE_DATETIME('%F', data)) BETWEEN start_date AND end_date;
