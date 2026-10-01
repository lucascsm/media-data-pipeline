DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY);
DECLARE end_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE v_insert_columns STRING;
DECLARE v_select_columns STRING;

ALTER TABLE `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
DROP COLUMN IF EXISTS frequencia;

ALTER TABLE `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
ADD COLUMN IF NOT EXISTS alcance FLOAT64;

DELETE FROM `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario`
WHERE data_date BETWEEN start_date AND end_date;

SET v_insert_columns = (
  SELECT STRING_AGG(FORMAT('`%s`', column_name), ', ' ORDER BY ordinal_position)
  FROM `${PROJECT_ID}.${DATASET_WORK}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'tiktok_all_partitioned_diario'
);

SET v_select_columns = (
  SELECT STRING_AGG(
    CASE
      WHEN dst.column_name = 'data_date' THEN "DATE(SAFE.PARSE_DATETIME('%F %H:%M:%S', src.data)) AS data_date"
      WHEN src_col.column_name IS NULL THEN FORMAT(
        'CAST(NULL AS %s) AS `%s`',
        dst.data_type,
        dst.column_name
      )
      WHEN dst.column_name = 'alcance' THEN "SAFE_CAST(src.`alcance` AS FLOAT64) AS alcance"
      ELSE FORMAT('src.`%s`', dst.column_name)
    END,
    ', '
    ORDER BY dst.ordinal_position
  )
  FROM `${PROJECT_ID}.${DATASET_WORK}.INFORMATION_SCHEMA.COLUMNS` AS dst
  LEFT JOIN `${PROJECT_ID}.${DATASET_RAW}.INFORMATION_SCHEMA.COLUMNS` AS src_col
    ON src_col.table_name = 'all_tiktok'
    AND src_col.column_name = dst.column_name
  WHERE dst.table_name = 'tiktok_all_partitioned_diario'
);

EXECUTE IMMEDIATE FORMAT(
  """
  INSERT INTO `${PROJECT_ID}.${DATASET_WORK}.tiktok_all_partitioned_diario` (%s)
  SELECT %s
  FROM `${PROJECT_ID}.${DATASET_RAW}.all_tiktok` AS src
  WHERE DATE(SAFE.PARSE_DATETIME('%%F %%H:%%M:%%S', src.data)) BETWEEN @start_date AND @end_date
  """,
  v_insert_columns,
  v_select_columns
)
USING start_date AS start_date, end_date AS end_date;
