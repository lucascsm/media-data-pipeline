CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_alcance_semanal`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/alcance_semanal_all_new.parquet']
);
