CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_kwai`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/dados_kwai_all_new.parquet']
);
