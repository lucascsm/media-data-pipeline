CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_tiktok`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/dados_tiktok_all_new.parquet']
);