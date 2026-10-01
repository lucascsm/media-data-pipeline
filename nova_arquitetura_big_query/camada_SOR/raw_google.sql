CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_google`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/dados_google_ads_all_new.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_google_conversions`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/google_ads_conversions_all_new.parquet']
);
