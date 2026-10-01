CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${DATASET_RAW}.all_meta`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${DATA_BUCKET}/output/meta_ads_data_all_new.parquet']
);