LOAD DATA OVERWRITE `${PROJECT_ID}.${DATASET_CONFIG}.depara_visao_eventos`
(
  produto STRING,
  veiculo STRING,
  visao STRING,
  valor STRING,
  valor_amigavel STRING,
  prioridade INT64,
  tipo_linha STRING,
  evento_principal STRING,
  modalidade STRING
)
FROM FILES (
  format = 'CSV',
  uris = ['gs://${DATA_BUCKET}/output/depara visao-evento.csv'],
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_jagged_rows = TRUE
);
