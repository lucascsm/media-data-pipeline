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
