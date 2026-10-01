ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.reprocessamento_semanal`
ADD COLUMN IF NOT EXISTS conversoes_diretas FLOAT64;

ALTER TABLE `${PROJECT_ID}.${DATASET_SPEC}.reprocessamento_semanal`
ADD COLUMN IF NOT EXISTS conversoes_cross FLOAT64;

insert into `${PROJECT_ID}.${DATASET_SPEC}.reprocessamento_semanal` (
semana,
nome_semana,
produto,
plataforma,
nome_campanha,
conversoes,
conversoes_diretas,
conversoes_cross,
dia_foto
)
SELECT 
semana,
nome_semana,
produto,
plataforma,
nome_campanha,
sum(conversoes) as conversoes,
sum(conversoes_diretas) as conversoes_diretas,
sum(conversoes_cross) as conversoes_cross,
current_date() as dia_foto
FROM (
SELECT 
PARSE_DATE('%d/%m/%Y', semana) as semana,
custo,
nome_semana,
produto,
veiculo as plataforma,
nome_campanha,
conversoes,
COALESCE(SAFE_CAST(conversoes AS FLOAT64), 0) AS conversoes_diretas,
COALESCE(SAFE_CAST(JSON_VALUE(TO_JSON_STRING(s), '$.conversoes_cross') AS FLOAT64), 0) AS conversoes_cross
from `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal` s)
where semana=DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY) and custo > 0
group by 1,2,3,4,5
order by 1 desc
