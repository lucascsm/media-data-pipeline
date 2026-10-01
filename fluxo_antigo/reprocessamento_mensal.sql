SELECT 
semana,
nome_semana,
produto,
plataforma,
visao,
sum(conversoes) as conversoes,
current_date() as dia_foto
FROM (
SELECT 
PARSE_DATE('%d/%m/%Y', semana) as semana,
nome_semana,
produto,
plataforma,
visao,
conversoes
from `${PROJECT_ID}.${DATASET_REPORTS}.resumo_semanal_pbi`)
where semana between DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) and current_date()
group by 1,2,3,4,5
order by 1 desc 