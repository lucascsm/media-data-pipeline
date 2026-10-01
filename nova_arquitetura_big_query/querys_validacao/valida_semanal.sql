with semanal_novo as(
SELECT 
"semanal_novo" AS processo,
nome_semana,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(cliques) AS cliques,
SUM(parcela_impr) AS parcela_impressao,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
WHERE semana='30/03/2026'
group by nome_semana,
produto
),
semanal_antigo as (
  SELECT
"semanal_antigo" AS processo,
nome_semana,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(clique) AS cliques,
SUM(parcela_impressao) AS parcela_impressao,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_REPORTS}.resumo_semanal_pbi`
WHERE semana='30/03/2026'
group by nome_semana,produto
)
SELECT * FROM semanal_novo
UNION ALL
select * from semanal_antigo
order by nome_semana , produto