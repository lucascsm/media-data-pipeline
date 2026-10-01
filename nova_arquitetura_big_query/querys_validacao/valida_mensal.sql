with mensal_novo as(
SELECT 
"mensal_novo" AS processo,
veiculo,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(cliques) AS cliques,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_resumo`
WHERE data between '2026-03-01' and '2026-03-31'
group by 
veiculo,
produto
),

mensal_antigo as (
SELECT 
"mensal_antigo" AS processo,
veiculo,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(cliques) AS cliques,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_REPORTS}.resumo_diario_pbi_new` 
WHERE data between '2026-03-01' and '2026-03-31'
group by 
veiculo,
produto
)
select * from mensal_novo 
union all 
select * from mensal_antigo 
order by 
veiculo,
produto
