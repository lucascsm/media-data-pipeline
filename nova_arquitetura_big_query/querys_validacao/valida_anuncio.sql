with anuncio_novo as(
SELECT 
"anuncio_novo" AS processo,
veiculo,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(cliques) AS cliques,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_SPEC}.media_all_diario_anuncio`
WHERE data between '2026-03-01' and '2026-03-20'
group by 
veiculo,
produto
),

anuncio_antigo as (
SELECT 
"anuncio_antigo" AS processo,
veiculo,
produto,
SUM(custo) AS custo,
SUM(impressoes) AS impressoes,
SUM(cliques) AS cliques,
SUM(conversoes) AS conversoes
FROM `${PROJECT_ID}.${DATASET_REPORTS}.diario_anuncio` 
WHERE data between '2026-03-01' and '2026-03-20'
group by 
veiculo,
produto
)
select * from anuncio_novo 
union all 
select * from anuncio_antigo 
order by 
-- ad_id ='REPLACE_WITH_AD_ID'
veiculo,
produto