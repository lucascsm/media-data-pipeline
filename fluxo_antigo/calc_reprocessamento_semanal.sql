CREATE OR REPLACE TABLE ${PROJECT_ID}.${DATASET_REPORTS}.calc_var_reprocessamento AS
with sumariza_semanal as (
    select 
    semana,
    nome_semana,
    produto,
    plataforma,
    visao,
    sum(conversoes) as conversoes_reprocessamento,
    from ${PROJECT_ID}.${DATASET_REPORTS}.resumo_semanal_pbi
)

select 
a.semana,
a.nome_semana,
a.produto,
a.plataforma,
a.visao,
a.conversoes as conversoes_reprocessamento,
b.conversoes as conversoes_atuais
from ${PROJECT_ID}.${DATASET_REPORTS}.reprocessamento_semanal as a
inner join sumariza_semanal as b 
on(a.semana=PARSE_DATE('%d/%m/%Y', b.semana) and a.nome_semana=b.nome_semana and a.produto=b.produto and a.plataforma=b.plataforma and a.visao=b.visao)


