SELECT
    plataforma,
    produto,
    visao,
    sum(custo) as investimento,
    sum(impressoes) as impressoes,
    sum(conversoes) as conversoes,
  
FROM
    `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
WHERE
    semana='13/04/2026' and custo>0 and visao!='Outro' and plataforma='google' and produto!='Produto B'
GROUP BY 
    plataforma,
    produto,visao

order by     
plataforma,
    produto
    ,visao

    SELECT
    veiculo,
    produto,
    sum(custo) as investimento,
    sum(impressoes) as impressoes,
    sum(cliques) as cliques,
    sum(conversoes) as conversoes,
FROM
    `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
WHERE
    semana='${DIAGNOSTIC_WEEK_LABEL}' AND ((custo>0) OR (custo=0 AND conversoes>=20))
GROUP BY 
    produto,
    veiculo
    
order by 
produto,    
veiculo




        SELECT
        visao,
        sum(conversoes) as conversoes,
    FROM
        `${PROJECT_ID}.${DATASET_SPEC}.media_all_semanal`
    WHERE
        semana='${DIAGNOSTIC_WEEK_LABEL}' and veiculo like "%google%" and produto="${SPECIAL_PRODUCT}" AND ((custo>0) OR (custo=0 AND conversoes>=20))
    GROUP BY 
    1
    order by   
    1