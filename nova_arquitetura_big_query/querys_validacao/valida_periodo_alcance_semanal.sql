-- Completed weeks must always contain a Monday-to-Sunday API interval.
-- Any returned row indicates that a partial collection overwrote weekly reach.
WITH semanas_concluidas AS (
    SELECT
        semana_ref,
        produto,
        LOWER(veiculo) AS veiculo,
        id_cliente,
        MIN(data_inicio_semana) AS menor_data_inicio,
        MAX(data_fim_semana) AS maior_data_fim,
        COUNT(*) AS linhas,
        COUNTIF(
            data_inicio_semana != semana_ref
            OR data_fim_semana != DATE_ADD(semana_ref, INTERVAL 6 DAY)
        ) AS linhas_periodo_incompleto,
        COUNT(DISTINCT id_campanha) AS campanhas
    FROM `${PROJECT_ID}.${DATASET_WORK}.alcance_semanal_partitioned`
    WHERE semana_ref < DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), WEEK(MONDAY))
    GROUP BY
        semana_ref,
        produto,
        veiculo,
        id_cliente
)
SELECT
    semana_ref,
    produto,
    veiculo,
    id_cliente,
    menor_data_inicio,
    maior_data_fim,
    linhas,
    linhas_periodo_incompleto,
    campanhas
FROM semanas_concluidas
WHERE linhas_periodo_incompleto > 0
ORDER BY
    semana_ref DESC,
    produto,
    veiculo,
    id_cliente;
