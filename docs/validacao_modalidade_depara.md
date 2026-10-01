# Validar classificação e modalidade

O de/para usa produto, veículo e visão como dimensões. Para cada combinação deve haver uma única modalidade, mesmo quando a configuração contém várias linhas de palavras e eventos.

O lookup escalar com `SELECT DISTINCT` preserva a granularidade e falha diante de modalidades conflitantes. Não substitua esse lookup por um join sem validar a unicidade: o join pode repetir investimento, impressões e cliques.

Execute o arquivo renderizado de [validação](../nova_arquitetura_big_query/querys_validacao/valida_modalidade_depara.sql) no seu ambiente após configurar e publicar os SQLs. Conflitos e divergências devem ser zero. Confira também contagem de linhas e somas de métricas antes e depois de qualquer alteração.

Esta documentação descreve o modelo; não contém resultados, contagens ou validações de um ambiente empresarial.
