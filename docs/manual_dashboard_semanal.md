# Consumir as tabelas no BI

O fluxo é APIs → Parquet no Cloud Storage → SOR/RAW → SOT/WORK → SPEC. As tabelas SPEC fornecem dados diários, semanais, mensais, por anúncio e por grupo de anúncios, além de eventos e fotografias de reprocessamento.

A configuração de palavras e prioridades determina a visão da campanha; a configuração de eventos determina a conversão considerada para a visão. Configure as regras com seus próprios dados e confirme a granularidade de cada saída antes de criar medidas no BI.

Não some linhas de campanha, grupo e anúncio como se fossem registros independentes. Não some alcance diário, por conta ou campanha para obter alcance único de um período maior. Não some médias de frequência nem agregue investimento após expandir eventos.

Para uma atualização completa, a coleta, a publicação das procedures e o CALL da orquestração são etapas diferentes. Um job concluído não confirma que a tabela final está atualizada. Compare a data máxima, cobertura de contas, totais e eventuais falhas no resumo do coletor.

O repositório fornece a camada de dados. Um arquivo Power BI, medidas DAX e regras de acesso ao relatório precisam ser implementados no seu ambiente.
