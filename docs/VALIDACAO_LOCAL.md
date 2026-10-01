# Evidências de validação local

Preparação em 01/10/2026. A comparação usa a base local; não houve comparação com imagem publicada ou execução em produção.

- Os 116 arquivos de origem incluídos no inventário conservaram seus hashes.
- 138 funções compartilhadas do `src` conservaram exatamente o AST do corpo, desconsiderando docstrings e posições de linha.
- Oito funções existentes mudaram para carregar catálogo/aliases de configuração, selecionar a opção de conversões por conta, configurar o diagnóstico Meta, bloquear coleta sem contas e usar identificação genérica do cliente HTTP.
- Os 60 SQLs da arquitetura atual conservaram a estrutura de operadores, expressões numéricas, colunas e joins. A comparação desconsiderou comentários, valores de strings parametrizados e qualificadores de projeto/dataset; não é uma comparação de resultados em BigQuery.
- A geração local resolveu os parâmetros de todos os 72 SQLs atuais, antigos e de diagnóstico.
- Os 14 testes passaram, incluindo conflito no catálogo, conversão de eventos, limites de datas, substituição de métricas no reprocessamento dos quatro provedores, preservação de arquivos em extrações vazias de Meta/TikTok/Kwai e ausência de chamadas cloud no plano de deploy.
- A inicialização preservou arquivos locais já preenchidos e criou somente o modelo do provedor escolhido. A conferência local identificou campos pendentes sem exibir seus valores, ocultou conteúdo de YAML inválido e exigiu secrets apenas dos canais selecionados.
- A auditoria privada comparou os arquivos públicos com os identificadores e credenciais conhecidos na origem e não encontrou correspondências. Esses valores e a lista privada de comparação não integram o repositório.

As regras históricas de produtos, eventos customizados, URLs, orçamentos e metas foram substituídas por exemplos configuráveis; elas precisam ser preenchidas e validadas com suas próprias regras antes de uso.

Não foram executados login/chamadas aos provedores, build Docker, publicação SQL, deploy, Scheduler, CALL ou recuperação de dados. A verificação de sintaxe e renderização SQL não equivale a um dry-run do BigQuery.

Para repetir a validação:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m compileall -q src scripts fluxo_antigo
.\.venv\Scripts\python.exe scripts/render_sql.py
.\.venv\Scripts\python.exe scripts/check_public_files.py
.\deploy_automation.ps1 -PythonExecutable .\.venv\Scripts\python.exe
.\deploy_sql.ps1 -PythonExecutable .\.venv\Scripts\python.exe
```

As dependências principais dos testes vieram do ambiente Python existente; a base IANA `tzdata` foi instalada em um ambiente separado para a validação no Windows. Uma instalação limpa de todas as dependências ainda deve ser testada antes do primeiro deploy. As dependências originais não tinham lockfile; a compatibilidade futura depende das versões instaladas.
