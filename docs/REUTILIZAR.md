# Reutilizar o pipeline com seus parâmetros

O código recebe suas contas e credenciais, coleta as métricas dos provedores, grava Parquet e organiza as tabelas para BI. Para trocar a organização, as contas ou a classificação dos produtos, edite os arquivos locais de configuração. As configurações abaixo pertencem a quem clonar o projeto.

## 1. Instalar

Abra o terminal na pasta do repositório. Os comandos usam PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 2. Escolher os canais e criar suas configurações

Exemplo para começar somente com Google:

```powershell
.\.venv\Scripts\python.exe scripts/configure.py --init --channels google
```

Para Google e Meta, use `--channels google,meta`. Os canais disponíveis são google, meta, tiktok e kwai. O comando cria somente os modelos dos canais escolhidos, além do catálogo, da configuração cloud e de um de/para local. Se um arquivo já existir, seus valores serão preservados.

Os arquivos sem `.example` são seus arquivos locais e estão ignorados pelo Git. O script não solicita, imprime ou valida credenciais pela internet.

## 3. Preencher os parâmetros

| Arquivo | O que preencher | Quem usa |
|---|---|---|
| `config/accounts.yaml` | Plataforma, ID de conta e nome do seu produto | Todos os coletores |
| `config/google.yaml` | Developer token e credenciais OAuth | Google Ads |
| `config/meta.yaml` | App ID, app secret e access token | Meta |
| `config/tiktok.yaml` | Access token; os demais campos são modelos para outras operações | TikTok |
| `config/kwai.yaml` | Access token ou configuração completa de refresh | Kwai |
| `config/depara_visao_eventos.local.csv` | Suas palavras, prioridades, eventos, visões e modalidades | Classificação SQL |
| `config/cloud.local.json` | Seu projeto, bucket, datasets, jobs, agenda e referências a secrets | Deploy e geração de SQL |

No catálogo, o nome do produto é definido por você. Por exemplo, para uma empresa de cursos:

```yaml
accounts:
  - platform: google
    account_id: "REPLACE_WITH_YOUR_ACCOUNT_ID"
    product: "Cursos online"
    aggregate_conversions: false
product_aliases:
  cursos: "Cursos online"
```

Troque o ID pelo seu antes de validar. Você pode incluir várias contas e produtos. A combinação plataforma + conta deve ser única, e o nome do produto deve corresponder à coluna `produto` do seu de/para. Para Meta, utilize o prefixo `act_` esperado pelo SDK.

Os modelos não fazem referência a uma empresa ou produto real. O de/para contém palavras e eventos fictícios para demonstrar o schema; preencha com suas campanhas e com os eventos efetivamente retornados pelos provedores antes de publicar a camada SQL.

`login_customer_id` do Google é opcional: habilite-o no YAML se seu acesso passar por uma conta gerente. No Kwai, deixe os campos opcionais vazios quando usar apenas um access token; configure refresh_token, client_id e secret_key juntos se utilizar renovação.

## 4. Conferir antes de coletar

```powershell
.\.venv\Scripts\python.exe scripts/configure.py --check --channels google
```

O checker aponta arquivos ausentes, campos não preenchidos, placeholders, ausência de contas e duplicidade no catálogo. Ele informa o nome do campo, sem mostrar valores de credenciais. Uma validação aprovada confirma a configuração local; validade do token, acesso às contas e disponibilidade da API serão conhecidos na chamada real.

## 5. Executar com suas contas

```powershell
New-Item -ItemType Directory -Force data/output/backup | Out-Null
.\.venv\Scripts\python.exe -m src.main --channels google
```

Esse comando acessa a API e grava seus dados em `data/output`. Para escolher um período específico:

```powershell
.\.venv\Scripts\python.exe -m src.main_reprocess --channels google --start_date 2025-01-01 --end_date 2025-01-07
```

As datas são exemplos. Escolha seu período e consulte `--help` para tamanho de blocos, pausas e alcance semanal. Para alterar os caminhos locais, use CONFIG_DIR, ACCOUNT_CATALOG_PATH, GCS_MOUNT e OUTPUT_PATH no ambiente. `--config-dir` do checker deve apontar para o mesmo CONFIG_DIR usado pelo coletor.

## 6. Usar seu Google Cloud e seu BigQuery

Você pode coletar localmente sem usar Cloud Run. Para usar a nuvem, preencha `cloud.local.json`. Para um job só de Google, configure as referências de secrets `google.yaml` e `accounts.yaml`; referências de provedores não selecionados podem ficar com o exemplo.

```powershell
.\.venv\Scripts\python.exe scripts/configure.py --check --channels google --cloud
.\.venv\Scripts\python.exe scripts/render_sql.py --config config/cloud.local.json
.\deploy_automation.ps1 -ConfigPath config/cloud.local.json -Channels google -PythonExecutable .\.venv\Scripts\python.exe
```

O último comando apenas mostra o plano. Use `-Apply` para publicar no seu ambiente, depois de preparar bucket, secrets e permissões. Para o BigQuery, a fonte Parquet e o seu de/para precisam estar no bucket. Envie o CSV local de configuração para `output/depara visao-evento.csv`, conforme o contrato da carga.

Confira o [README](../README.md) para publicação SQL, execução do CALL e rotina completa. Configuração, publicação e atualização dos dados são etapas distintas. As [regras de granularidade](manual_dashboard_semanal.md) continuam valendo para quaisquer contas ou produtos que utilizar.

## Onde personalizar depois

Contas e produtos ficam no catálogo; classificação e eventos ficam no CSV; infraestrutura fica no JSON; períodos de reprocessamento são argumentos da CLI. As janelas automáticas da coleta diária e da promoção SQL ainda são constantes no runner/orquestração: o README indica onde alterá-las e como validar o impacto. APIs, granularidade, moedas ou modelos de métricas diferentes podem exigir adaptações nos coletores e transformações.
