from src.legacy_config import legacy_rule
from src.legacy_config import load_legacy_settings
import pandas as pd
from pathlib import Path

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS RELATIVOS
# =============================================================================
from src.utils_do_pai import GCS_MOUNT
OUTPUT_DIR        = GCS_MOUNT / "output"
INPUT_FILE        = OUTPUT_DIR / "resumo_diario_powerbi.csv"
#BUDGET_FILE       = GCS_MOUNT / "ORCADO_ANO_2025.xlsx"
OUTPUT_FILE_TOTAL = OUTPUT_DIR / legacy_rule("LABEL_OR_MATCH_32")
#Pra local abaixo
#INPUT_FILE        = "/path/to/project"
#BUDGET_FILE       =  "/path/to/project"
#OUTPUT_FILE_TOTAL = "/path/to/project"
budget_data = load_legacy_settings().get("budget_data", {})

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {INPUT_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. LEITURA DOS DADOS
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    required_cols = {"data", "nivel", "Produto", "veiculo", "custo", "conversoes", "cliques"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        raise KeyError(
            f"Colunas obrigatórias ausentes em {INPUT_FILE}: {', '.join(sorted(missing_cols))}"
        )
    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    df = df[df['nivel'] == 'campanha']

    # 2. AGRUPAMENTO DIÁRIO POR PRODUTO E VEÍCULO
    df['produto_key'] = df['Produto'].str.upper().str.strip()
    df['veiculo_key'] = df['veiculo'].str.upper().str.strip()
    daily = (
        df.groupby([df['data'], df['produto_key'], df['veiculo_key']], as_index=False)
          .agg({'custo':'sum','conversoes':'sum','cliques':'sum'})
    )
    daily.rename(columns={
        'data':'Dia','produto_key':'Produto','veiculo_key':'Veículo',
        'custo':'Custo','conversoes':'Aquisição direta','cliques':'Cliques'
    }, inplace=True)
    daily['Dia'] = pd.to_datetime(daily['Dia'])
    daily['mes'] = daily['Dia'].dt.to_period('M').dt.to_timestamp()

    # 3. CRIAÇÃO DO DataFrame DE ORÇAMENTO A PARTIR DO DICIONÁRIO
    budget_df = pd.DataFrame([
        {
            'mes': pd.to_datetime(mes),
            'Produto': produto,
            'Veículo': veiculo,
            'Investimento Previsto mês': valor
        }
        for mes, produtos in budget_data.items()
        for produto, veiculos in produtos.items()
        for veiculo, valor in veiculos.items()
    ])

    # 4. JUNÇÃO COM ORÇAMENTO
    merged = pd.merge(
        daily,
        budget_df,
        on=['Produto','Veículo','mes'],
        how='left'
    )

    # 5. ATRIBUIÇÃO DO CPA PARÂMETRO
    cpa_params = load_legacy_settings().get("cpa_params", {})
    merged['CPA Parâmetro'] = merged['Produto'].map(cpa_params)

    # 6. CRIAÇÃO DA LINHA "Todos" POR PRODUTO
    todos = (
        merged.groupby(['Dia','Produto','mes'], as_index=False)
              .agg({
                  'Custo':'sum',
                  'Aquisição direta':'sum',
                  'Cliques':'sum',
                  'Investimento Previsto mês':'sum'
              })
    )
    todos['Veículo'] = 'Todos'
    todos['CPA Parâmetro'] = todos['Produto'].map(cpa_params)

    # 7. PREPARAÇÃO DO DF FINAL
    merged = merged.drop(columns=['mes'])
    todos = todos.drop(columns=['mes'])
    final_df = pd.concat([merged, todos], ignore_index=True)
    final_df = final_df[[
        'Dia','Produto','Veículo','Investimento Previsto mês',
        'Custo','Aquisição direta','Cliques','CPA Parâmetro'
    ]]
    final_df.sort_values(['Produto','Dia','Veículo'], inplace=True)

    # 8. EXPORTAÇÃO
    final_df.to_csv(OUTPUT_FILE_TOTAL, index=False)
    print(f"Novo modelo estratégico exportado para: {OUTPUT_FILE_TOTAL}")

if __name__ == "__main__":
    main()
