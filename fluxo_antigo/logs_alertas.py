from src.legacy_config import legacy_rule
import os
import pandas as pd
import json
import datetime
import re
import sys
from pathlib import Path
import google.generativeai as genai

# Configuração dos caminhos (definidos internamente)
from src.utils_do_pai import GCS_MOUNT
# GCS_MOUNT = Path("/path/to/project")
OUTPUT_DIR = GCS_MOUNT / "output"
PATH_LOGS = OUTPUT_DIR / "logs.csv"
PATH_ESTRATEGICO_DIARIO = OUTPUT_DIR / legacy_rule("LABEL_OR_MATCH_32")
OUTPUT_CSV = OUTPUT_DIR / 'recomendacoes.csv'

# Configuração da API do Google
GOOGLE_API_KEY = os.environ["GEMINI_API_KEY"]
genai.configure(api_key=GOOGLE_API_KEY)

# Função para formatar números
def formatar_numero(valor, eh_porcentagem=False, casas_decimais=1):
    """
    Formata números para melhor legibilidade:
    - Valores inteiros para contagens (cliques, conversões)
    - Valores com 1 casa decimal para porcentagens
    """
    if valor is None:
        return None
    
    # Para valores inteiros (contagens)
    if not eh_porcentagem:
        return int(round(valor))
    
    # Para porcentagens
    return round(valor, casas_decimais)

# Função para calcular diferenças percentuais semanais
def calcular_diferenca_percentual_semanal(df):
    """
    Calcula os KPIs da semana atual, comparando com a semana anterior e com a média das 3 semanas anteriores.
    
    Args:
        df: DataFrame com os dados do dashboard
        
    Returns:
        Dicionário com os resultados agrupados por produto e veículo
    """
    # Converte a coluna de data para datetime (caso não esteja)
    df['Dia'] = pd.to_datetime(df['Dia'])
    # Filtra possíveis valores faltantes e remove linhas desnecessárias (se houver)
    df = df.dropna(subset=['Dia', 'Produto', 'Veículo'])
    # Remove quaisquer espaços em branco nos nomes de veículos/produtos
    df['Produto'] = df['Produto'].astype(str).str.strip()
    df['Veículo'] = df['Veículo'].astype(str).str.strip()

    # Identifica a última semana completa (segunda a domingo) presente nos dados
    # Encontrar a última data de domingo disponível no dataset
    df['weekday'] = df['Dia'].dt.weekday  # 0 = segunda, ..., 6 = domingo
    if (df['weekday'] == 6).any():
        last_sunday = df[df['weekday'] == 6]['Dia'].max()
    else:
        # Se não houver domingo nos dados, assume a última data como fim de semana atual
        last_sunday = df['Dia'].max()
    # Calcula a segunda-feira da semana atual (última semana completa)
    current_week_monday = last_sunday - pd.Timedelta(days=6)
    current_week_sunday = last_sunday
    # Semana anterior (mesmo intervalo) - 7 dias antes
    prev_week_monday = current_week_monday - pd.Timedelta(days=7)
    prev_week_sunday = current_week_sunday - pd.Timedelta(days=7)
    # Três semanas anteriores (excluindo a semana anterior já considerada)
    older3_weeks_start = prev_week_monday - pd.Timedelta(days=21)  # segunda-feira 3 semanas antes da anterior
    older3_weeks_end = prev_week_monday - pd.Timedelta(days=1)     # domingo imediatamente antes da semana anterior

    # Função auxiliar para calcular soma dos KPIs por grupo em um DataFrame filtrado (de uma semana ou conjunto de semanas)
    def aggregate_kpis(df_group, group_cols):
        """
        Agrupa o DataFrame dado pelos campos em group_cols e soma as métricas principais.
        Retorna um DataFrame com colunas de métricas agregadas.
        """
        if df_group.empty:
            # Retorna DataFrame vazio com colunas esperadas se não houver dados
            return pd.DataFrame(columns=group_cols + ['Custo', 'Cliques', 'Aquisição direta'])
        agg_df = df_group.groupby(group_cols).agg({
            'Custo': 'sum',
            'Cliques': 'sum',
            'Aquisição direta': 'sum'
        }).reset_index()
        return agg_df

    # Prepara dataframes filtrados para cada intervalo
    # Filtrar dados para cada período
    current_df = df[(df['Dia'] >= current_week_monday) & (df['Dia'] <= current_week_sunday)]
    prev_df = df[(df['Dia'] >= prev_week_monday) & (df['Dia'] <= prev_week_sunday)]
    older3_df = df[(df['Dia'] >= older3_weeks_start) & (df['Dia'] <= older3_weeks_end)]

    # Agrega dados por produto e veículo para cada período
    current_by_product_vehicle = aggregate_kpis(current_df, ['Produto', 'Veículo'])
    prev_by_product_vehicle = aggregate_kpis(prev_df, ['Produto', 'Veículo'])
    older3_by_product_vehicle = aggregate_kpis(older3_df, ['Produto', 'Veículo'])

    # Converte os DataFrames de referência em dicionários para acesso rápido
    prev_dict = {}
    for _, row in prev_by_product_vehicle.iterrows():
        produto = row['Produto']
        veiculo = row['Veículo']
        if produto not in prev_dict:
            prev_dict[produto] = {}
        prev_dict[produto][veiculo] = row

    older3_dict = {}
    # Calcula média das 3 semanas anteriores
    if not older3_by_product_vehicle.empty:
        older3_sum = older3_by_product_vehicle.groupby(['Produto', 'Veículo']).agg({
            'Custo': 'sum',
            'Cliques': 'sum',
            'Aquisição direta': 'sum'
        }).reset_index()
        
        # Dividir as somas totais por 3 para obter média semanal
        for _, row in older3_sum.iterrows():
            produto = row['Produto']
            veiculo = row['Veículo']
            if produto not in older3_dict:
                older3_dict[produto] = {}
            older3_dict[produto][veiculo] = {
                'Produto': produto,
                'Veículo': veiculo,
                'Custo': row['Custo'] / 3.0,
                'Cliques': row['Cliques'] / 3.0,
                'Aquisição direta': row['Aquisição direta'] / 3.0
            }

    # Função auxiliar para calcular variação percentual entre valor atual e valor de referência
    def calc_percent_variation(current_val, reference_val):
        """
        Retorna a variação percentual de current_val em relação a reference_val.
        Se reference_val = 0, retorna None para indicar não aplicável (evitar divisão por zero).
        """
        if reference_val is None or pd.isna(reference_val):
            return None
        if reference_val == 0:
            if current_val == 0:
                return 0.0
            else:
                return None  # variação infinita ou não definida
        return ((current_val - reference_val) / reference_val) * 100.0

    # Monta o resultado em dicionário para saída JSON
    result = {}
    
    # Produtos únicos
    produtos = current_by_product_vehicle['Produto'].unique()
    
    # Para cada produto
    for produto in produtos:
        if produto not in result:
            result[produto] = {}
        
        # Filtrar dados do produto atual
        produto_atual = current_by_product_vehicle[current_by_product_vehicle['Produto'] == produto]
        
        # Para cada veículo do produto
        for _, row in produto_atual.iterrows():
            veiculo = row['Veículo']
            current_clicks = row['Cliques']
            current_conv = row['Aquisição direta']
            
            # Calcula taxa de aquisição atual
            current_conversion_rate = (current_conv / current_clicks * 100.0) if current_clicks != 0 else 0.0
            
            # Busca valores da semana anterior
            prev_clicks = None
            prev_conv = None
            prev_rate = None
            if produto in prev_dict and veiculo in prev_dict[produto]:
                prev_clicks = prev_dict[produto][veiculo]['Cliques']
                prev_conv = prev_dict[produto][veiculo]['Aquisição direta']
                if prev_clicks != 0:
                    prev_rate = (prev_conv / prev_clicks * 100.0)
            
            # Busca valores da média das 3 semanas anteriores
            avg3_clicks = None
            avg3_conv = None
            avg3_rate = None
            if produto in older3_dict and veiculo in older3_dict[produto]:
                avg3_clicks = older3_dict[produto][veiculo]['Cliques']
                avg3_conv = older3_dict[produto][veiculo]['Aquisição direta']
                if avg3_clicks != 0:
                    avg3_rate = (avg3_conv / avg3_clicks * 100.0)
            
            # Calcula variações percentuais
            clicks_var_prev = calc_percent_variation(current_clicks, prev_clicks)
            clicks_var_avg3 = calc_percent_variation(current_clicks, avg3_clicks)
            conv_var_prev = calc_percent_variation(current_conv, prev_conv)
            conv_var_avg3 = calc_percent_variation(current_conv, avg3_conv)
            rate_var_prev = calc_percent_variation(current_conversion_rate, prev_rate)
            rate_var_avg3 = calc_percent_variation(current_conversion_rate, avg3_rate)
            
            # Monta o dicionário para este veículo
            result[produto][veiculo] = {
                "cliques": formatar_numero(current_clicks),
                "cliques_var_sem_ant": formatar_numero(clicks_var_prev, eh_porcentagem=True),
                "cliques_var_med_3": formatar_numero(clicks_var_avg3, eh_porcentagem=True),
                "conversoes": formatar_numero(current_conv),
                "conversoes_var_sem_ant": formatar_numero(conv_var_prev, eh_porcentagem=True),
                "conversoes_var_med_3": formatar_numero(conv_var_avg3, eh_porcentagem=True),
                "taxa_aquisicao": formatar_numero(current_conversion_rate, eh_porcentagem=True, casas_decimais=2),
                "taxa_aquisicao_var_sem_ant": formatar_numero(rate_var_prev, eh_porcentagem=True),
                "taxa_aquisicao_var_med_3": formatar_numero(rate_var_avg3, eh_porcentagem=True)
            }
    
    return result

# Função para encontrar logs
def encontrar_logs(df_logs):
    """
    Filtra os registros das últimas 4 semanas e retorna um dicionário estruturado por produto e veículo.
    
    Args:
        df_logs: DataFrame com os logs
        
    Returns:
        Dicionário com os logs por produto e veículo
    """
    # Converte a coluna de data para datetime
    df_logs['Data'] = pd.to_datetime(df_logs['Data'], format='%d/%m/%Y')

    # Remove espaços em branco de colunas de interesse
    df_logs['Produto'] = df_logs['Produto'].astype(str).str.strip()
    df_logs['Canal'] = df_logs['Canal'].astype(str).str.strip()
    # Define o intervalo de 4 semanas (28 dias) até a última semana atual (mesmo intervalo da função anterior)
    data_atual = df_logs['Data'].max()  # data máxima presente nos logs (considera que execução é após coletar logs recentes)
    # Vamos considerar as 4 semanas finais até essa data como "últimas 4 semanas"
    # Encontrar a segunda-feira da semana da data_atual (para alinhar semanas)
    semana_atual_segunda = data_atual - pd.Timedelta(days=data_atual.weekday())
    # Início das 4 semanas atrás
    inicio_4_semanas = semana_atual_segunda - pd.Timedelta(days=21)  # 21 dias atrás da segunda atual (começo da primeira das 4 semanas)
    # Filtra logs dentro desse intervalo (inclusive)
    mask = (df_logs['Data'] >= inicio_4_semanas) & (df_logs['Data'] <= data_atual)
    df_recent_logs = df_logs[mask]
    # Monta o dicionário de saída agrupado por Canal (veículo) e Produto
    logs_dict = {}
    for _, row in df_recent_logs.iterrows():
        canal = row['Canal']
        produto = row['Produto']
        data = row['Data'].strftime('%Y-%m-%d')
        # Combina tipo de otimização e detalhe em uma descrição única
        tipo_opt = str(row.get('Tipo de Otimização', ''))
        detalhe = str(row.get('Detalhamento Ação', ''))
        if detalhe and not pd.isna(detalhe):
            descricao = (tipo_opt + " - " + detalhe).strip(" -")
        else:
            descricao = tipo_opt
        # Inicializa níveis no dicionário, se não existentes
        if produto not in logs_dict:
            logs_dict[produto] = {}
        if canal not in logs_dict[produto]:
            logs_dict[produto][canal] = []
        # Adiciona o log no formato {data: ..., descricao: ...}
        logs_dict[produto][canal].append({
            "data": data,
            "descricao": descricao
        })
    return logs_dict

# Função para converter para o novo formato JSON
def converter_para_novo_formato(dados_por_produto, logs_por_produto):
    """
    Converte os dados do formato original para o novo formato solicitado.
    
    Args:
        dados_por_produto: Dicionário com os dados por produto e veículo
        logs_por_produto: Dicionário com os logs por produto e veículo
        
    Returns:
        Dicionário no novo formato solicitado
    """
    # Novo formato solicitado
    novo_formato = {
        "produtos": {}
    }
    
    # Para cada produto nos dados
    for produto, veiculos in dados_por_produto.items():
        novo_formato["produtos"][produto] = {}
        
        # Calcular totais para o produto (Todos)
        total_cliques = 0
        total_conversoes = 0
        
        # Adicionar dados de cada veículo para este produto
        for veiculo, dados in veiculos.items():
            if veiculo != "Todos":  # Ignorar o "Todos" original se existir
                novo_formato["produtos"][produto][veiculo] = dados
                
                # Somar para o total
                total_cliques += dados.get("cliques", 0)
                total_conversoes += dados.get("conversoes", 0)
                
                # Adicionar logs se existirem
                if produto in logs_por_produto and veiculo in logs_por_produto[produto]:
                    novo_formato["produtos"][produto][veiculo]["logs"] = logs_por_produto[produto][veiculo]
        
        # Calcular taxa de aquisição total
        taxa_aquisicao_total = (total_conversoes / total_cliques * 100.0) if total_cliques != 0 else 0.0
        
        # Adicionar o total como "Todos"
        novo_formato["produtos"][produto]["Todos"] = {
            "cliques": total_cliques,
            "conversoes": total_conversoes,
            "taxa_aquisicao": formatar_numero(taxa_aquisicao_total, eh_porcentagem=True, casas_decimais=2)
        }
        
        # Tentar calcular variações para o total se possível
        # (Isso exigiria ter os totais das semanas anteriores, que não estamos calculando aqui)
        # Por simplicidade, deixamos apenas os valores absolutos
    
    # Adicionar explicação sobre 7d e 3sem
    explicacao = {
        "metadados": {
            "explicacao_termos": {
                "7d": "Representa os dados dos últimos 7 dias corridos, comparados com o período anterior equivalente.",
                "3sem": "Representa a média das últimas 3 semanas completas (excluindo a semana atual e a semana anterior)."
            },
            "data_atualizacao": datetime.date.today().strftime('%Y-%m-%d'),
            "formato_dados": {
                "cliques": "Número inteiro representando a quantidade total de cliques",
                "cliques_var_sem_ant": "Variação percentual dos cliques em relação à semana anterior (valor arredondado)",
                "cliques_var_med_3": "Variação percentual dos cliques em relação à média das 3 semanas anteriores (valor arredondado)",
                "conversoes": "Número inteiro representando a quantidade total de conversões/aquisições",
                "conversoes_var_sem_ant": "Variação percentual das conversões em relação à semana anterior (valor arredondado)",
                "conversoes_var_med_3": "Variação percentual das conversões em relação à média das 3 semanas anteriores (valor arredondado)",
                "taxa_aquisicao": "Porcentagem de conversões por clique, com 2 casas decimais",
                "taxa_aquisicao_var_sem_ant": "Variação percentual da taxa de aquisição em relação à semana anterior (valor arredondado)",
                "taxa_aquisicao_var_med_3": "Variação percentual da taxa de aquisição em relação à média das 3 semanas anteriores (valor arredondado)"
            }
        }
    }
    
    # Mesclar explicação com o novo formato
    novo_formato.update(explicacao)
    
    return novo_formato

# Função para explicar com IA
def explica_com_ai(novo_formato_json) -> str:
    """
    Usa a biblioteca google.generativeai (Gemini 2.5 PRO) para gerar um texto de alerta
    com base no novo formato JSON que contém dados e logs.
    Retorna a string resultante pronta para exibir na aba de alertas.
    """
    # 1) Prepara o prompt
    instrucao = (
        "Você é um analista de dados das campanhas de mídia paga da organização configurada "
        + legacy_rule("LABEL_OR_MATCH_33")
        + "esta semana e merece atenção. Use logs recentes para tentar explicar variações. "
        "Seja objetivo (até 2 frases), traga no máximo dois pontos por produto. Inclua a data observada. "
        "NÃO formate nada como markdown, tenha em mente que isso vai direto pro Power BI, nada de **. "
        "\n\n"
        "IMPORTANTE: Os valores numéricos já estão formatados para facilitar sua análise:\n"
        "- 'cliques' e 'conversoes' são números inteiros (ex: 1500 cliques)\n"
        "- 'cliques_var_sem_ant' e similares são variações percentuais (ex: -6.3 significa queda de 6,3%)\n"
        "- 'taxa_aquisicao' é uma porcentagem com 2 casas decimais (ex: 0.95 significa taxa de 0,95%)\n"
        "\n"
        "O termo '7d' refere-se aos últimos 7 dias corridos, e '3sem' refere-se à média das últimas 3 semanas completas."
    )
    
    dados_json = json.dumps(novo_formato_json, ensure_ascii=False)
    prompt = (
        f"Dados de desempenho semanal e logs:\n{dados_json}\n\n"
        f"{instrucao}"
    )

    # 2) Chama o Gemini
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        # Em caso de erro, logue e retorne string vazia ou mensagem de fallback
        print(f"[explica_com_ai] Erro ao chamar Gemini: {e}")
        return ""

# Função para transformar recomendação em CSV
def transforma_recomendacao_csv(texto_recomendacao, output_path):
    """
    Transforma o texto de recomendação gerado pelo modelo em um CSV estruturado com colunas:
    Data, Veículo e Recomendação.
    
    Args:
        texto_recomendacao: Texto gerado pela IA
        output_path: Caminho para salvar o CSV
        
    Returns:
        Caminho do arquivo CSV salvo
    """
    if not texto_recomendacao:
        return None

    # 1) Calcula último domingo como data padrão (formato dd/mm/YYYY)
    hoje = datetime.date.today()
    dias_para_domingo = (hoje.weekday() + 1) % 7  # segunda=0 ... domingo=6
    ultimo_domingo = hoje - datetime.timedelta(days=dias_para_domingo)
    default_date = ultimo_domingo.strftime('%d/%m/%Y')

    # 2) Lista de veículos para detecção
    veiculos = ["Google", "Meta", "TikTok"]

    rows = []
    # 3) Itera sobre cada linha não vazia
    for line in texto_recomendacao.splitlines():
        line = line.strip()
        if not line:
            continue

        # 4) Regex para extrair [data opcional] + produto + texto
        m = re.match(
            r'^(?:(\d{2}/\d{2}(?:/\d{2,4})?)\s*[-:]?\s*)?'
            + legacy_rule("LABEL_OR_MATCH_34"),
            line, flags=re.IGNORECASE
        )
        if m:
            date_part = m.group(1) or default_date
            produto   = m.group(2).capitalize()
            rec_text  = m.group(3).strip()
        else:
            date_part = default_date
            produto   = None
            rec_text  = line

        # 5) Detecta veículo único na recomendação
        encontrados = [v for v in veiculos if v.lower() in rec_text.lower()]
        veic = encontrados[0] if len(encontrados) == 1 else "Geral"

        # 6) Monta texto final da recomendação
        if produto:
            recomendacao = f"{produto}: {rec_text}"
        else:
            recomendacao = rec_text

        rows.append({
            "Data": date_part,
            "Veículo": veic,
            "Recomendação": recomendacao
        })

    # 7) Gera o DataFrame e salva CSV (sem coluna de variação)
    df_recs = pd.DataFrame(rows, columns=["Data", "Veículo", "Recomendação"])
    df_recs.to_csv(output_path, index=False, encoding='utf-8-sig')
    return output_path

def main():
    """
    Função principal que centraliza todo o fluxo, sem necessidade de parâmetros externos.
    Usa os caminhos definidos globalmente no início do arquivo.
    
    Returns:
        0 se sucesso, 1 se erro
    """
    try:
        print("Iniciando processamento de logs e alertas...")
        
        # 1) Carrega os dados
        print(f"Carregando dados do dashboard: {PATH_ESTRATEGICO_DIARIO}")
        df_dashboard = pd.read_csv(PATH_ESTRATEGICO_DIARIO)
        
        print(f"Carregando logs: {PATH_LOGS}")
        df_logs = pd.read_csv(PATH_LOGS, sep=";")
        
        # 2) Calcula diferenças percentuais semanais
        print(legacy_rule("LABEL_OR_MATCH_35"))
        dados_por_produto = calcular_diferenca_percentual_semanal(df_dashboard)
        
        # 3) Lê os logs de otimização
        print(legacy_rule("LABEL_OR_MATCH_36"))
        logs_por_produto = encontrar_logs(df_logs)
        
        # 4) Converte para o novo formato (mantém apenas em memória)
        print("Convertendo dados para o novo formato...")
        novo_formato = converter_para_novo_formato(dados_por_produto, logs_por_produto)
        
        # 5) Gera o texto de alertas com a IA usando o novo formato
        print("Gerando texto de alerta com IA...")
        alerta_texto = explica_com_ai(novo_formato)
        if alerta_texto:
            print("Texto de alerta gerado pela IA:")
            print(alerta_texto)
            
            # 6) Transforma o texto em CSV de recomendações
            print(f"Transformando texto em CSV e salvando em: {OUTPUT_CSV}")
            csv_path = transforma_recomendacao_csv(alerta_texto, OUTPUT_CSV)
            if csv_path:
                print(f"Recomendações salvas com sucesso em: {csv_path}")
            else:
                print("Não foi possível gerar o CSV de recomendações.")
        else:
            print("Nenhum alerta gerado (texto vazio ou erro na API).")
        
        print("Processamento concluído com sucesso!")
        return 0
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        return 1

if __name__ == "__main__":
    # Executa a função principal
    main()
