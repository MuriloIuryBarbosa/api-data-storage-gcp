import os
import pandas as pd
from google.cloud import bigquery

# Função para expandir dados aninhados em um dicionário
def expand_dict(d, prefix=''):
    items = {}
    if isinstance(d, dict):
        for key, value in d.items():
            new_key = prefix + key
            if isinstance(value, dict):
                items.update(expand_dict(value, new_key + '_'))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    items.update(expand_dict(item, new_key + f'_{i}_'))
            else:
                items[new_key] = value
    elif isinstance(d, list):
        for i, item in enumerate(d):
            items.update(expand_dict(item, prefix + f'_{i}_'))
    else:
        items[prefix[:-1]] = d  # Tratar caso de valor único não estruturado
    
    return items

# Configurações do BigQuery
project_id = 'guizilim'
dataset_id = 'db_guiza'
raw_table_id = 'db_guiza_Raw_Ckl_CartaoVerde'
gold_table_id = 'Gold_Ckl_CartaoVerde'  # Nome da tabela gold fixo

# Definir a variável de ambiente para as credenciais
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Usuario\Documents\Python\Banco-GCP\BigQuery JSON Key\guizilim-41f2d09ce28c.json"

# Consulta ao BigQuery
query = f'SELECT categories FROM `{project_id}.{dataset_id}.{raw_table_id}` LIMIT 100'

# Ler os dados do BigQuery
client = bigquery.Client(project=project_id)
df = client.query(query).to_dataframe()

# Verificar se há dados na coluna 'categories'
if not df.empty and not pd.isna(df['categories'][0]):
    # Converter o texto da coluna 'categories' em uma estrutura Python usando eval
    df['categories'] = df['categories'].apply(eval)
    
    # Expandir os dados aninhados em um dicionário plano
    df['flattened_data'] = df['categories'].apply(expand_dict)
    
    # Criar um DataFrame a partir do dicionário plano
    normalized_df = pd.DataFrame(df['flattened_data'].tolist())

    # Caminho para salvar o arquivo Excel
    excel_path = r"C:\Users\Usuario\OneDrive - Suzano S A\Work\Data Analysis\Bases\logs_apis\categories_data.xlsx"
    
    # Salvar o DataFrame em um arquivo Excel
    normalized_df.to_excel(excel_path, index=False)
    
    print(f"Dados salvos com sucesso em {excel_path}")
else:
    print("Não há dados válidos na coluna 'categories'.")
