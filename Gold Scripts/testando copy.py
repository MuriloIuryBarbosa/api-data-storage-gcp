import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

def expand_dict(d, parent_key='', sep='_'):

    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(expand_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

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

    # Verificar e criar colunas dinamicamente na tabela gold
    table_ref = client.dataset(dataset_id).table(gold_table_id)
    table = client.get_table(table_ref)  # Obtém a estrutura da tabela

    # Lista de nomes das colunas existentes
    existing_columns = [schema_field.name for schema_field in table.schema]

    # Preparar os dados para inserção
    if not normalized_df.empty:
        rows_to_insert = normalized_df.to_dict(orient='records')

        # Inserir dados na tabela gold
        errors = client.insert_rows_json(table, rows_to_insert)
        if errors == []:
            print("Dados inseridos com sucesso na tabela gold.")
        else:
            print(f"Erros ao inserir dados: {errors}")
    else:
        print("Não há dados válidos para inserção.")
else:
    print("Não há dados na coluna 'categories' ou a coluna está vazia.")