import requests
import mysql.connector
from datetime import datetime
import random
import time
import pymysql
import warnings
import os
from dotenv import load_dotenv
from pandas import json_normalize  # Importa a função json_normalize do pandas

# Suprimir avisos de InsecureRequestWarning
warnings.simplefilter('ignore', requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar a conexão com o MySQL
db_ip_public = os.getenv('MYSQL_HOST')
db_user = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')
token = os.getenv('Token_Ckl_MapeamentoTombamentos')
table_name = os.getenv('Table_Ckl_MapeamentoTombamentos')

# Verificação de variáveis de ambiente
if not all([db_ip_public, db_user, db_password, db_name, token, table_name]):
    raise ValueError("Uma ou mais variáveis de ambiente não estão definidas.")

# Função para testar a conexão com o MySQL
def testar_conexao():
    try:
        conexao = pymysql.connect(
            host=db_ip_public,
            user=db_user,
            password=db_password,
            db=db_name
        )
        if conexao:
            print("Conexão ao MySQL estabelecida com sucesso.")
            conexao.close()
            return True
        else:
            print("Falha ao conectar ao MySQL.")
            return False
    except pymysql.MySQLError as err:
        print(f"Erro ao conectar ao MySQL: {err}")
        return False

# Função para processar uma página de avaliações
def processar_pagina(url, headers, retries=5):
    for i in range(retries):
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            evaluations_data = response.json()
            evaluation_ids = [str(evaluation['evaluationId']) for evaluation in evaluations_data.get('data', [])]
            return evaluation_ids
        elif response.status_code == 429:
            wait_time = 2 ** i + random.random()
            print(f"Limite de requisições excedido. Aguardando {wait_time:.2f} segundos antes de tentar novamente.")
            time.sleep(wait_time)
        else:
            print(f"Falha ao obter a lista de 'evaluationId' da página {url}. Status code: {response.status_code}")
            return []
    return []

# Função para converter data para o formato YYYY-MM-DD
def converter_data(data_str):
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None

# Função para verificar se um ID já está registrado no banco de dados
def id_ja_registrado(cursor, table_name, id):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (id,))
    result = cursor.fetchone()
    return result[0] > 0

# Função para obter as colunas existentes na tabela
def obter_colunas_existentes(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row[0] for row in cursor.fetchall()}  # Conjunto para rápida verificação

# Função para criar colunas dinamicamente se não existirem
def criar_colunas_dinamicamente(cursor, table_name, colunas):
    colunas_existentes = obter_colunas_existentes(cursor, table_name)
    for coluna in colunas:
        if coluna not in colunas_existentes:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{coluna}` TEXT")

# Função principal
def main():
    if not testar_conexao():
        print("Abortando a execução devido a falha na conexão com o MySQL.")
        return

    try:
        conexao = mysql.connector.connect(
            host=db_ip_public,
            user=db_user,
            password=db_password,
            database=db_name
        )
        cursor = conexao.cursor()

        # Lista de IDs de checklists
        checklist_ids = [579938]

        # Definindo os headers
        headers = {
            'Authorization': f'Bearer {token}'
        }

        # Inicializa contador de novos processos
        num_new_processes = 0

        # Loop através dos IDs dos checklists
        for checklist_id in checklist_ids:
            page = 1
            zero_count = 0  # Contador de páginas consecutivas com 0 IDs

            while True:
                url = f"https://api-analytics.checklistfacil.com.br/v1/evaluations?status=6&checklistId={checklist_id}&page={page}"
                evaluation_ids = processar_pagina(url, headers)
                total_registros_pagina = len(evaluation_ids)

                if total_registros_pagina == 0:
                    zero_count += 1
                else:
                    zero_count = 0

                if zero_count >= 2:
                    print("Duas páginas consecutivas retornaram 0 IDs. Finalizando o loop.")
                    break

                # Verificar IDs já registrados no banco de dados
                sql_select = f"SELECT id FROM {table_name}"
                cursor.execute(sql_select)
                registered_ids = {result[0] for result in cursor.fetchall()}  # Conjunto para rápida verificação

                ids_to_register = [id for id in evaluation_ids if id not in registered_ids]
                novos_registros_pagina = len(ids_to_register)

                for i, id in enumerate(ids_to_register, start=1):
                    try:
                        url_evaluation = f"https://integration.checklistfacil.com.br/v2/evaluations/{id}"
                        response = requests.get(url_evaluation, headers=headers, verify=False)
                        if response.status_code == 200:
                            informacoes = response.json()
                            
                            # Usando json_normalize para converter dados aninhados
                            flat_data = json_normalize(informacoes)

                            # Verificar e criar colunas dinamicamente
                            criar_colunas_dinamicamente(cursor, table_name, flat_data.columns)

                            # Preparar valores convertidos para inserção
                            valores = [str(flat_data[col].iloc[0]) for col in flat_data.columns]

                            # Inserir os dados no banco de dados MySQL
                            colunas = ', '.join(f"`{col}`" for col in flat_data.columns)
                            placeholders = ', '.join(['%s'] * len(flat_data.columns))
                            sql_insert = f"INSERT INTO {table_name} ({colunas}) VALUES ({placeholders})"

                            cursor.execute(sql_insert, valores)
                            conexao.commit()
                            num_new_processes += 1

                            print(f"Registrando ID {id} ({i}/{novos_registros_pagina}) da página {page}")
                        else:
                            print(f"A requisição para o ID {id} falhou com o status code: {response.status_code}")

                    except mysql.connector.Error as err:
                        print(f"Erro ao inserir o ID {id}: {err}")
                        continue

                print(f"Checklist {checklist_id} - IDs na página {page}: {total_registros_pagina}")
                print(f"IDs já registrados no banco de dados: {len(registered_ids)}")
                print(f"Novos registros a serem registrados: {novos_registros_pagina}")

                page += 1
                time.sleep(65)

    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao MySQL: {err}")

    finally:
        if conexao.is_connected():
            cursor.close()
            conexao.close()
            print("Conexão ao MySQL fechada.")

    print("Processo finalizado.")
    print(f"Novos Processos registrados no Banco: {num_new_processes}")

if __name__ == "__main__":
    main()
