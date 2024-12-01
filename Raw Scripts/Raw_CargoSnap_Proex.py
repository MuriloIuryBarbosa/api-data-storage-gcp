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
token = os.getenv('Token_CargoSnap_Proex')
table_name = os.getenv('Table_CargoSnap_Proex')

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

# Função para processar uma página de dados da API
def processar_pagina(url, headers, retries=5):
    for i in range(retries):
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            data = response.json()
            return data.get('data', [])
        elif response.status_code == 429:
            wait_time = 2 ** i + random.random()
            print(f"Limite de requisições excedido. Aguardando {wait_time:.2f} segundos antes de tentar novamente.")
            time.sleep(wait_time)
        else:
            print(f"Falha ao obter dados da página {url}. Status code: {response.status_code}")
            return []
    return []

# Função para verificar se um ID já está registrado no banco de dados
def id_ja_registrado(cursor, table_name, id):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (str(id),))
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

        # Inicializa o número da página
        page = 1
        more_pages = True

        # Loop enquanto houver mais páginas
        while more_pages:
            # URL da API com o parâmetro de página atualizado
            url = f'https://api.cargosnap.com/api/v2/forms/1757?format=json&token=[{token}]&limit=200&startdate=2024-01-01&enddate=2099-12-31&page={page}'

            # Fazendo a requisição GET para a API
            data = processar_pagina(url, headers={})

            # Se não houver mais dados, sair do loop
            if not data:
                more_pages = False
                break

            # Verificar IDs já registrados no banco de dados
            sql_select = f"SELECT id FROM {table_name}"
            cursor.execute(sql_select)
            registered_ids = {str(result[0]) for result in cursor.fetchall()}  # Conjunto para rápida verificação

            ids_to_register = [item for item in data if str(item['id']) not in registered_ids]
            novos_registros_pagina = len(ids_to_register)

            for item in ids_to_register:
                id_Proex = item['id']
                try:
                    # Usando json_normalize para converter dados aninhados
                    flat_data = json_normalize(item)

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

                    # Exemplo de saída de progresso para cada registro
                    print(f"Registrando ID {id_Proex}")

                except mysql.connector.Error as err:
                    print(f"Erro ao inserir o ID {id_Proex}: {err}")
                    continue

            # Imprimir quantos registros foram encontrados e quantos serão registrados
            print(f"IDs retornados na página {page}: {len(data)}")
            print(f"IDs já registrados no banco de dados: {len(registered_ids)}")
            print(f"Novos registros a serem registrados: {novos_registros_pagina}")

            # Incrementa o número da página para a próxima iteração
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

if __name__ == "__main__":
    main()
