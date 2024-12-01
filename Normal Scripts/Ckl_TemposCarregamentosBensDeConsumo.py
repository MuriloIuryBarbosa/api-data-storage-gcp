import requests
import mysql.connector
from datetime import datetime
import random
import time
import pymysql
import warnings
import os
from dotenv import load_dotenv

# Suprimir avisos de InsecureRequestWarning
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar a conexão com o MySQL
db_ip_public = os.getenv('MYSQL_HOST')
db_user = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')
token = os.getenv('Token_Ckl_TemposCarregamentosBensDeConsumo')
table_name = os.getenv('Table_Ckl_TemposCarregamentosBensDeConsumo2')

# Função para testar a conexão com o MySQL
def testar_conexao():
    try:
        conexao = pymysql.connect(
            host=db_ip_public,
            user=db_user,
            password=db_password,
            db=db_name,
            ssl_disabled=True  # Ignorar verificação SSL
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
def processar_pagina(url_ConsultaIDChecklist, headers, retries=5):
    for i in range(retries):
        response = requests.get(url_ConsultaIDChecklist, headers=headers, verify=False)  # Ignorar verificação SSL
        if response.status_code == 200:
            evaluations_data = response.json()
            evaluation_ids = [evaluation['evaluationId'] for evaluation in evaluations_data['data']]
            return evaluation_ids
        elif response.status_code == 429:
            wait_time = 2 ** i + random.random()
            print(f"Limite de requisições excedido. Aguardando {wait_time:.2f} segundos antes de tentar novamente.")
            time.sleep(wait_time)
        else:
            print(f"Falha ao obter a lista de 'evaluationId' da página {url_ConsultaIDChecklist}. Status code: {response.status_code}")
            return []
    return []

def converter_data(data_str):
    if data_str is None:
        return None  # Retorna None se a data não estiver presente
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Erro ao converter a data '{data_str}': {e}")
        return None  # Retorna None se a data estiver em um formato inesperado
    

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
        checklist_ids = [549492]
        # Definindo os headers
        headers = {
            'Authorization': f'Bearer {token}'
        }
        # Inicializa contador de novos processos
        num_new_processes = 0

        # Loop através dos IDs dos checklists
        for checklist_id in checklist_ids:
            page = 1
            more_pages = True

            while more_pages:
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

                sql_select = f"SELECT id_Ckl FROM {table_name}"
                cursor.execute(sql_select)
                registered_ids = [result[0] for result in cursor.fetchall()]

                ids_to_register = [id for id in evaluation_ids if id not in registered_ids]
                novos_registros_pagina = len(ids_to_register)

                for i, id in enumerate(ids_to_register, start=1):
                    try:
                        url_evaluation = f"https://integration.checklistfacil.com.br/v2/evaluations/{id}"
                        response = requests.get(url_evaluation, headers=headers, verify=False)  # Ignorar verificação SSL
                        if response.status_code == 200:
                            informacoes = response.json()

                            # Extrair informações adicionais
                            checklist_name = informacoes['checklist']['name']
                            unidade_nome = informacoes['unit']['name']
                            usuario_nome = informacoes['user']['name']
                            status = informacoes['status']
                            assinaturas = ", ".join([f"{assinatura['name']} - {assinatura['role']}" for assinatura in informacoes['signatures']])

                            # Acessando a lista de dicionários dentro de 'categories'
                            categorias = informacoes.get('categories', [])

                            # Variáveis para armazenar os valores desejados
                            UnidadeRegistro = None
                            QtdFuncionariosCarregando = None
                            Operacao = None
                            NumeroDT = None
                            PlacaCarreta = None
                            DataInicio = None
                            HoraInicio = None
                            DataFim = None
                            HoraFim = None
                            MotivoAtraso1 = None
                            MotivoAtraso2 = None

                            # Iterando sobre a lista de dicionários
                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'Unidade':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            UnidadeRegistro = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Qtd. Funcionários no carregamento':
                                        QtdFuncionariosCarregando = item['answer']['number'] if item.get('answer') else None                            
                                    if item['name'] == 'Operação':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Operacao = item['answer']['selectedOptions'][0]['text']                          
                                    if item['name'] == 'Número da DT':
                                        NumeroDT = item['answer']['number'] if item.get('answer') else None
                                    if item['name'] == 'Placa da carreta':
                                        PlacaCarreta = item['answer']['text'] if item.get('answer') else None   
                                    if item['name'] == 'Data de início':
                                        DataInicio = item['answer']['text'] if item.get('answer') else None
                                        DataInicio = converter_data(DataInicio)  # Converter para YYYY-MM-DD
                                    if item['name'] == 'Data de finalização':
                                        DataFim = item['answer']['text'] if item.get('answer') else None
                                        DataFim = converter_data(DataFim)  # Converter para YYYY-MM-DD
                                    if item['name'] == 'Hora de início':
                                        HoraInicio = item['answer']['text'] if item.get('answer') else None                                                        
                                    if item['name'] == 'Hora de finalização':
                                        HoraFim = item['answer']['text'] if item.get('answer') else None    
                                    if item['name'] == '1 - Motivo do atraso':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MotivoAtraso1 = item['answer']['selectedOptions'][0]['text']
                                    elif item['name'] == '2 - Motivo do atraso':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MotivoAtraso2 = item['answer']['selectedOptions'][0]['text']

                            # Inserir os dados no banco de dados MySQL
                            sql = f"INSERT INTO {table_name} (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, UnidadeRegistro, QtdFuncionariosCarregando, Operacao, NumeroDT, PlacaCarreta, DataInicio, HoraInicio, DataFim, HoraFim, MotivoAtraso1, MotivoAtraso2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, UnidadeRegistro, QtdFuncionariosCarregando, Operacao, NumeroDT, PlacaCarreta, DataInicio, HoraInicio, DataFim, HoraFim, MotivoAtraso1, MotivoAtraso2)
                                                
                            cursor.execute(sql, val)
                            conexao.commit()
                            num_new_processes += 1

                            # Exemplo de saída de progresso para cada registro
                            print(f"Registrando ID {id} ({i}/{novos_registros_pagina}) da página {page}")
                        else:
                            print(f"A requisição para o ID {id} falhou: {response.status_code}")

                    except mysql.connector.Error as err:
                        print(f"Erro ao inserir o ID {id}: {err}")
                        # Continue para o próximo ID
                        continue
                print(f"IDs retornados na página {page}: {total_registros_pagina}")
                print(f"IDs já registrados no banco de dados: {len(registered_ids)}")
                print(f"Novos registros a serem registrados: {novos_registros_pagina}")

                #more_pages = len(evaluation_ids) > 0
                page += 1
                time.sleep(65)

    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao MySQL: {err}")

    finally:
        try:
            if conexao.is_connected():
                cursor.close()
                conexao.close()
                print("Conexão ao MySQL fechada.")
        except NameError:
            pass

    print("Processo finalizado.")
    print(f"Novos Processos registrados no Banco: {num_new_processes}")

if __name__ == "__main__":
    main()
