import requests
import mysql.connector
from datetime import datetime
import random
import time
import pymysql
import warnings

# Suprimir avisos de InsecureRequestWarning
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# Configurar a conexão com o Cloud SQL usando IP público
db_ip_public = "35.224.29.30"  # Altere para o IP público da sua instância do Cloud SQL
db_user = "Murilo"
db_password = "Shurillo@100"
db_name = "db_guiza"

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
def processar_pagina(url_ConsultaIDChecklist, headers, retries=5):
    for i in range(retries):
        response = requests.get(url_ConsultaIDChecklist, headers=headers, verify=False)
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

# Função para converter data para o formato YYYY-MM-DD
def converter_data(data_str):
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None

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

        # Lista de IDs de checklists (Checklist inutilizado: 182814)
        checklist_ids = [468457]

        # Definindo os headers
        headers = {
            'Authorization': 'Bearer 7S3WgLwQ4oYaAx3CgGEy5B1bsbHZ1VkF4hZscNV2V3d0BDFikhxr0bz8E6bEOzXpYg9zrGATw6ed5JWe3N31WirDQVOq75KgWEp9ua8Swk73xrBFko8I75v184icegRs'
        }

        # Inicializa contador de novos processos
        num_new_processes = 0

        # Loop através dos IDs dos checklists
        for checklist_id in checklist_ids:
            page = 1
            more_pages = True
            #max_pages = 9999999
            num_new_processes = 0  # Inicializa a variável
            zero_count = 0

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

                sql_select = "SELECT id_Ckl FROM d_Ckl_BabySitterTransf"
                cursor.execute(sql_select)
                registered_ids = [result[0] for result in cursor.fetchall()]

                ids_to_register = [id for id in evaluation_ids if id not in registered_ids]
                novos_registros_pagina = len(ids_to_register)

                for i, id in enumerate(ids_to_register, start=1):
                    try:
                        url_evaluation = f"https://integration.checklistfacil.com.br/v2/evaluations/{id}"
                        response = requests.get(url_evaluation, headers=headers, verify=False)
                        if response.status_code == 200:
                            informacoes = response.json()

                            checklist_name = informacoes['checklist']['name']
                            unidade_nome = informacoes['unit']['name']
                            usuario_nome = informacoes['user']['name']
                            status = informacoes['status']
                            assinaturas = ", ".join([f"{assinatura['name']} - {assinatura['role']}" for assinatura in informacoes['signatures']])
                            DataDoBaby = informacoes['startedAt']

                            categorias = informacoes.get('categories', [])

                            Turno = None
                            PlacaDaCarreta = None
                            HaviaAvariaNaCarga = None
                            TipoDeMaterial = None
                            PesoEmKG = None
                            MedidaImediata = None
                            AplicadorBabySitter = None
                            OperadorBabySitter = None
                            FotoDaAvaria = None

                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'TIPO DE MATERIAL':
                                        TipoDeMaterial = item['answer']['selectedOptions'][0]['text'] if item.get('answer') and item['answer'].get('selectedOptions') else None
                                    if item['name'] == 'PLACA DA CARRETA':
                                        PlacaDaCarreta = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'FOI IDENTIFICADO ALGUM MATERIAL AVARIADO?':
                                        HaviaAvariaNaCarga = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'PESO (EM KG)':
                                        PesoEmKG = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'FOTO DA AVARIA':
                                        if item.get('attachments'):
                                            # Extrai as URLs e as adiciona à lista FotoDaAvaria
                                            urls = [attachment['url'] for attachment in item['attachments']]
                                            # Concatena as URLs em uma string separada por vírgulas
                                            FotoDaAvaria = ','.join(urls)
                                            break  # Opcional: sai do loop se encontrou o item 'TURNO'


                                    if item['name'] == 'NOME COMPLETO DO RESPONSÁVEL PELA APLICAÇÃO DO BABY SITTER':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            AplicadorBabySitter = item['answer']['selectedOptions'][0]['text']

                                    if item['name'] == 'NOME COMPLETO DO OPERADOR DE EMPILHADEIRA QUE CARREGOU O VEÍCULO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            OperadorBabySitter = item['answer']['selectedOptions'][0]['text']

                                    if item['name'] == 'TURNO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Turno = item['answer']['selectedOptions'][0]['text']

                            # Inserir os dados no banco de dados MySQL
                            sql = "INSERT INTO d_Ckl_BabySitterTransf (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, DataDoBaby, Turno, PlacaDaCarreta, HaviaAvariaNaCarga, TipoDeMaterial, PesoEmKG, MedidaImediata, AplicadorBabySitter, OperadorBabySitter, FotoDaAvaria) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, DataDoBaby, Turno, PlacaDaCarreta, HaviaAvariaNaCarga, TipoDeMaterial, PesoEmKG, MedidaImediata, AplicadorBabySitter, OperadorBabySitter, FotoDaAvaria)

                            cursor.execute(sql, val)
                            conexao.commit()
                            num_new_processes += 1

                            print(f"Registrando ID {id} ({i}/{novos_registros_pagina}) da página {page}")
                        else:
                            print(f"A requisição para o ID {id} falhou com o status code: {response.status_code}")

                    except mysql.connector.Error as err:
                        print(f"Erro ao inserir o ID {id}: {err}")
                        continue

                print(f"IDs retornados na página {page}: {total_registros_pagina}")
                print(f"IDs já registrados no banco de dados: {len(registered_ids)}")
                print(f"Novos registros a serem registrados: {novos_registros_pagina}")

                more_pages = len(evaluation_ids) > 0
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
