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
token = os.getenv('Token_Ckl_RelatorioDesvios')
table_name = os.getenv('Table_Ckl_RelatorioDesvios0')

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

        # Lista de IDs de checklists
        checklist_ids = [411635]

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
            zero_count = 0  # Contador de páginas consecutivas com 0 IDs

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
                        response = requests.get(url_evaluation, headers=headers, verify=False)
                        if response.status_code == 200:
                            informacoes = response.json()

                            checklist_name = informacoes['checklist']['name']
                            unidade_nome = informacoes['unit']['name']
                            usuario_nome = informacoes['user']['name']
                            status = informacoes['status']
                            assinaturas = ", ".join([f"{assinatura['name']} - {assinatura['role']}" for assinatura in informacoes['signatures']])

                            categorias = informacoes.get('categories', [])

                            DataDoDesvio = None
                            Solicitante = None
                            AbertoPara = None
                            DescricaoDesvio = None
                            AbertoPor = None
                            CaracterizacaoDesvio = None
                            DescricaoMedidaImediata = None
                            HouveMedidaImediata = None
                            TipoComportamental = None
                            TipoEstrutural = None
                            TipoDocumental = None
                            TipoReprovaVeiculo = None
                            TipoOcorrenciaSemVitima = None
                            NomeResponsavel = None

                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'DATA DO DESVIO':
                                        DataDoDesvio = item['answer']['text'] if item.get('answer') else None
                                        DataDoDesvio = converter_data(DataDoDesvio)
                                    if item['name'] == 'ABERTO PARA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            AbertoPara = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'ABERTO POR:':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            AbertoPor = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CARACTERIZAÇÃO DO DESVIO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CaracterizacaoDesvio = item['answer']['selectedOptions'][0]['text']

                                    if item['name'] == 'TIPO DE DESVIO - COMPORTAMENTAL':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoComportamental = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'TIPO DE DESVIO - ESTRUTURAL':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoEstrutural = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'TIPO DE DESVIO - DOCUMENTAL':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoDocumental = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'TIPO DE DESVIO - REPROVA DE VEICULO (SINALIZAR PLACA DO CARRO)':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoReprovaVeiculo = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'TIPO DE DESVIO - OCORRÊNCIA SEM VITIMA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoOcorrenciaSemVitima = item['answer']['selectedOptions'][0]['text']

                                    if item['name'] == 'SINALIZAR O NOME DO RESPONSAVEL PELO DESVIO (OBRIGATORIO INSERIR NOME E SOBRENOME E SEM CARACTERES ESPECIAIS)':
                                        NomeResponsavel = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'DESCRIÇÃO DO DESVIO':
                                        DescricaoDesvio = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'HOUVE MEDIDA IMEDIATA?':
                                        HouveMedidaImediata = item['answer']['evaluative'] if item.get('answer') else None
                                    elif item['name'] == 'DESCRIÇÃO DA MEDIDA IMEDIATA':
                                        if item.get('answer') and item['answer']['text']:
                                            DescricaoMedidaImediata = item['answer']['text']
                                            

                            sql = f"""INSERT INTO {table_name}
                                    (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, DataDesvio, AbertoPara, DescricaoDesvio, AbertoPor, CaracterizacaoDesvio, 
                                    DescricaoMedidaImediata, HouveMedidaImediata, TipoComportamental, TipoEstrutural, TipoDocumental, TipoReprovaVeiculo, TipoOcorrenciaSemVitima, NomeResponsavel) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, DataDoDesvio, AbertoPara, DescricaoDesvio, AbertoPor, CaracterizacaoDesvio, DescricaoMedidaImediata, HouveMedidaImediata, TipoComportamental, TipoEstrutural, TipoDocumental, TipoReprovaVeiculo, TipoOcorrenciaSemVitima, NomeResponsavel)
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
