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
token = os.getenv('Token_Ckl_AuditoriaComportamental')
table_name = os.getenv('Table_Ckl_AuditoriaComportamental2')

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
        checklist_ids = [104501, 522152]

        # Definindo os headers
        headers = {
            'Authorization': f'Bearer {token}'
        }

        # Inicializa contador de novos processos
        num_new_processes = 0

        # Loop através dos IDs dos checklists
        for checklist_id in checklist_ids:
            page = 1
            zero_count = 0  # Contador para páginas com zero registros consecutivas

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

                            # Extrair informações adicionais
                            checklist_name = informacoes['checklist']['name']
                            unidade_nome = informacoes['unit']['name']
                            usuario_nome = informacoes['user']['name']
                            status = informacoes['status']
                            assinaturas = ", ".join([f"{assinatura['name']} - {assinatura['role']}" for assinatura in informacoes['signatures']])

                            # Acessando a lista de dicionários dentro de 'categories'
                            categorias = informacoes.get('categories', [])

                            # Variáveis para armazenar os valores desejados
                            CAB_IdentificacaoAuditado = None
                            CAB_IdentificacaoAuditor = None
                            DataAplicacao = None
                            EPI_EPIsNecessariosUtilizados = None
                            EPI_EPIEmBoasCondicoes = None
                            EPI_EPIUtilizadoCorretamente = None
                            EPI_EPILimpo = None
                            COMP_Uniformizado = None
                            COMP_Adorno = None
                            COMP_ParticipouDDS = None
                            COMP_PreencheuCKL = None
                            COMP_PortandoCelular = None
                            COMP_OperadorIdentificado = None
                            OPER_TravaRodasCorreto = None
                            OPER_AplicouLOTO = None
                            OPER_SeguindoInsSeg = None
                            OPER_ISTDaAtividade = None
                            OPER_FerramentasTrabOk = None
                            OPER_TravaQuedasInspecionado = None
                            OPER_ParalisaTarefaPorDesvio = None
                            CONH_PercepcaoRisco = None
                            CONH_DireitoDeRecusa = None
                            CONH_Dds = None
                            CONH_ConheceIST = None
                            CONH_ConheceNRs = None
                            CONH_ConheceRiscosAtiv = None
                            CONH_ConheceVerAgir = None

                            # Iterando sobre a lista de dicionários
                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'Identificação do auditado':
                                        CAB_IdentificacaoAuditado = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Identificação do auditor':
                                        signatures = item.get('signatures')
                                        if signatures and len(signatures) > 0:
                                            CAB_IdentificacaoAuditor = f"{signatures[0]['name']} - {signatures[0]['role']}"
                                    if item['name'] == 'Data de aplicação':
                                        DataAplicacao = item['answer']['text'] if item.get('answer') else None
                                        DataAplicacao = converter_data(DataAplicacao)  # Converter para YYYY-MM-DD                            
                                    if item['name'] == 'Os EPIs necessários são utilizados na atividade?':
                                        EPI_EPIsNecessariosUtilizados = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Os EPIs estão em boas condições de uso? ':
                                        EPI_EPIEmBoasCondicoes = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Os EPIs estão sendo utilizados corretamente?':
                                        EPI_EPIUtilizadoCorretamente = item['answer']['evaluative'] if item.get('answer') else None
                                    
                                    if item['name'] == 'Os EPIs possuem aspectos de limpo?':
                                        EPI_EPILimpo = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Devidamente Uniformizado?':
                                        COMP_Uniformizado = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Está sem qualquer tipo de adorno?':
                                        COMP_Adorno = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Participou do DDS? Soube dizer o assunto?':
                                        COMP_ParticipouDDS = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'O colaborador preencheu o check list de seu equipamento? ':
                                        COMP_PreencheuCKL = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Está sem portar ou utilizar o celular?':
                                        COMP_PortandoCelular = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'O operador de máquinas e/ou veículos está devidamente identificado e com crachá visível?':
                                        COMP_OperadorIdentificado = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Fez a aplicação do bloqueio ou outro sistema que garanta a isolação da máquina ou do veículo?':
                                        OPER_TravaRodasCorreto = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Aplicou o LOTO (Lockout and Tagout) ?':
                                        OPER_AplicouLOTO = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Seguindo as instruções de segurança de trabalho para esta atividade?':
                                        OPER_SeguindoInsSeg = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Possui o IST (Instrução de Segurança de Trabalho) da atividade e está executando conforme instrução?':
                                        OPER_ISTDaAtividade = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'As ferramentas para esta atividade estão em condições adequadas de trabalho?':
                                        OPER_FerramentasTrabOk = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'As travas de quedas estão inspecionadas e em condições de uso?':
                                        OPER_TravaQuedasInspecionado = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Paralisa a tarefa ao identificar desvio dos critérios e condições de segurança?':
                                        OPER_ParalisaTarefaPorDesvio = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Percebe riscos na atividade e age de maneira a evitar acidentes?':
                                        CONH_PercepcaoRisco = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Tem conhecimento do direito de recusa ao trabalho por perigo eminente?':
                                        CONH_DireitoDeRecusa = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Participou do DDS? Soube dizer o assunto?':
                                        CONH_Dds = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Conhece a Instrução de Segurança de Trabalho para esta atividade?':
                                        CONH_ConheceIST = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Conhece as normas regulamentadoras (NRs) aplicáveis à sua atividade?':
                                        CONH_ConheceNRs = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Conhece os riscos da atividade?':
                                        CONH_ConheceRiscosAtiv = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Conhece o ver e agir de segurança?':
                                        CONH_ConheceVerAgir = item['answer']['evaluative'] if item.get('answer') else None
                                        
                            # Inserindo os dados no banco de dados
                            sql_insert = "INSERT INTO Ckl_AuditoriaComportamental (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, CAB_IdentificacaoAuditado, CAB_IdentificacaoAuditor, EPI_EPIsNecessariosUtilizados, EPI_EPIEmBoasCondicoes, EPI_EPIUtilizadoCorretamente, EPI_EPILimpo, COMP_Uniformizado, COMP_Adorno, COMP_ParticipouDDS, COMP_PreencheuCKL, COMP_PortandoCelular, COMP_OperadorIdentificado, OPER_TravaRodasCorreto, OPER_AplicouLOTO, OPER_SeguindoInsSeg, OPER_ISTDaAtividade, OPER_FerramentasTrabOk, OPER_TravaQuedasInspecionado, OPER_ParalisaTarefaPorDesvio, CONH_PercepcaoRisco, CONH_DireitoDeRecusa, CONH_Dds, CONH_ConheceIST, CONH_ConheceNRs, CONH_ConheceRiscosAtiv, CONH_ConheceVerAgir, DataAplicacao) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, CAB_IdentificacaoAuditado, CAB_IdentificacaoAuditor, EPI_EPIsNecessariosUtilizados, EPI_EPIEmBoasCondicoes, EPI_EPIUtilizadoCorretamente, EPI_EPILimpo, COMP_Uniformizado, COMP_Adorno, COMP_ParticipouDDS, COMP_PreencheuCKL, COMP_PortandoCelular, COMP_OperadorIdentificado, OPER_TravaRodasCorreto, OPER_AplicouLOTO, OPER_SeguindoInsSeg, OPER_ISTDaAtividade, OPER_FerramentasTrabOk, OPER_TravaQuedasInspecionado, OPER_ParalisaTarefaPorDesvio, CONH_PercepcaoRisco, CONH_DireitoDeRecusa, CONH_Dds, CONH_ConheceIST, CONH_ConheceNRs, CONH_ConheceRiscosAtiv, CONH_ConheceVerAgir, DataAplicacao)

                            cursor.execute(sql_insert, val)
                            conexao.commit()

                            cursor.execute(sql_insert, val)
                            conexao.commit()
                            num_new_processes += 1
                            print(f"Registro {i}/{novos_registros_pagina} inserido com sucesso.")
                        else:
                            print(f"Falha ao obter informações da avaliação {id}. Status code: {response.status_code}")
                    except requests.exceptions.RequestException as e:
                        print(f"Erro ao processar a avaliação {id}: {str(e)}")
                        continue

                print(f"Processamento da página {page} concluído. {novos_registros_pagina} novos registros inseridos.")
                page += 1

        print(f"Processo concluído. Total de novos registros inseridos: {num_new_processes}")
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao MySQL: {err}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conexao' in locals() and conexao and conexao.is_connected():
            conexao.close()
            print("Conexão ao MySQL fechada.")

if __name__ == "__main__":
    main()