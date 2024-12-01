import requests
import pymysql
from datetime import datetime
import time  # Importar a biblioteca time para lidar com a espera em caso de erro 429

# Lista de IDs de checklists
checklist_ids = [
    466746
]

# Definindo o header
headers = {
    'Authorization': 'Bearer ETxnWmRI0GJrRcfsiJjGcUTyvTWjWoxjKTsHT70xRJyxoR9Z8biemrEm8iw0RyqGH0yUWbjNFcZQIUL6xHUUdsbQin5IVZ2tzUy825K3CIEKJdZf2kjreEykMSaucdol'
}

# Configurar a conexão com o Cloud SQL usando IP público
db_ip_public = "35.224.29.30"  # Altere para o IP público da sua instância do Cloud SQL
db_user = "Murilo"
db_password = "Shurillo@100"
db_name = "db_guiza"

# Função para processar uma página de avaliações
def processar_pagina(url_ConsultaIDChecklist, headers):
    response = requests.get(url_ConsultaIDChecklist, headers=headers)
    if response.status_code == 200:
        evaluations_data = response.json()
        evaluation_ids = [evaluation['evaluationId'] for evaluation in evaluations_data['data']]
        return evaluation_ids
    elif response.status_code == 429:
        print("Limite de requisições excedido. Aguarde alguns segundos e tente novamente.")
        time.sleep(60)  # Aguardar 60 segundos antes de tentar novamente
        return processar_pagina(url_ConsultaIDChecklist, headers)  # Chamar a função novamente após esperar
    else:
        print(f"Falha ao obter a lista de 'evaluationId' da página {url_ConsultaIDChecklist}. Status code: {response.status_code}")
        return []

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

# Função principal
def main(data, context):
    if not testar_conexao():
        print("Abortando a execução devido a falha na conexão com o MySQL.")
        return

    for checklist_id in checklist_ids:
        page = 1
        max_pages = 5
        num_new_processes = 0  # Inicializa a variável

        while page <= max_pages:
            url = f"https://api-analytics.checklistfacil.com.br/v1/evaluations?status=6&checklistId={checklist_id}&page={page}"
            evaluation_ids = processar_pagina(url, headers)
            total_registros_pagina = len(evaluation_ids)

            print(f"Total de IDs mapeados para na página {page} do checklist {checklist_id}: {total_registros_pagina}")

            if total_registros_pagina == 0:
                print("Nenhum ID encontrado nesta página. Avançando para a próxima página...")
                page += 1
                continue

            try:
                # Conectar ao banco de dados MySQL
                conexao = pymysql.connect(
                    host=db_ip_public,
                    user=db_user,
                    password=db_password,
                    db=db_name
                )
                cursor = conexao.cursor()

                # Consultar quantos IDs já existem no banco de dados
                sql_count_existing_ids = "SELECT COUNT(*) FROM Ckl_CartaoVerde WHERE id_Ckl IN (%s)"
                
                # Convertendo a lista de IDs em uma string separada por vírgula
                ids_str = ','.join(map(str, evaluation_ids))

                # Executar a consulta SQL
                cursor.execute(sql_count_existing_ids % ids_str)
                num_existing_ids = cursor.fetchone()[0]

                print(f"Consultando no banco se {num_existing_ids} IDs já estão registrados...")

                # Verificar quantos IDs ainda precisam ser registrados
                num_remaining_ids = total_registros_pagina - num_existing_ids
                print(f"{num_existing_ids} IDs já registrados, {num_remaining_ids} IDs ainda precisam ser registrados.")

            except pymysql.MySQLError as err:
                print(f"Erro ao consultar IDs existentes no banco: {err}")

            for i, id in enumerate(evaluation_ids, start=1):
                try:
                    # Verificar se o ID já existe no banco de dados
                    sql_select = "SELECT COUNT(*) FROM Ckl_CartaoVerde WHERE id_Ckl = %s"
                    cursor.execute(sql_select, (id,))
                    result = cursor.fetchone()

                    if result[0] == 0:  # ID não existe no banco de dados
                        url_evaluation = f"https://integration.checklistfacil.com.br/v2/evaluations/{id}"
                        response = requests.get(url_evaluation, headers=headers)
                        if response.status_code == 200:
                            informacoes = response.json()

                            checklist_name = informacoes['checklist']['name']
                            unidade_nome = informacoes['unit']['name']
                            usuario_nome = informacoes['user']['name']
                            status = informacoes['status']
                            assinaturas = ", ".join([f"{assinatura['name']} - {assinatura['role']}" for assinatura in informacoes['signatures']])

                            categorias = informacoes.get('categories', [])

                            # Variáveis para armazenar os valores desejados
                            ColaboradorReconhecido = None
                            AcaoPositiva = None
                            DataAplicacao = None

                            # Iterando sobre a lista de dicionários
                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'NOME COMPLETO DO COLABORADOR RECONHECIDO':
                                        ColaboradorReconhecido = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'DATA DE APLICAÇÃO':
                                        DataAplicacao = item['answer']['text'] if item.get('answer') else None
                                        DataAplicacao = converter_data(DataAplicacao)  # Converter para YYYY-MM-DD
                                    elif item['name'] == 'AÇÃO POSITIVA OBSERVADA + FOTO DO CARTÃO.':
                                        AcaoPositiva = item['answer']['text'] if item.get('answer') else None

                            # Inserir os dados no banco de dados MySQL
                            sql = "INSERT INTO Ckl_CartaoVerde (id_Ckl, ColaboradorReconhecido, AcaoPositiva, Status, Checklist, Unidade, Usuario, Assinatura, DataChecklist) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, ColaboradorReconhecido, AcaoPositiva, status, checklist_name, unidade_nome, usuario_nome, assinaturas, DataAplicacao)

                            cursor.execute(sql, val)
                            conexao.commit()
                            num_new_processes += 1

                            print(f"Registrando ID {id} ({i}/{total_registros_pagina}) da página {page}")
                        else:
                            print(f"A requisição para o ID {id} falhou com o status code: {response.status_code}")

                except pymysql.MySQLError as err:
                    print(f"Erro ao inserir o ID {id}: {err}")
                    # Continue para o próximo ID
                    continue

            page += 1

# Função para converter data para o formato YYYY-MM-DD
def converter_data(data_str):
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None
