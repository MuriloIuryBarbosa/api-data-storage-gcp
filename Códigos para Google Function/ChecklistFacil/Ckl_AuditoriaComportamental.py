import requests
import pymysql
from datetime import datetime
import time  # Importar a biblioteca time para lidar com a espera em caso de erro 429

# Lista de IDs de checklists
checklist_ids = [
    522152,
    104501
]

# Definindo o header
headers = {
    'Authorization': 'Bearer PA4zilFkZqbKonEUv4aiLXkWjgKqh0ZvpWeH7KAYM8ves48K9fJOrZOlm9VsxjhOXmXzTlDf1Au0Ih6U6GQJ15cE1R9PLVNmxyO9MYlxqfFSavD646bkdxHwKiCEEbos'
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
                sql_count_existing_ids = "SELECT COUNT(*) FROM Ckl_AuditoriaComportamental WHERE id_Ckl IN (%s)"
                
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
                    sql_select = "SELECT COUNT(*) FROM Ckl_AuditoriaComportamental WHERE id_Ckl = %s"
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

                                    if item['name'] == 'O operador de empilhadeira está portando o seu cartão de identificação conforme NR 11?':
                                        COMP_OperadorIdentificado = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Posicionou o trava rodas no pneu dianteiro esquerdo do veiculo?':
                                        OPER_TravaRodasCorreto = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Aplicou o sistema LOTO? (chave retirada e inserida na caixa porta-chaves)':
                                        OPER_AplicouLOTO = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Está seguindo as Instruções de Segurança do Trabalho da atividade auditada?':
                                        OPER_SeguindoInsSeg = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'O colaborador soube dizer onde encontrar a IST das suas atividades?':
                                        OPER_ISTDaAtividade = item['answer']['evaluative'] if item.get('answer') else None


                                    if item['name'] == 'As máquinas, ferramentas ou equipamentos de trabalho, estão em boas condições de uso?':
                                        OPER_FerramentasTrabOk = item['answer']['evaluative'] if item.get('answer') else None


                                    if item['name'] == 'Inspecionou visualmente e realizou um teste de funcionamento no trava quedas retrátil?':
                                        OPER_TravaQuedasInspecionado = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Ao identificar interferência ou sobreposição, o colaborador paralisa a atividade e orienta o colega?':
                                        OPER_ParalisaTarefaPorDesvio = item['answer']['evaluative'] if item.get('answer') else None
                                        
                                    if item['name'] == 'O que é Percepção de Risco?':
                                        CONH_PercepcaoRisco = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'O que é direito de recusa?':
                                        CONH_DireitoDeRecusa = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'O que é DDS?':
                                        CONH_Dds = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Tem conhecimento da sua Instrução de Trabalho? Soube dizer ao menos 3 recomendações de segurança contidas na IST de sua atividade?':
                                        CONH_ConheceIST = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Tem conhecimento da NR que rege a sua atividade? (Ex: NR 11, NR 18, NR 33 e NR 35)':
                                        CONH_ConheceNRs = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Sabe dizer quais são os riscos da sua atividade?':
                                        CONH_ConheceRiscosAtiv = item['answer']['evaluative'] if item.get('answer') else None

                                    if item['name'] == 'Tem conhecimento da política de Ver e Agir? Sabe como registrar?':
                                        CONH_ConheceVerAgir = item['answer']['evaluative'] if item.get('answer') else None
                            
                            # Inserir os dados no banco de dados MySQL
                            sql = "INSERT INTO Ckl_AuditoriaComportamental (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, CAB_IdentificacaoAuditado, CAB_IdentificacaoAuditor, EPI_EPIsNecessariosUtilizados, EPI_EPIEmBoasCondicoes, EPI_EPIUtilizadoCorretamente, EPI_EPILimpo, COMP_Uniformizado, COMP_Adorno, COMP_ParticipouDDS, COMP_PreencheuCKL, COMP_PortandoCelular, COMP_OperadorIdentificado, OPER_TravaRodasCorreto, OPER_AplicouLOTO, OPER_SeguindoInsSeg, OPER_ISTDaAtividade, OPER_FerramentasTrabOk, OPER_TravaQuedasInspecionado, OPER_ParalisaTarefaPorDesvio, CONH_PercepcaoRisco, CONH_DireitoDeRecusa, CONH_Dds, CONH_ConheceIST, CONH_ConheceNRs, CONH_ConheceRiscosAtiv, CONH_ConheceVerAgir, DataAplicacao) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, CAB_IdentificacaoAuditado, CAB_IdentificacaoAuditor, EPI_EPIsNecessariosUtilizados, EPI_EPIEmBoasCondicoes, EPI_EPIUtilizadoCorretamente, EPI_EPILimpo, COMP_Uniformizado, COMP_Adorno, COMP_ParticipouDDS, COMP_PreencheuCKL, COMP_PortandoCelular, COMP_OperadorIdentificado, OPER_TravaRodasCorreto, OPER_AplicouLOTO, OPER_SeguindoInsSeg, OPER_ISTDaAtividade, OPER_FerramentasTrabOk, OPER_TravaQuedasInspecionado, OPER_ParalisaTarefaPorDesvio, CONH_PercepcaoRisco, CONH_DireitoDeRecusa, CONH_Dds, CONH_ConheceIST, CONH_ConheceNRs, CONH_ConheceRiscosAtiv, CONH_ConheceVerAgir, DataAplicacao)
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
