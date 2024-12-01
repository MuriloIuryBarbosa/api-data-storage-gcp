import requests
import pymysql
from datetime import datetime
import time  # Importar a biblioteca time para lidar com a espera em caso de erro 429

# Lista de IDs de checklists
checklist_ids = [
    520643
]

# Definindo o header
headers = {
    'Authorization': 'Bearer 7UymqmBJ0Tl3vAvmjxy4x55EU8wbUtJxA62TWxTDS1eFdqTwD2JqSrWjJoe0lhsnwgIc7c7b6zydBHrTR87eRYbZl24syExNAGJytst8nbtPvxsYlwibmuKTEdrAWwp7'
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
                sql_count_existing_ids = "SELECT COUNT(*) FROM Ckl_AuditoriaProcessos WHERE id_Ckl IN (%s)"
                
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
                    sql_select = "SELECT COUNT(*) FROM Ckl_AuditoriaProcessos WHERE id_Ckl = %s"
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
                            Avaliador = None
                            NomeAuditado = None
                            ColaboradoresAuditados = None
                            ProcessoAuditado = None
                            VeiculoAuditado = None
                            AlgumProcessoFoiInfligido = None
                            HouveMedidaImediata = None
                            ProcedimentoAtendeOperacao = None
                            DataAplicacao = None
                            DiscriminarProcessosNaoSeguidos = None
                            FechamentoDeCaminhao = None
                            DescricaoMedidaImediata = None
                            FechamentoDoCaminhao = None
                            AcomodacaoCaminhaoNaDoca = None
                            AberturaDeCaminhao = None
                            InspecaoDeMaterial = None
                            VistoriaVeicular = None
                            TravaRodasERetiradaChava = None
                            LocalCarregamentoIsolado = None
                            AbrePortasBauComCuidado = None
                            InspecaoVisualeriorVeiculo = None
                            RetiraItensDesnecessariosDoVeiculo = None
                            UtilizaMascaraParaLimpezaassoalho = None
                            UtilizaEPIsNaAtividade = None
                            ErgonomiaAplicadaAoForrarAssoalho = None
                            InspecaoEChecklistDaPaleteria = None
                            CarregamentoManualPadronizado = None
                            AmarracoesDuranteProceso = None
                            LEvaPaletesVaziosConformeFinalzi = None
                            CarregamentoAmarracaoMadeiritePadronizado = None
                            LuzDeEmergenciaEmCarregamentoNoturno = None
                            RetiradaLuzDeEmergenciaAoFim = None
                            TodosOsPinosDaGuardaOk = None
                            TateamentoDaGuarda = None
                            RetiradaDeFueirosDeGuardas = None
                            CuidadoAoBaixarFueiros = None
                            TestesDeSistemaAntiQuedas = None
                            AcessaVeiculoComEscada = None
                            CoDeSegurancaETravaQuedasAoElonar = None
                            ErgonomiaAplicadaAoElonar = None
                            TravaQuedasLigadoAoCabo = None
                            DescidaComEscadaDoVeiculo = None
                            LancamentoDeCasCorreto = None
                            FerramenteCorretaParaCantoneiras = None
                            ApoioGanchoJouG = None
                            ApoioGanchoJouG2 = None
                            ProcedimentoSeguroParaCatracasFixas = None
                            UtilizaCordasParaPrenderLona = None
                            ApertaNosComPerna = None
                            Check360AoRedorVeiculo = None
                            ProlongadorAlavancaManuseioSider = None
                            MovimentaCortinaComCuidado = None
                            RemoveuTravessasComCuidado = None
                            TodasColunasPossuemTravas = None
                            MantidaAoMenosUmaColunaFizada = None
                            TrilhosPossuemFimDeCurso = None
                            RespeitoRegraseracaoPessoasMaquinas = None
                            UtilizaPalmasDaMaoAoTravarColunas = None
                            LancamentoCasCorreto = None
                            EncaixeDeTravessasSemForca = None
                            NaoTensionaCaComCortinaFechada = None
                            DataRealizacao = None
                            HorarioRealizadao = None
                            CatracaApoiandoFivelaComMaos = None
                            ExisteOportunidadeNaoMapeade = None
                            InspecaoVisualDeItensProtecao = None
                            AbreVeiculoUsandoEscada = None
                            ForcaExcessivaRemocaoDEArcos = None
                            AvaliadoTodosOsPinosParaPrender = None
                            AberturaDeGuardaPor3Colaboradores = None
                            ColaboradorSeguaCorrenteApoio = None
                            FerramentaAbeturaGuardasUsada = None
                            GuardasAbertasComCuidado = None
                            FueirosDuplosDesmontados = None
                            FueirosBaixadosComCuidado = None
                            GuardasInferioresBaixadasComCuidado = None
                            eracaoMaquinaPessoal = None
                            InspecionadoFuncionamentoTravaQuedas = None
                            UtilizaEsacadaNoFechamentoVeiculo = None
                            GuardasReposicionadasCorretamente = None
                            ContagemAltaAoPosicionarGuardas = None
                            TresColaboradoresReposicionaGuarda = None
                            ColaboradorDeCimaAguardaAjudaDebaixo = None
                            FerramenteFechamentoUtilizada = None


                            # Iterando sobre a lista de dicionários
                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'RESPONSÁVEL PELA AVALIAÇÃO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Avaliador = item['answer']['selectedOptions'][0]['text']
                                for item in items:
                                    if item['name'] == 'COLABORADORES AVALIADOS NO PROCESSO (inserir a assinatura de todos os envolvidos)':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            ColaboradoresAuditados = item['answer']['selectedOptions'][0]['text']
                                for item in items:
                                    if item['name'] == 'Identificação do auditado':
                                        NomeAuditado = item['answer']['text'] if item.get('answer') else None
                                for item in items:
                                    if item['name'] == 'QUAL PROCESSO SERÁ AUDITADO?':
                                        ProcessoAuditado = item['answer']['text'] if item.get('answer') else None
                                for item in items:
                                    if item['name'] == 'QUE TIPO DE VEÍCULO SERÁ AVALIADO?':
                                        VeiculoAuditado = item['answer']['text'] if item.get('answer') else None  
                                for item in items:
                                    if item['name'] == 'ALGUM PROCEDIMENTO FOI INFLINGIDO?':
                                        AlgumProcessoFoiInfligido = item['answer']['evaluative'] if item.get('answer') else None                                                                                           
                                    if item['name'] == 'DATA DE APLICAÇÃO':
                                        DataAplicacao = item['answer']['text'] if item.get('answer') else None
                                        DataAplicacao = converter_data(DataAplicacao)  # Converter para YYYY-MM-DD
                
                            
                            # Inserir os dados no banco de dados MySQL
                            sql = "INSERT INTO Ckl_AuditoriaProcessos (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, Avaliador, ColaboradoresAuditados, ProcessoAuditado, VeiculoAuditado, AlgumProcessoFoiInfligido, DataAplicacao) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, Avaliador, ColaboradoresAuditados, ProcessoAuditado, VeiculoAuditado, AlgumProcessoFoiInfligido, DataAplicacao)
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
