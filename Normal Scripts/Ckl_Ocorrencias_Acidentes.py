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
token = os.getenv('Token_Ckl_Ocorrencias_Acidentes')
table_name = os.getenv('Table_Ckl_Ocorrencias_Acidentes')

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
        checklist_ids = [474561]

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

                            # Inicializar todas as variáveis
                            DataOcorrencia = None 
                            HoraOcorrencia = None
                            TipoOcorrencia = None
                            OcorrenciaComVitimas = None
                            LiderImediato = None
                            OcorrenciaComunicada = None
                            Descricao = None 
                            TipoColaborador = None
                            NomeColaborador = None
                            FuncaoColaborador = None 
                            TempoNaFuncao = None
                            TempoDeEmpresa = None
                            HorasTrabalhadasAntesDoAcidente = None
                            DiasTrabalhadosDesdeFolga = None 
                            JornadaTrabalhada = None 
                            RegiaoDoCorpoAtingida = None 
                            RegiaoAtingidaMembroInferior = None 
                            LadoAtingidoMembroInferior = None
                            TipoDeLesao = None 
                            LesaoDetalhada = None 
                            UtilizavaEPI = None 
                            EquipeResponsavelInvestigacao = None
                            FluxogramaCausaEfeito = None 
                            
                            RegiaoAtingidaMembroSuperior = None 
                            OcorrenciaSemVitimas = None
                            ClassificacaoOcorrencia = None 
                            MaquinaOuEquipamentoRelacionadoOcorrencia = None
                            PrincipalOfensor = None
                            HouveAtestado = None 
                            MeioAmbienteGerouOcorrencia = None
                            MedicaoQuantificacaoGerouOcorrencia = None 
                            ManagementGestaoGerouOcorrencia = None 
                            EmpregadoMateriaPrimaGerouOcorrencia = None
                            LocalDeTrabalhoGerouOcorrencia = None
                            LocalDeTrabalho = None
                            FerramentasGerouOcorrencia = None
                            MetodoDeTrabalhoGerouOcorrencia = None
                            MaoDeObraGerouOcorrencia = None
                            CausaRaiza = None
                            RegiaoAtingidaTorax = None
                            LagoAtingidoTorax = None 
                            Cid = None
                            MaquinasFerramentasEquipamentos = None
                            MaoDeObra = None 
                            RegiaoAtingidaCabeca = None 
                            LadoAtingidoCabeca = None 
                            MaterialEmpregadoMateriaPrima = None 
                            MetodoDeTrabalho = None
                            LadoAtingidoMembrosSuperiores = None 
                            RegiaoAtingidaAbdomen = None 
                            ManagementGestao = None 
                            DiasDeAfastamento = None

                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'Data da ocorrência':
                                        DataOcorrencia = item['answer']['text'] if item.get('answer') else None
                                        DataOcorrencia = converter_data(DataOcorrencia)
                                    if item['name'] == 'Hora da ocorrência':
                                        HoraOcorrencia = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Tipo de ocorrência':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoOcorrencia = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Classificação da ocorrência':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            ClassificacaoOcorrencia = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Ocorrência sem vítimas':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            OcorrenciaSemVitimas = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Ocorrência com vítimas':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            OcorrenciaComVitimas = item['answer']['selectedOptions'][0]['text']     
                                    if item['name'] == 'Veículo/Máquina/Equipamento Relacionado Diretamente com a ocorrência':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MaquinaOuEquipamentoRelacionadoOcorrencia = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Qual foi o principal ofensor?':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            PrincipalOfensor = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Líder imediato':
                                        LiderImediato = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'O Supervisor, técnico de segurança e cliente foram avisados (Nessa ordem)?':
                                        OcorrenciaComunicada = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Descrição da ocorrência e fotográfia.':
                                        Descricao = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Colaborador é Fixo ou Intermitente?':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoColaborador = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Nome do colaborador':
                                        NomeColaborador = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Função':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FuncaoColaborador = item['answer']['selectedOptions'][0]['text']    
                                    if item['name'] == 'Tempo na função':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TempoNaFuncao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Tempo de Empresa':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TempoDeEmpresa = item['answer']['selectedOptions'][0]['text']    
                                    if item['name'] == 'Horas Trabalhadas antes do acidente':
                                        HorasTrabalhadasAntesDoAcidente = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Dias Trabalhados em Relação à Ultima Folga':
                                        DiasTrabalhadosDesdeFolga = item['answer']['text'] if item.get('answer') else None                                        
                                    if item['name'] == 'Jornada Trabalhada':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JornadaTrabalhada = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Região do corpo atingida':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            RegiaoDoCorpoAtingida = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especificação da Região Atingida da Cabeça':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            RegiaoAtingidaCabeca = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Lado Atingido - Cabeça':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            LadoAtingidoCabeca = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especificação da Região Atingida dos Membros Superiores':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            RegiaoAtingidaMembroSuperior = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Lado atingido - Membros Superiores':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            LadoAtingidoMembrosSuperiores = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especificação da Região Atingida dos membros Inferiores':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            RegiaoAtingidaMembroInferior = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Lado atingido - Membros inferiores':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            LadoAtingidoMembroInferior = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especificação da Região Atingida do Torax':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            RegiaoAtingidaTorax = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Tipo de Lesão':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoDeLesao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Lesão Detalhada':
                                        LesaoDetalhada = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Utilizava EPI?':
                                        UtilizavaEPI = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Equipe responsável pela investigação':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            EquipeResponsavelInvestigacao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Fluxograma de Causa e Efeito':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FluxogramaCausaEfeito = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Houve Atestado? Se Sim inserir em anexo':
                                        HouveAtestado = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Algo relacionado ao Meio Ambiente foi responsável por gerar o evento?':
                                        MeioAmbienteGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Algo relacionado ao Medição/Quantificação foi responsável por gerar o evento?':
                                        MedicaoQuantificacaoGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None                                        
                                    if item['name'] == 'Algo relacionado ao Management/Gestão foi responsável por gerar o evento?':
                                        ManagementGestaoGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None                                        
                                    if item['name'] == 'Algo relacionado ao Material Empregado/Matéria Prima foi responsável por gerar o evento?':
                                        EmpregadoMateriaPrimaGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None                                                                                
                                    if item['name'] == 'Algo relacionado ao Local/Área de Trabalho foi responsável por gerar o evento?':
                                        LocalDeTrabalhoGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None  
                                    if item['name'] == 'Dias de afastamento':
                                        DiasDeAfastamento = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Algo relacionado a Maquinas Ferramentas/Equipamentos foi responsável por gerar o evento?':
                                        FerramentasGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None                                          
                                    if item['name'] == 'Algo relacionado ao Método de Trabalho foi responsável por gerar o evento?':
                                        MetodoDeTrabalhoGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None                                                                                  
                                    if item['name'] == 'Algo relacionado a Mão de Obra foi responsável por gerar o evento?':
                                        MaoDeObraGerouOcorrencia = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Especificação da Região Atingida do Abdômen':
                                        RegiaoAtingidaAbdomen = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Lado atingido - Torax':
                                        LagoAtingidoTorax = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'Cid':
                                        Cid = item['answer']['text'] if item.get('answer') else None    
                                    if item['name'] == 'Causa Raiz':
                                        CausaRaiza = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Veículo/Máquina/Equipamento Relacionado Diretamente com a ocorrência':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MaquinasFerramentasEquipamentos = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Mão de Obra':
                                        MaoDeObra = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Local / Área de Trabalho':
                                        LocalDeTrabalho = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Material Empregado / Material Prima':
                                        MaterialEmpregadoMateriaPrima = item['answer']['text'] if item.get('answer') else None                                                                              

                                    if item['name'] == 'Método de Trabalho':
                                        MetodoDeTrabalho = item['answer']['text'] if item.get('answer') else None
                                    if item['name'] == 'Management/Gestão':
                                        ManagementGestao = item['answer']['text'] if item.get('answer') else None                                        

                            sql = f"""INSERT INTO {table_name} (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, DataOcorrencia, HoraOcorrencia, TipoOcorrencia, OcorrenciaComVitimas, LiderImediato, OcorrenciaComunicada, Descricao, TipoColaborador, NomeColaborador, FuncaoColaborador, TempoNaFuncao, TempoDeEmpresa, HorasTrabalhadasAntesDoAcidente, DiasTrabalhadosDesdeFolga, JornadaTrabalhada, RegiaoDoCorpoAtingida, RegiaoAtingidaMembroInferior, LadoAtingidoMembroInferior, TipoDeLesao, LesaoDetalhada, UtilizavaEPI, EquipeResponsavelInvestigacao, FluxogramaCausaEfeito, RegiaoAtingidaMembroSuperior, OcorrenciaSemVitimas, ClassificacaoOcorrencia, MaquinaOuEquipamentoRelacionadoOcorrencia, PrincipalOfensor, HouveAtestado, MeioAmbienteGerouOcorrencia, MedicaoQuantificacaoGerouOcorrencia, ManagementGestaoGerouOcorrencia, EmpregadoMateriaPrimaGerouOcorrencia, LocalDeTrabalhoGerouOcorrencia, LocalDeTrabalho, FerramentasGerouOcorrencia, MetodoDeTrabalhoGerouOcorrencia, MaoDeObraGerouOcorrencia, CausaRaiza, RegiaoAtingidaTorax, LagoAtingidoTorax, CID, MaquinasFerramentasEquipamentos, MaoDeObra, RegiaoAtingidaCabeca, LadoAtingidoCabeca, MaterialEmpregadoMateriaPrima, MetodoDeTrabalho, LadoAtingidoMembrosSuperiores, RegiaoAtingidaAbdomen, ManagementGestao, DiasDeAfastamento) VALUES (%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s)"""
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, DataOcorrencia, HoraOcorrencia, TipoOcorrencia, OcorrenciaComVitimas, LiderImediato, OcorrenciaComunicada, Descricao, TipoColaborador, NomeColaborador, FuncaoColaborador, TempoNaFuncao, TempoDeEmpresa, HorasTrabalhadasAntesDoAcidente, DiasTrabalhadosDesdeFolga, JornadaTrabalhada, RegiaoDoCorpoAtingida, RegiaoAtingidaMembroInferior, LadoAtingidoMembroInferior, TipoDeLesao, LesaoDetalhada, UtilizavaEPI, EquipeResponsavelInvestigacao, FluxogramaCausaEfeito, RegiaoAtingidaMembroSuperior, OcorrenciaSemVitimas, ClassificacaoOcorrencia, MaquinaOuEquipamentoRelacionadoOcorrencia, PrincipalOfensor, HouveAtestado, MeioAmbienteGerouOcorrencia, MedicaoQuantificacaoGerouOcorrencia, ManagementGestaoGerouOcorrencia, EmpregadoMateriaPrimaGerouOcorrencia, LocalDeTrabalhoGerouOcorrencia, LocalDeTrabalho, FerramentasGerouOcorrencia, MetodoDeTrabalhoGerouOcorrencia, MaoDeObraGerouOcorrencia, CausaRaiza, RegiaoAtingidaTorax, LagoAtingidoTorax, Cid, MaquinasFerramentasEquipamentos, MaoDeObra, RegiaoAtingidaCabeca, LadoAtingidoCabeca, MaterialEmpregadoMateriaPrima, MetodoDeTrabalho, LadoAtingidoMembrosSuperiores, RegiaoAtingidaAbdomen, ManagementGestao, DiasDeAfastamento)
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
