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
table_name = os.getenv('Table_Ckl_RelatorioDesvios2')

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
        checklist_ids = [606139]

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
                            DataDesvio = None
                            AbertoPara = None
                            CaracterizacaoDesvio = None
                            UnidadeDesvio = None
                            UnidadeSelecionada = None
                            Bostik = None
                            CDCariacica = None
                            CDMaracanau = None
                            CDPinhais = None
                            CDRSerra = None
                            CDRSuzano = None
                            Ibema = None
                            ImerysCachoeiro = None
                            ImerysLimeira = None
                            ImerysMogi = None
                            Intermitente = None
                            JDCampinas = None
                            JDCatalao = None
                            JDHorizontina = None
                            JDIdaiatuba = None
                            JDMOntenegro = None
                            JSLJacarei = None
                            Matriz = None
                            MunkSJO = None
                            Niterra = None
                            OJIPiracicaba = None
                            PlacoFeira = None
                            PlacoMogi = None
                            SuzanLog = None
                            SuzanoAracruz = None
                            SuzanoBelem = None
                            SuzanoFabrica = None
                            SuzanoImperatriz = None
                            SuzanoLimeira = None
                            SuzanoRioVerde = None
                            WeberCamacari = None
                            WeberJandira = None
                            WeberMogi = None
                            YamahaSP = None
                            TipoDesvioComportamental = None
                            DesvioProcedimentoOperacionalSider = None
                            DesvioProcedimentoOperacionalGraneleiro = None
                            DesvioProcedimentoOperacionalCargaSeca = None
                            DesvioProcedimentoOperacionalBau = None
                            DesvioProcedimentoOperacionalConteiner = None
                            DesvioProcedimentoOperacionalOperEmpilhadeira = None
                            DesvioProcedimentoOperacionalLimpezaOrganizacao = None
                            DesvioProcedimentoOperacionalArtificeManutencao = None
                            DesvioProcedimentoOperacionalVistoria = None
                            DesvioProcedimentoOperacionalGpsita = None
                            DesvioProcedimentoOperacionalRetrabalho = None
                            DesvioProcedimentoOperacionalPaleteiraManual = None
                            DesvioProcedimentoOperacionalPaleteiraEletrica = None
                            DesvioProcedimentoOperacionalCkl = None
                            DesvioProcedimentoTrabalhoEmAltura = None
                            FalhaNoUsoDeEPI = None
                            TipoDesvioEstrutural = None
                            CondicoesDaEdificacao = None
                            Empihadeira = None
                            EPC = None
                            FerramentasManuais = None
                            Kombi = None
                            PaleteiraEletrica = None
                            PaleteiraManual = None
                            TrabalhoEmAltura = None
                            VeiculoBau = None
                            VeiculoGraneleiro = None
                            VeiculosCargaSeca = None
                            VeiculosConteiner = None
                            VeiculosPranchaCegonha = None
                            VeiculosSider = None
                            FalhaDocumental = None
                            FalhaEmDocumentosIndividual = None
                            DocumentosDeSegurandaEmpresa = None
                            FalhaDeIST = None
                            FalhaNaIST = None
                            CausaRaizOcorrenciaSemVitima = None
                            CausaRaizComportamental = None
                            CausaRaizEstrutural = None
                            HouveMedidaImediata = None
                            AbertoPor = None

                            for categoria in categorias:
                                items = categoria.get('items', [])
                                for item in items:
                                    if item['name'] == 'DATA DO DESVIO':
                                        DataDesvio = item['answer']['text'] if item.get('answer') else None
                                        DataDesvio = converter_data(DataDesvio)
                                    if item['name'] == 'ABERTO PARA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            AbertoPara = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CARACTERIZAÇÃO DO DESVIO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CaracterizacaoDesvio = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'PARA INDICAR O AUTOR DO DESVIO, SELECIONE A UNIDADE':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            UnidadeDesvio = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'BOSTIK':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Bostik = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CD CARIACICA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CDCariacica = item['answer']['selectedOptions'][0]['text']                                        
                                    if item['name'] == 'CD MARACANAU':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CDMaracanau = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CD PINHAIS':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CDPinhais = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CDR SERRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CDRSerra = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'CDR SUZANO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CDRSuzano = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'IBEMA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Ibema = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'IMERYS CACHOEIRO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            ImerysCachoeiro = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'IMERYS LIMEIRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            ImerysLimeira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'IMERYS MOGI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            ImerysMogi = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'INTERMITENTE':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Intermitente = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JD - CAMPINAS':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JDCampinas = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JD - CATALÃO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JDCatalao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JD - HORIZONTINA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JDHorizontina = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JD - INDAIATUBA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JDIdaiatuba = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JD - MONTENEGRO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JDMOntenegro = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'JSL JACAREI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            JSLJacarei = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'MATRIZ':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Matriz = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'MUNKSJO':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MunkSJO = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'NITERRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            MunkSJO = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'OJI - PIRACICABA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            OJIPiracicaba = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'PLACO FEIRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            PlacoFeira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'PLACO MOGI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            PlacoMogi = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANLOG':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanLog = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO ARACRUZ':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoAracruz = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO BELÉM':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoBelem = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO FABRICA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoFabrica = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO IMPERATRIZ':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoFabrica = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO LIMEIRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoLimeira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'SUZANO RIOVERDE':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            SuzanoRioVerde = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'WEBER CAMAÇARI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            WeberCamacari = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'WEBER JANDIRA':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            WeberJandira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'WEBER MOGI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            WeberMogi = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'YAMAHA - SP':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            YamahaSP = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'TIPO DE DESVIO COMPORTAMENTAL':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoDesvioComportamental = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Sider':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalSider = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Graneleiro':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalGraneleiro = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Carga Seca':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalCargaSeca = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Baú':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalBau = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Conteiner':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalConteiner = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Operação de Empilhadeira':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalOperEmpilhadeira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Limpeza e Organização':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalLimpezaOrganizacao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Artífice de Manutenção':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalArtificeManutencao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Vistoria':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalVistoria = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Gpsita':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalGpsita = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Retrabalho':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalRetrabalho = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Uso da Paleteira Manual':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalPaleteiraManual = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - Uso da Paleteira Elétrica':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalPaleteiraEletrica = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Operacional - CKL':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoOperacionalCkl = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Desvio Procedimento Trabalho em Altura':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DesvioProcedimentoTrabalhoEmAltura = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Falha no uso de EPI':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FalhaNoUsoDeEPI = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Tipo de desvio estrutural':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TipoDesvioEstrutural = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Condições da Edificação':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CondicoesDaEdificacao = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Empilhadeira':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Empihadeira = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'EPC':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            EPC = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Ferramentas Manuais':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FerramentasManuais = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Kombi':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            Kombi = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Paleteira Elétrica':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            PaleteiraEletrica = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Paleteira Manual':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            PaleteiraManual = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Trabalho em altura':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            TrabalhoEmAltura = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículo Baú':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculoBau = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículo Graneleiro':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculoGraneleiro = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículos Carga Seca':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculosCargaSeca = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículos Conteiner':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculosConteiner = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículos Prancha/Cegonha':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculosPranchaCegonha = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Veículos Sider':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            VeiculosSider = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especifique a falha documental':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FalhaDocumental = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Falha em documentos individual do colaborador':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FalhaEmDocumentosIndividual = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Documentos de segurança da empresa':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            DocumentosDeSegurandaEmpresa = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Falta de IST':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FalhaDeIST = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Falha na IST':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            FalhaNaIST = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Especifique a causa raíz da ocorrência sem vítima':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CausaRaizOcorrenciaSemVitima = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Causa Raiz Comportamental':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CausaRaizComportamental = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'Causa Raíz Estrutural':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            CausaRaizEstrutural = item['answer']['selectedOptions'][0]['text']
                                    if item['name'] == 'HOUVE MEDIDA IMEDIATA':
                                        HouveMedidaImediata = item['answer']['evaluative'] if item.get('answer') else None
                                    if item['name'] == 'ABERTO POR':
                                        if item.get('answer') and item['answer'].get('selectedOptions'):
                                            AbertoPor = item['answer']['selectedOptions'][0]['text']

                            sql = f"""INSERT INTO {table_name}
                                    (id_Ckl, Checklist, Unidade, Usuario, Status, Assinatura, DataDesvio, AbertoPara, CaracterizacaoDesvio, UnidadeDesvio, UnidadeSelecionada, 
                                    Bostik, CDCariacica, CDMaracanau, CDPinhais, CDRSerra, CDRSuzano, Ibema, ImerysCachoeiro, ImerysLimeira, ImerysMogi, Intermitente, JDCampinas, 
                                    JDCatalao, JDHorizontina, JDIdaiatuba, JDMOntenegro, JSLJacarei, Matriz, MunkSJO, Niterra, OJIPiracicaba, PlacoFeira, PlacoMogi, SuzanLog, SuzanoAracruz, 
                                    SuzanoBelem, SuzanoFabrica, SuzanoImperatriz, SuzanoLimeira, SuzanoRioVerde, WeberCamacari, WeberJandira, WeberMogi, YamahaSP, TipoDesvioComportamental, 
                                    DesvioProcedimentoOperacionalSider, DesvioProcedimentoOperacionalGraneleiro, DesvioProcedimentoOperacionalCargaSeca, DesvioProcedimentoOperacionalBau, 
                                    DesvioProcedimentoOperacionalConteiner, DesvioProcedimentoOperacionalOperEmpilhadeira, DesvioProcedimentoOperacionalLimpezaOrganizacao, DesvioProcedimentoOperacionalArtificeManutencao, 
                                    DesvioProcedimentoOperacionalVistoria, DesvioProcedimentoOperacionalGpsita, DesvioProcedimentoOperacionalRetrabalho, 
                                    DesvioProcedimentoOperacionalPaleteiraManual, DesvioProcedimentoOperacionalPaleteiraEletrica, DesvioProcedimentoOperacionalCkl, DesvioProcedimentoTrabalhoEmAltura, FalhaNoUsoDeEPI, 
                                    TipoDesvioEstrutural, CondicoesDaEdificacao, Empihadeira, EPC, FerramentasManuais, Kombi, PaleteiraEletrica, PaleteiraManual, TrabalhoEmAltura, 
                                    VeiculoBau, VeiculoGraneleiro, VeiculosCargaSeca, VeiculosConteiner, VeiculosPranchaCegonha, VeiculosSider, FalhaDocumental, FalhaEmDocumentosIndividual, 
                                    DocumentosDeSegurandaEmpresa, FalhaDeIST, FalhaNaIST, CausaRaizOcorrenciaSemVitima, CausaRaizComportamental, CausaRaizEstrutural, HouveMedidaImediata, AbertoPor) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                            val = (id, checklist_name, unidade_nome, usuario_nome, status, assinaturas, DataDesvio, AbertoPara, CaracterizacaoDesvio, UnidadeDesvio, UnidadeSelecionada, 
                                   Bostik, CDCariacica, CDMaracanau, CDPinhais, CDRSerra, CDRSuzano, Ibema, ImerysCachoeiro, ImerysLimeira, ImerysMogi, Intermitente, JDCampinas, 
                                   JDCatalao, JDHorizontina, JDIdaiatuba, JDMOntenegro, JSLJacarei, Matriz, MunkSJO, Niterra, OJIPiracicaba, PlacoFeira, PlacoMogi, SuzanLog, SuzanoAracruz, 
                                   SuzanoBelem, SuzanoFabrica, SuzanoImperatriz, SuzanoLimeira, SuzanoRioVerde, WeberCamacari, WeberJandira, WeberMogi, YamahaSP, TipoDesvioComportamental, 
                                   DesvioProcedimentoOperacionalSider, DesvioProcedimentoOperacionalGraneleiro, DesvioProcedimentoOperacionalCargaSeca, DesvioProcedimentoOperacionalBau, 
                                   DesvioProcedimentoOperacionalConteiner, DesvioProcedimentoOperacionalOperEmpilhadeira, DesvioProcedimentoOperacionalLimpezaOrganizacao, DesvioProcedimentoOperacionalArtificeManutencao, 
                                   DesvioProcedimentoOperacionalVistoria, DesvioProcedimentoOperacionalGpsita, DesvioProcedimentoOperacionalRetrabalho, 
                                   DesvioProcedimentoOperacionalPaleteiraManual, DesvioProcedimentoOperacionalPaleteiraEletrica, DesvioProcedimentoOperacionalCkl, DesvioProcedimentoTrabalhoEmAltura, FalhaNoUsoDeEPI, 
                                   TipoDesvioEstrutural, CondicoesDaEdificacao, Empihadeira, EPC, FerramentasManuais, Kombi, PaleteiraEletrica, PaleteiraManual, TrabalhoEmAltura, 
                                   VeiculoBau, VeiculoGraneleiro, VeiculosCargaSeca, VeiculosConteiner, VeiculosPranchaCegonha, VeiculosSider, FalhaDocumental, FalhaEmDocumentosIndividual, 
                                   DocumentosDeSegurandaEmpresa, FalhaDeIST, FalhaNaIST, CausaRaizOcorrenciaSemVitima, CausaRaizComportamental, CausaRaizEstrutural, HouveMedidaImediata, AbertoPor)
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
