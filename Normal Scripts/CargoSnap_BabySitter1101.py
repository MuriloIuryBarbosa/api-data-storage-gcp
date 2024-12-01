import requests
import mysql.connector
from datetime import datetime
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
token = os.getenv('Token_CargoSnap_BabySitter1101')
table_name = os.getenv('Table_CargoSnap_BabySitter1101_2')

# Conectar ao banco de dados MySQL
conexao = mysql.connector.connect(
    host = db_ip_public,
    user = db_user,
    password = db_password,
    database = db_name
)

cursor = conexao.cursor()

# Inicializa o número da página
page = 1
more_pages = True

# Loop enquanto houver mais páginas
while more_pages:
    # URL da API com o parâmetro de página atualizado
    url = f'https://api.cargosnap.com/api/v2/forms/1756?format=json&token=[{token}]&limit=200&startdate=1999-01-01&enddate=2099-12-31&page={page}'

    # Fazendo a requisição GET para a API
    response = requests.get(url, verify=False)

    # Verificando se a requisição foi bem sucedida
    if response.status_code == 200:
        # Convertendo a resposta para JSON
        data = response.json()

        # Se não houver mais dados, sair do loop
        if not data['data']:
            more_pages = False
            break

        # Consultar os id_BabySitter existentes no banco de dados
        cursor.execute(f"SELECT id_BabySitter FROM {table_name}")
        resultados_existente = cursor.fetchall()
        ids_existente = {row[0] for row in resultados_existente}  # Conjunto de ids existentes

        # Iterar sobre os dados da API
        total_registros_pagina = len(data['data'])
        novos_registros_pagina = 0
        for item in data['data']:
            id_BabySitter = item['id']
            if id_BabySitter not in ids_existente:
                novos_registros_pagina += 1
                try:
                    # Inicialize todas as variáveis com None ou um valor padrão
                    id_Formulario = None
                    Formulario = None
                    Usuario = None
                    Dt = None
                    DataInicioBaby = None
                    DataFimBaby = None
                    DuplaColeta = None
                    Unidade = None
                    Transportadora = None
                    NomeTransportadora = None
                    PlacaCarreta = None
                    LocalCarregamento = None
                    Observacoes = None
                    NomeConferente = None
                    NomeOperador = None
                    Mercado = None
                    TipoMaterial = None
                    AvariasNoCarregamento = None
                    NumeroConteiner = None
                    NomeCliente = None
                    FardoDesalinhado = None
                    MaterialEmContatoComAssoalho = None
                    InsetosOuSujeiraNoVeiculo = None
                    OdoresMofoOuUmidadeNoveiculo = None
                    OutrasMercadoriasCarregadasNoVeiculo = None
                    CarroceriaEmBomEstado = None
                    VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade = None
                    LonaEstaEmBomEstado = None
                    NomeOperador = None
                    NomeConferente = None
                    ProblemaNoStrecth = None
                    PaleteQuebrado = None
                    ProdutoComMolhadura = None
                    PresencaDeCorpoEstranho = None
                    PresencaDeInsetos = None
                    InstrucoesExpedicao = None
                    QtdPacotesCDRSuzano = None
                    QtdVolumesFTMFOB = None

                    # Extraindo os dados do item
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        if campo['label'] == 'Unidade':
                            Unidade = campo['value']
                        if campo['label'] == 'DT':
                            Dt = int(float(campo['value'])) if campo['value'].replace('.', '').isdigit() else None
                        if campo['label'] == 'Data de início do Baby Sitter':
                            DataInicioBaby = campo['value']
                        if campo['label'] == 'Data finalização do Baby Sitter':
                            DataFimBaby = campo['value']
                        if campo['label'] == 'DUPLA COLETA?':
                            DuplaColeta = campo['value']
                        if campo['label'] == 'Transportadora':
                            Transportadora = campo['value']
                        if campo['label'] == 'Nome Transportadora':
                            NomeTransportadora = campo['value']
                        if campo['label'] == 'Placa da Carreta':
                            PlacaCarreta = campo['value']
                        if campo['label'] == 'Local de Carregamento (Suzano)':
                            LocalCarregamento = campo['value']
                        if campo['label'] == 'Observações:':
                            Observacoes = campo['value']
                        if campo['label'] == 'Nome Conferente':
                            NomeConferente = campo['value']
                        if campo['label'] == 'Nome do operador que carregou':
                            NomeOperador = campo['value']
                        if campo['label'] == 'Mercado':
                            Mercado = campo['value']
                        if campo['label'] == 'Material':
                            TipoMaterial = campo['value']
                        if campo['label'] == 'Existem produtos com Avaria?':
                            AvariasNoCarregamento = campo['value']
                        if campo['label'] == 'No. Container':
                            NumeroConteiner = campo['value']
                        if campo['label'] == 'Nome do Cliente':
                            NomeCliente = campo['value']
                        if campo['label'] == 'Fardo Desalinhado?':
                            FardoDesalinhado = campo['value']
                        if campo['label'] == 'Os materiais estão acomodados em pallets ou sem contato direto com o assoalho do caminhão?':
                            MaterialEmContatoComAssoalho = campo['value']
                        if campo['label'] == 'Existem presenças visíveis de pragas urbanas (mosquitos, formigas, baratas, pombos, ratos, qualquer tipo de inseto) ou qualquer evidência de sua presença como fezes, ninhos e outros?':
                            InsetosOuSujeiraNoVeiculo = campo['value']
                        if campo['label'] == 'Há odores estranhos, mofos e/ou umidade no caminhão e/ou nos produtos?':
                            OdoresMofoOuUmidadeNoveiculo = campo['value']
                        if campo['label'] == 'Existem de produtos químicos, tóxicos ou outros não pertinentes a entrega em cima do veículo que possui produtos Suzano?':
                            OutrasMercadoriasCarregadasNoVeiculo = campo['value']
                        if campo['label'] == 'A Carroceria está em bom estado, livre de buracos ou desgaste que possa comprometer o produto ?':
                            CarroceriaEmBomEstado = campo['value']
                        if campo['label'] == 'Os produtos e o caminhão estão livres de poeira, areia, terra, farpas de madeira, pregos ou outras sujidades?':
                            VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade = campo['value']
                        if campo['label'] == 'Lonas (quando aplicável) está em bom estado, livre de buracos ou desgaste que possa comprometer o produto ?':
                            LonaEstaEmBomEstado = campo['value']
                        if campo['label'] == 'Problema no Strecth / Shirink?':
                            ProblemaNoStrecth = campo['value']
                        if campo['label'] == 'Palete Quebrado?':
                            PaleteQuebrado = campo['value']
                        if campo['label'] == 'Produto com Molhadura?':
                            ProdutoComMolhadura = campo['value']
                        if campo['label'] == 'Presença de Corpo Estranho?':
                            PresencaDeCorpoEstranho = campo['value']
                        if campo['label'] == 'Presença de Insetos?':
                            PresencaDeInsetos = campo['value']
                        if campo['label'] == 'Instruções Expedição':
                            InstrucoesExpedicao = campo['value']
                        if campo['label'] == 'Quantidade de Pacotes ( Cdr Suzano)':
                            QtdPacotesCDRSuzano = campo['value']
                        if campo['label'] == 'Quantidade de Volumes (FTM/BOB)':
                            QtdVolumesFTMFOB = campo['value']

                    # Inserindo os dados no banco de dados MySQL
                    sql = f"INSERT INTO {table_name} (id_BabySitter, id_Formulario, Usuario, Formulario, Dt, DataInicioBaby, DataFimBaby, DuplaColeta, Unidade, Transportadora, NomeTransportadora, PlacaCarreta, LocalCarregamento, Observacoes, Mercado, TipoMaterial, AvariasNoCarregamento, NumeroConteiner, NomeCliente, FardoDesalinhado, MaterialEmContatoComAssoalho, InsetosOuSujeiraNoVeiculo, OdoresMofoOuUmidadeNoveiculo, OutrasMercadoriasCarregadasNoVeiculo, CarroceriaEmBomEstado, VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade, LonaEstaEmBomEstado, NomeOperador, NomeConferente, ProblemaNoStrecth, PaleteQuebrado, ProdutoComMolhadura, PresencaDeCorpoEstranho, PresencaDeInsetos, InstrucoesExpedicao, QtdPacotesCDRSuzano, QtdVolumesFTMFOB) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    val = (id_BabySitter, id_Formulario, Usuario, Formulario, Dt, DataInicioBaby, DataFimBaby, DuplaColeta, Unidade, Transportadora, NomeTransportadora, PlacaCarreta, LocalCarregamento, Observacoes, Mercado, TipoMaterial, AvariasNoCarregamento, NumeroConteiner, NomeCliente, FardoDesalinhado, MaterialEmContatoComAssoalho, InsetosOuSujeiraNoVeiculo, OdoresMofoOuUmidadeNoveiculo, OutrasMercadoriasCarregadasNoVeiculo, CarroceriaEmBomEstado, VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade, LonaEstaEmBomEstado, NomeOperador, NomeConferente, ProblemaNoStrecth, PaleteQuebrado, ProdutoComMolhadura, PresencaDeCorpoEstranho, PresencaDeInsetos, InstrucoesExpedicao, QtdPacotesCDRSuzano, QtdVolumesFTMFOB)
                    cursor.execute(sql, val)
                    conexao.commit()

                    # Exemplo de saída de progresso para cada registro
                    print(f"Registrando ID {id_BabySitter} de {total_registros_pagina} da página {page}")
                except mysql.connector.Error as err:
                    print(f"Erro ao inserir o ID {id_BabySitter}: {err}")
                    # Continue para o próximo ID
                    continue

        # Imprimir quantos IDs foram retornados nesta página
        print(f"IDs retornados na página {page}: {total_registros_pagina}")
        # Imprimir quantos registros foram encontrados e quantos serão registrados
        print(f"IDs já registrados no banco de dados: {len(ids_existente)}")
        print(f"Novos registros a serem registrados: {novos_registros_pagina}")

        # Incrementa o número da página para a próxima iteração
        page += 1

    else:
        # Se a requisição falhar, imprime o status code e sai do loop
        print(f"A requisição falhou com o status code: {response.status_code}")
        break

# Fechar a conexão com o banco de dados
conexao.close()

# Imprime mensagem de conclusão
print('\nInserção concluída com sucesso!')