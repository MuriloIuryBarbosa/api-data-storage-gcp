import requests
import mysql.connector
from datetime import datetime, timedelta, timedelta
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
token = os.getenv('Token_CargoSnap_BabySitter5400')
table_name = os.getenv('Table_CargoSnap_BabySitter5400_2')

# Função para calcular as datas de início e fim
def get_date_range(days=90):
    """Retorna uma tupla com a data de início e a data de hoje formatadas."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# Obter intervalo de datas
start_date, end_date = get_date_range()

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
    url = f'https://api.cargosnap.com/api/v2/forms/4137?format=json&token=[{token}]&limit=200&startdate={start_date}&enddate={end_date}&page={page}'

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
                    Armazem = None
                    PlacaCarreta = None
                    LocalCarregamento = None
                    Observacoes = None
                    NomeConferente = None
                    NomeOperador = None
                    Mercado = None
                    TipoMaterial = None
                    AvariasNoCarregamento = None
                    NumeroConteiner = None
                    BobinaPaletizada = None
                    NomeCliente_ME = None
                    NomeCliente_MI = None
                    NomeCliente_Outro = None
                    FardoDesalinhado = None
                    MaterialEmContatoComAssoalho = None
                    InsetosOuSujeiraNoVeiculo = None
                    OdoresMofoOuUmidadeNoveiculo = None
                    OutrasMercadoriasCarregadasNoVeiculo = None
                    CarroceriaEmBomEstado = None
                    VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade = None
                    LonaEstaEmBomEstado = None
                    AssinaturaOperador = None
                    AssinaturaConferente = None

                    # Extraindo os dados do item
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        if campo['label'] == 'Unidade':
                            Unidade = campo['value']
                        if campo['label'] == 'DT':
                            Dt = campo['value']
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
                        if campo['label'] == 'Armazém':
                            Armazem = campo['value']
                        if campo['label'] == 'Placa da Carreta':
                            PlacaCarreta = campo['value']
                        if campo['label'] == 'Local de Carregamento (Limeira)':
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
                        if campo['label'] == 'Bobina Paletizada?':
                            BobinaPaletizada = campo['value']
                        if campo['label'] == 'Nome do Cliente ME':
                            NomeCliente_ME = campo['value']
                        if campo['label'] == 'Nome do Cliente MI':
                            NomeCliente_MI = campo['value']
                        if campo['label'] == 'Nome do Cliente - Outro':
                            NomeCliente_Outro = campo['value']
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
                        if campo['label'] == 'Assinatura Operador':
                            AssinaturaOperador = campo['value']
                        if campo['label'] == 'Assinatura Conferente':
                            AssinaturaConferente = campo['value']

                    # Inserindo os dados no banco de dados MySQL
                    sql = f"INSERT INTO {table_name} (id_BabySitter, id_Formulario, Usuario, Formulario, Dt, DataInicioBaby, DataFimBaby, DuplaColeta, Unidade, Transportadora, NomeTransportadora, Armazem, PlacaCarreta, LocalCarregamento, Observacoes, NomeConferente, NomeOperador, Mercado, TipoMaterial, AvariasNoCarregamento, NumeroConteiner, BobinaPaletizada, NomeCliente_ME, NomeCliente_MI, NomeCliente_Outro, FardoDesalinhado, MaterialEmContatoComAssoalho, InsetosOuSujeiraNoVeiculo, OdoresMofoOuUmidadeNoveiculo, OutrasMercadoriasCarregadasNoVeiculo, CarroceriaEmBomEstado, VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade, LonaEstaEmBomEstado, AssinaturaOperador, AssinaturaConferente) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    val = (id_BabySitter, id_Formulario, Usuario, Formulario, Dt, DataInicioBaby, DataFimBaby, DuplaColeta, Unidade, Transportadora, NomeTransportadora, Armazem, PlacaCarreta, LocalCarregamento, Observacoes, NomeConferente, NomeOperador, Mercado, TipoMaterial, AvariasNoCarregamento, NumeroConteiner, BobinaPaletizada, NomeCliente_ME, NomeCliente_MI, NomeCliente_Outro, FardoDesalinhado, MaterialEmContatoComAssoalho, InsetosOuSujeiraNoVeiculo, OdoresMofoOuUmidadeNoveiculo, OutrasMercadoriasCarregadasNoVeiculo, CarroceriaEmBomEstado, VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade, LonaEstaEmBomEstado, AssinaturaOperador, AssinaturaConferente)
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