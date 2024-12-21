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
token = os.getenv('Token_CargoSnap_Proex')
table_name = os.getenv('Table_CargoSnap_Proex_2')

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
    host=db_ip_public,
    user=db_user,
    password=db_password,
    database=db_name
)

cursor = conexao.cursor()

# Inicializa o número da página 
page = 1
more_pages = True

# Loop enquanto houver mais páginas
while more_pages:
    # URL da API com o parâmetro de página atualizado
    url = f'https://api.cargosnap.com/api/v2/forms/1757?format=json&token={token}&limit=200&startdate={start_date}&enddate={end_date}&page={page}'

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

        # Consultar os id_Proex existentes no banco de dados
        cursor.execute(f"SELECT id_Proex FROM {table_name}")
        resultados_existente = cursor.fetchall()
        ids_existente = {row[0] for row in resultados_existente}  # Conjunto de ids existentes

        # Iterar sobre os dados da API
        total_registros_pagina = len(data['data'])
        novos_registros_pagina = 0
        for item in data['data']:
            id_Proex = item['id']
            if id_Proex not in ids_existente:
                novos_registros_pagina += 1
                try:
                    # Inicialize todas as variáveis com None ou um valor padrão
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']
                    Unidade = None
                    Local = None
                    TipoOcorrencia = None
                    TipoVeiculo = None
                    Mercado = None
                    Cliente = None
                    CodigoRecebedor = None
                    Dt = None
                    PesoProgramado = None
                    PesoFaturado = None
                    Remessa1 = None
                    Ordem1 = 0
                    Item1 = None
                    SKUBloqueada1 = None
                    TipoMaterial1 = None
                    PesoNaoAtendido1 = None
                    Remessa2 = None
                    Ordem2 = 0
                    Item2 = None
                    SKUBloqueada2 = None
                    TipoMaterial2 = None
                    PesoNaoAtendido2 = None
                    Remessa3 = None
                    Ordem3 = 0
                    Item3 = None
                    SKUBloqueada3 = None
                    TipoMaterial3 = None
                    PesoNaoAtendido3 = None
                    Remessa4 = None
                    Ordem4 = 0
                    Item4 = None
                    SKUBloqueada4 = None
                    TipoMaterial4 = None
                    PesoNaoAtendido4 = None
                    Remessa5 = None
                    Ordem5 = 0
                    Item5 = None
                    SKUBloqueada5 = None
                    TipoMaterial5 = None
                    PesoNaoAtendido5 = None
                    Turno = None
                    UsuarioSAP = None
                    DataProex = None
                    Comentario = None

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        label = campo['label']
                        value = campo['value']
                        if label == 'Unidade':
                            Unidade = value
                        elif label == 'Local':
                            Local = value
                        elif label == 'Tipo de Ocorrência':
                            TipoOcorrencia = value
                        elif label == 'Tipo de Veículo':
                            TipoVeiculo = value
                        elif label == 'Mercado':
                            Mercado = value
                        elif label == 'Cliente':
                            Cliente = value
                        elif label == 'Código do recebedor':
                            CodigoRecebedor = value
                        elif label == 'DT:':
                            Dt = value
                        elif label == 'Peso Programado:':
                            PesoProgramado = value
                        elif label == 'Peso Faturado:':
                            PesoFaturado = value
                        elif label == '1 - Remessa:':
                            Remessa1 = value
                        elif label == '1 - Ordem de venda':
                            Ordem1 = value if value is not None else 0
                        elif label == '1 - Item':
                            Item1 = value
                        elif label == '1 - SKU bloqueada:':
                            SKUBloqueada1 = value
                        elif label == '1 - Tipo Material:':
                            TipoMaterial1 = value
                        elif label == '1 - Peso não atendido especifico:':
                            PesoNaoAtendido1 = value
                        elif label == '2 - Remessa:':
                            Remessa2 = value
                        elif label == '2 - Ordem de venda':
                            Ordem2 = value if value is not None else 0
                        elif label == '2 - Item':
                            Item2 = value
                        elif label == '2 - SKU bloqueada:':
                            SKUBloqueada2 = value
                        elif label == '2 - Tipo Material:':
                            TipoMaterial2 = value
                        elif label == '2 - Peso não atendido especifico:':
                            PesoNaoAtendido2 = value
                        elif label == '3 - Remessa:':
                            Remessa3 = value
                        elif label == '3 - Ordem de venda':
                            Ordem3 = value if value is not None else 0
                        elif label == '3 - Item':
                            Item3 = value
                        elif label == '3 - SKU bloqueada:':
                            SKUBloqueada3 = value
                        elif label == '3 - Tipo Material:':
                            TipoMaterial3 = value
                        elif label == '3 - Peso não atendido especifico:':
                            PesoNaoAtendido3 = value
                        elif label == '4 - Remessa:':
                            Remessa4 = value
                        elif label == '4 - Ordem de venda':
                            Ordem4 = value if value is not None else 0
                        elif label == '4 - Item':
                            Item4 = value
                        elif label == '4 - SKU bloqueada:':
                            SKUBloqueada4 = value
                        elif label == '4 - Tipo Material:':
                            TipoMaterial4 = value
                        elif label == '4 - Peso não atendido especifico:':
                            PesoNaoAtendido4 = value
                        elif label == '5 - Remessa:':
                            Remessa5 = value
                        elif label == '5 - Ordem de venda':
                            Ordem5 = value if value is not None else 0
                        elif label == '5 - Item':
                            Item5 = value
                        elif label == '5 - SKU bloqueada:':
                            SKUBloqueada5 = value
                        elif label == '5 - Tipo Material:':
                            TipoMaterial5 = value
                        elif label == '5 - Peso não atendido especifico:':
                            PesoNaoAtendido5 = value
                        elif label == 'Turno':
                            Turno = value
                        elif label == 'UsuarioSAP':
                            UsuarioSAP = value
                        elif label == 'Data Proex':
                            DataProex = value
                        elif label == 'Comentário':
                            Comentario = value

                    # Preparando os valores para a inserção
                    colunas = [
                        'id_Formulario', 'id_Proex', 'Usuario', 'Formulario', 'Unidade', 'Local', 'TipoOcorrencia', 'TipoVeiculo', 
                        'Mercado', 'Cliente', 'CodigoRecebedor', 'Dt', 'PesoProgramado', 'PesoFaturado', 'Remessa1', 'Ordem1', 
                        'Item1', 'SKUBloqueada1', 'TipoMaterial1', 'PesoNaoAtendido1', 'Remessa2', 'Ordem2', 'Item2', 'SKUBloqueada2', 
                        'TipoMaterial2', 'PesoNaoAtendido2', 'Remessa3', 'Ordem3', 'Item3', 'SKUBloqueada3', 'TipoMaterial3', 
                        'PesoNaoAtendido3', 'Remessa4', 'Ordem4', 'Item4', 'SKUBloqueada4', 'TipoMaterial4', 'PesoNaoAtendido4', 
                        'Remessa5', 'Ordem5', 'Item5', 'SKUBloqueada5', 'TipoMaterial5', 'PesoNaoAtendido5', 'Turno', 'UsuarioSAP', 
                        'DataProex', 'Comentario'
                    ]
                    valores = [
                        id_Formulario, id_Proex, Usuario, Formulario, Unidade, Local, TipoOcorrencia, TipoVeiculo, 
                        Mercado, Cliente, CodigoRecebedor, Dt, PesoProgramado, PesoFaturado, Remessa1, Ordem1, 
                        Item1, SKUBloqueada1, TipoMaterial1, PesoNaoAtendido1, Remessa2, Ordem2, Item2, SKUBloqueada2, 
                        TipoMaterial2, PesoNaoAtendido2, Remessa3, Ordem3, Item3, SKUBloqueada3, TipoMaterial3, 
                        PesoNaoAtendido3, Remessa4, Ordem4, Item4, SKUBloqueada4, TipoMaterial4, PesoNaoAtendido4, 
                        Remessa5, Ordem5, Item5, SKUBloqueada5, TipoMaterial5, PesoNaoAtendido5, Turno, UsuarioSAP, 
                        DataProex, Comentario
                    ]
                    
                    # Filtrando apenas os valores não nulos
                    colunas_nao_nulas = [colunas[i] for i in range(len(valores)) if valores[i] is not None]
                    valores_nao_nulos = [valores[i] for i in range(len(valores)) if valores[i] is not None]

                    # Construindo a string de colunas dinâmica
                    colunas_sql = ', '.join(colunas_nao_nulas)
                    placeholders_sql = ', '.join(['%s'] * len(colunas_nao_nulas))

                    # Construindo a instrução SQL final
                    sql = f"INSERT INTO {table_name} ({colunas_sql}) VALUES ({placeholders_sql})"
                    
                    # Executando a inserção
                    cursor.execute(sql, valores_nao_nulos)
                    conexao.commit()

                    # Log de sucesso
                    print(f"Inserido registro id_Proex={id_Proex}")

                except mysql.connector.Error as err:
                    print(f"Erro ao inserir dados no MySQL: {err}")
                except Exception as e:
                    print(f"Erro inesperado: {e}")

        # Log de resumo da página
        print(f"Página {page}: Total de registros na página = {total_registros_pagina}, Novos registros adicionados = {novos_registros_pagina}")

        # Incrementa o número da página para buscar a próxima página
        page += 1

    else:
        print(f"Erro ao fazer a requisição para a API: {response.status_code}")
        more_pages = False

# Fechando a conexão com o banco de dados
cursor.close()
conexao.close()
