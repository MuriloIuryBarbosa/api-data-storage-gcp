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
token = os.getenv('Token_CargoSnap_Inspecao_Insetos')
table_name = os.getenv('Table_CargoSnap_Inspecao_Insetos')

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
    url = f'https://api.cargosnap.com/api/v2/forms/2489?format=json&token={token}&limit=200&startdate=2000-01-01&enddate=2099-12-31&page={page}'

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

        # Consultar os id_Inspecao  existentes no banco de dados
        cursor.execute(f"SELECT id_Inspecao  FROM {table_name}")
        resultados_existente = cursor.fetchall()
        ids_existente = {row[0] for row in resultados_existente}  # Conjunto de ids existentes

        # Iterar sobre os dados da API
        total_registros_pagina = len(data['data'])
        novos_registros_pagina = 0
        for item in data['data']:
            id_Inspecao  = item['id']
            if id_Inspecao  not in ids_existente:
                novos_registros_pagina += 1
                try:
                    # Inicialize todas as variáveis com None ou um valor padrão
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']
                    Data = None
                    Unidade = None
                    NomeConferente = None
                    TipoInspecao = None
                    Cliente = None
                    Rastreamento = None
                    TampoVivos = None
                    TampoMortos = None
                    ResmaVivos = None
                    ResmaMortos = None
                    PaleteRetrabalhado = None
                    PaletesReprovadosPorInsetos = None
                    PaletesRetrabalhados = None

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        label = campo['label']
                        value = campo['value']
                        if label == 'Unidade':
                            Unidade = value
                        elif label == 'Data':
                            Data = value                     
                        elif label == 'Nome do Conferente':
                            NomeConferente = value
                        elif label == 'Tipo da Inspeção':
                            TipoInspecao = value
                        elif label == 'Cliente':
                            Cliente = value
                        elif label == 'Rastreamento ':
                            Rastreamento = value
                        elif label == 'Tampo - Vivos ':
                            TampoVivos = value
                        elif label == 'Tampo - Mortos ':
                            TampoMortos = value
                        elif label == 'Resma Vivos ':
                            ResmaVivos = value
                        elif label == 'Resma Mortos ':
                            ResmaMortos = value
                        elif label == 'Palete Retrabalhados':
                            PaleteRetrabalhado = value
                        elif label == 'Palete Reprovados por Inseto':
                            PaletesReprovadosPorInsetos = value
                        elif label == 'Paletes Retrabalhados':
                            PaletesRetrabalhados = value

                    # Preparando os valores para a inserção
                    colunas = ['id_Formulario', 'Formulario', 'Data', 'id_Inspecao', 'Usuario', 'Unidade', 'NomeConferente', 'TipoInspecao', 'Cliente', 'Rastreamento', 'TampoVivos', 'TampoMortos', 'ResmaVivos', 'ResmaMortos', 'PaleteRetrabalhado', 'PaletesReprovadosPorInsetos', 'PaletesRetrabalhados']
                    valores = [id_Formulario, Formulario, Data, id_Inspecao, Usuario, Unidade, NomeConferente, TipoInspecao, Cliente, Rastreamento, TampoVivos, TampoMortos, ResmaVivos, ResmaMortos, PaleteRetrabalhado, PaletesReprovadosPorInsetos, PaletesRetrabalhados]
                    
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
                    print(f"Inserido registro id_Inspecao ={id_Inspecao }")

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
