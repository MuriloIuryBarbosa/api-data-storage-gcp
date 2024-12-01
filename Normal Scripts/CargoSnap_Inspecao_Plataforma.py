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
token = os.getenv('Token_CargoSnap_Inspecao_Plataforma')
table_name = os.getenv('Table_CargoSnap_Inspecao_Plataforma')

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
    url = f'https://api.cargosnap.com/api/v2/forms/5307?format=json&token={token}&limit=200&startdate=2000-01-01&enddate=2099-12-31&page={page}'

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

        # Consultar os id_Inspecao existentes no banco de dados
        cursor.execute(f"SELECT id_Inspecao FROM {table_name}")
        resultados_existente = cursor.fetchall()
        ids_existente = {row[0] for row in resultados_existente}  # Conjunto de ids existentes

        # Iterar sobre os dados da API
        total_registros_pagina = len(data['data'])
        novos_registros_pagina = 0
        for item in data['data']:
            id_Inspecao = item['id']
            if id_Inspecao not in ids_existente:
                novos_registros_pagina += 1
                try:
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Data = item['created_at']

                    # Convertendo a data para o formato adequado para o MySQL
                    Data = datetime.strptime(Data, '%Y-%m-%dT%H:%M:%S.%fZ').date()
                    Usuario = item['nick']

                    # Inicializando outras variáveis
                    QualPeso = None
                    MotivoAvaria = None
                    Cliente = None
                    MaterialRetiradoDaPlataforma = None
                    InspetorResponsavel = None
                    OperadorResponsavel = None

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        label = campo['label']
                        value = campo['value']
                        if label == 'Qual o peso ?':
                            QualPeso = value                    
                        elif label == 'Qual o Motivo da Avaria Recusada ?':
                            MotivoAvaria = value
                        elif label == 'Qual o Cliente ?':
                            Cliente = value
                        elif label == 'Material foi retirado da plataforma ?':
                            MaterialRetiradoDaPlataforma = value
                        elif label == 'Inspetor Responsável ?':
                            InspetorResponsavel = value
                        elif label == 'Qual operador está carregando ?':
                            OperadorResponsavel = value

                    # Preparando os valores para a inserção
                    colunas = ['id_Formulario', 'id_Inspecao', 'Data', 'Usuario', 'Formulario', 'QualPeso', 'MotivoAvaria', 'Cliente', 'MaterialRetiradoDaPlataforma', 'InspetorResponsavel', 'OperadorResponsavel']
                    valores = [id_Formulario, id_Inspecao, Data, Usuario, Formulario, QualPeso, MotivoAvaria, Cliente, MaterialRetiradoDaPlataforma, InspetorResponsavel, OperadorResponsavel]
                    
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
                    print(f"Inserido registro id_Inspecao = {id_Inspecao}")

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
