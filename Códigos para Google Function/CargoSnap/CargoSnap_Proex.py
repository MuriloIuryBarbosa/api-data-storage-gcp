import requests
import pymysql
from datetime import datetime, timedelta

    # Conectar ao banco de dados MySQL
    conexao = pymysql.connect(
        host=db_ip_public,
        user=db_user,
        password=db_password,
        db=db_name
    )
    cursor = conexao.cursor()

    # Inicializa o número da página
    page = 1
    max_pages = 5

    while page <= max_pages:
        # URL da API com o parâmetro de página atualizado
        url = f"https://api.cargosnap.com/api/v2/forms/1757?format=json&token=[bjJ2TGlrWExDZmVHcmtjQ2Q0cldYcHdYWENGbG84MDBfMzQ1]&limit=200&startdate=2024-01-01&enddate=2099-12-31&page={page}"

        # Fazendo a requisição GET para a API
        response = requests.get(url)

        # Verificando se a requisição foi bem sucedida
        if response.status_code == 200:
            # Convertendo a resposta para JSON
            data = response.json()

            # Se não houver mais dados, sair do loop
            if not data['data']:
                break

            # Iterar sobre os dados da API
            for item in data['data']:
                id_Proex = item['id']

                try:
                    # Inicialize todas as variáveis com None ou um valor padrão
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']

                    # Extraindo os dados do item
                    id_Formulario = item['form_id']
                    Formulario = item['form']['title']
                    Usuario = item['nick']

                    # Iterando sobre os campos no formulário
                    for campo in item['form']['fields']:
                        if campo['label'] == 'Unidade':
                            Unidade = campo['value']
                        elif campo['label'] == 'Local':
                            Local = campo['value']
                        elif campo['label'] == 'Tipo de Ocorrência':
                            TipoOcorrencia = campo['value']
                        elif campo['label'] == 'Tipo de Veículo':
                            TipoVeiculo = campo['value']
                        elif campo['label'] == 'Mercado':
                            Mercado = campo['value']
                        elif campo['label'] == 'Cliente':
                            Cliente = campo['value']
                        elif campo['label'] == 'Código do recebedor':
                            CodigoRecebedor = campo['value']
                        elif campo['label'] == 'DT:':
                            Dt = campo['value']
                        elif campo['label'] == 'Peso Programado:':
                            PesoProgramado = campo['value']
                        elif campo['label'] == 'Peso Faturado:':
                            PesoFaturado = campo['value']

                        elif campo['label'] == '1 - Remessa:':
                            Remessa1 = campo['value']
                        elif campo['label'] == '1 - Ordem de venda':
                            Ordem1 = campo['value']
                        elif campo['label'] == '1 - Item':
                            Item1 = campo['value']
                        elif campo['label'] == '1 - SKU bloqueada:':
                            SKUBloqueada1 = campo['value']
                        elif campo['label'] == '1 - Tipo Material:':
                            TipoMaterial1 = campo['value']
                        elif campo['label'] == '1 - Peso não atendido especifico:':
                            PesoNaoAtendido1 = campo['value']
                        
                        elif campo['label'] == '2 - Remessa:':
                            Remessa2 = campo['value']
                        elif campo['label'] == '2 - Ordem de venda':
                            Ordem2 = campo['value']
                        elif campo['label'] == '2 - Item':
                            Item2 = campo['value']
                        elif campo['label'] == '2 - SKU bloqueada:':
                            SKUBloqueada2 = campo['value']
                        elif campo['label'] == '2 - Tipo Material:':
                            TipoMaterial2 = campo['value']
                        elif campo['label'] == '2 - Peso não atendido especifico:':
                            PesoNaoAtendido2 = campo['value']

                        elif campo['label'] == '3 - Remessa:':
                            Remessa3 = campo['value']
                        elif campo['label'] == '3 - Ordem de venda':
                            Ordem3 = campo['value']
                        elif campo['label'] == '3 - Item':
                            Item3 = campo['value']
                        elif campo['label'] == '3 - SKU bloqueada:':
                            SKUBloqueada3 = campo['value']
                        elif campo['label'] == '3 - Tipo Material:':
                            TipoMaterial3 = campo['value']
                        elif campo['label'] == '3 - Peso não atendido especifico:':
                            PesoNaoAtendido3 = campo['value']

                        elif campo['label'] == '4 - Remessa:':
                            Remessa4 = campo['value']
                        elif campo['label'] == '4 - Ordem de venda':
                            Ordem4 = campo['value']
                        elif campo['label'] == '4 - Item':
                            Item4 = campo['value']
                        elif campo['label'] == '4 - SKU bloqueada:':
                            SKUBloqueada4 = campo['value']
                        elif campo['label'] == '4 - Tipo Material:':
                            TipoMaterial4 = campo['value']
                        elif campo['label'] == '4 - Peso não atendido especifico:':
                            PesoNaoAtendido4 = campo['value']

                        elif campo['label'] == '5 - Remessa:':
                            Remessa5 = campo['value']
                        elif campo['label'] == '5 - Ordem de venda':
                            Ordem5 = campo['value']
                        elif campo['label'] == '5 - Item':
                            Item5 = campo['value']
                        elif campo['label'] == '5 - SKU bloqueada:':
                            SKUBloqueada5 = campo['value']
                        elif campo['label'] == '5 - Tipo Material:':
                            TipoMaterial5 = campo['value']
                        elif campo['label'] == '5 - Peso não atendido especifico:':
                            PesoNaoAtendido5 = campo['value']
                            
                        elif campo['label'] == 'Turno':
                            Turno = campo['value']
                        elif campo['label'] == 'Emissor formulário (usuário SAP):':
                            UsuarioSAP = campo['value']
                        elif campo['label'] == 'Data Proex':
                            DataProex = campo['value']
                        elif campo['label'] == 'Comentários':
                            Comentario = campo['value']

                        # Após o loop, verifique se Ordem1 ainda é None e, se for, atribua um valor padrão
                        if Ordem1 is None:
                            Ordem1 = 0  # Ou outro valor padrão adequado
                        if Ordem2 is None:
                            Ordem2 = 0  # Ou outro valor padrão adequado

                        if 'Cliente' not in locals():
                            Cliente = None  # Ou outro valor padrão adequado
                        if 'CodigoRecebedor' not in locals():
                            CodigoRecebedor = None  # Ou outro valor padrão adequado
                        if 'Comentario' not in locals():
                            Comentario = None  # Ou outro valor padrão adequado
                        if 'Remessa1' not in locals():
                            Remessa1 = None  # Ou outro valor padrão adequado

                    # Inserindo os dados no banco de dados MySQL
                    sql = "INSERT INTO CargoSnap_Proex (id_Formulario, id_Proex, Usuario, Formulario) VALUES (%s, %s, %s, %s)"
                    val = (id_Formulario, id_Proex, Usuario, Formulario)
                    cursor.execute(sql, val)
                    conexao.commit()

                    # Exemplo de saída de progresso para cada registro
                    print(f"Registrando ID {id_Proex} da página {page}")
                except pymysql.MySQLError as err:
                    print(f"Erro ao inserir o ID {id_Proex}: {err}")
                    # Continue para o próximo ID
                    continue

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

# Função para converter data para o formato YYYY-MM-DD
def converter_data(data_str):
    try:
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None
