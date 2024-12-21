import requests
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import warnings

# Suprimir avisos de InsecureRequestWarning
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar as variáveis de ambiente
token = os.getenv('Token_CargoSnap_BabySitter1101')
table_name = os.getenv('Table_CargoSnap_BabySitter1101_2')

# Inicializa o número da página
page = 1
more_pages = True

# Lista para acumular todos os registros
all_data = []

# Loop enquanto houver mais páginas
while more_pages:
    # URL da API com o parâmetro de página atualizado
    url = f'https://api.cargosnap.com/api/v2/forms/1756?format=json&token=[{token}]&limit=200&startdate=2024-11-01&enddate=2099-12-31&page={page}'

    # Fazendo a requisição GET para a API
    response = requests.get(url, verify=False)

    # Verificando se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Convertendo a resposta para JSON
        data = response.json()

        # Se não houver mais dados, sair do loop
        if not data['data']:
            more_pages = False
            break

        # Iterar sobre os dados da API
        total_registros_pagina = len(data['data'])
        novos_registros_pagina = 0
        for item in data['data']:
            id_BabySitter = item['id']
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

                # Adicionar o registro à lista all_data
                all_data.append({
                    "id_BabySitter": id_BabySitter,
                    "id_Formulario": id_Formulario,
                    "Usuario": Usuario,
                    "Formulario": Formulario,
                    "Dt": Dt,
                    "DataInicioBaby": DataInicioBaby,
                    "DataFimBaby": DataFimBaby,
                    "DuplaColeta": DuplaColeta,
                    "Unidade": Unidade,
                    "Transportadora": Transportadora,
                    "NomeTransportadora": NomeTransportadora,
                    "PlacaCarreta": PlacaCarreta,
                    "LocalCarregamento": LocalCarregamento,
                    "Observacoes": Observacoes,
                    "Mercado": Mercado,
                    "TipoMaterial": TipoMaterial,
                    "AvariasNoCarregamento": AvariasNoCarregamento,
                    "NumeroConteiner": NumeroConteiner,
                    "NomeCliente": NomeCliente,
                    "FardoDesalinhado": FardoDesalinhado,
                    "MaterialEmContatoComAssoalho": MaterialEmContatoComAssoalho,
                    "InsetosOuSujeiraNoVeiculo": InsetosOuSujeiraNoVeiculo,
                    "OdoresMofoOuUmidadeNoveiculo": OdoresMofoOuUmidadeNoveiculo,
                    "OutrasMercadoriasCarregadasNoVeiculo": OutrasMercadoriasCarregadasNoVeiculo,
                    "CarroceriaEmBomEstado": CarroceriaEmBomEstado,
                    "VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade": VeiculoOuProdutosTemPoeiraOuFarpasOuSujidade,
                    "LonaEstaEmBomEstado": LonaEstaEmBomEstado,
                    "NomeOperador": NomeOperador,
                    "NomeConferente": NomeConferente,
                    "ProblemaNoStrecth": ProblemaNoStrecth,
                    "PaleteQuebrado": PaleteQuebrado,
                    "ProdutoComMolhadura": ProdutoComMolhadura,
                    "PresencaDeCorpoEstranho": PresencaDeCorpoEstranho,
                    "PresencaDeInsetos": PresencaDeInsetos,
                    "InstrucoesExpedicao": InstrucoesExpedicao,
                    "QtdPacotesCDRSuzano": QtdPacotesCDRSuzano,
                    "QtdVolumesFTMFOB": QtdVolumesFTMFOB
                })

                print(f"Registrando ID {id_BabySitter} de {total_registros_pagina} da página {page}")
            except Exception as err:
                print(f"Erro ao processar o ID {id_BabySitter}: {err}")
                continue

        # Incrementa o número da página para a próxima iteração
        page += 1

    else:
        print(f"A requisição falhou com o status code: {response.status_code}")
        break

# Criando o DataFrame com todos os dados
df = pd.DataFrame(all_data)

df.to_gbq('gold_log_operacoes_logisticas.cargosnap_babysitter1101', project_id='sz-00046-ws', if_exists='replace')

# Exibe o DataFrame completo
print(df)
print('\nInserção concluída com sucesso!')
