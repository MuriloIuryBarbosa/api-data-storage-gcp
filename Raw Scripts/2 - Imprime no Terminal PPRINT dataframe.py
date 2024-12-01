import pandas as pd
import requests

headers = {
    'Authorization': 'Bearer FpcpVRXdvIlV0UAeTmJDWLcc1KprazYLbcLrWhSFgZ5PfLAeixyzThvooiyBjjgiao05luvTUep2viXDqouigM2ZpXmD0OYeo5RkxKDAWVX1NteFHeJWBYacjHa1Zjlv'
}

# Lista de IDs de avaliações
ids = [
    106647472]

# Lista para armazenar os dados de todas as avaliações

dados_avaliacoes = []

# Itera sobre os IDs e faz a requisição para cada um
for id_avaliacao in ids:
    url = f"https://integration.checklistfacil.com.br/v2/evaluations/{id_avaliacao}"
    requisicao = requests.get(url, headers=headers, verify=False)
    if requisicao.status_code == 200:
        informacoes = requisicao.json()
        dados_avaliacoes.append(informacoes)
    else:
        print(f"Erro ao obter dados da avaliação {id_avaliacao}: {requisicao.status_code}")

# Função para tratar dados aninhados e normalizar em um dicionário
def flatten_dict(d, parent_key='', sep='_'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):  # Verifica se a lista não está vazia
                for i, item in enumerate(v):
                    items.update(flatten_dict(item, f"{new_key}[{i}]", sep))
            else:
                items[new_key] = v
        else:
            items[new_key] = v
    return items

# Normaliza os dados de todas as avaliações em uma lista de dicionários achatados
dados_achatados = []
for avaliacao in dados_avaliacoes:
    flat_data = flatten_dict(avaliacao)
    dados_achatados.append(flat_data)

# Cria o DataFrame a partir da lista de dicionários achatados
df = pd.DataFrame(dados_achatados)

# Salva o DataFrame em um arquivo Excel
caminho_arquivo = r"C:\Users\Usuario\Desktop\dados_avaliacoes_api.xlsx"
df.to_excel(caminho_arquivo, index=False)

print(f"DataFrame salvo em: {caminho_arquivo}")
