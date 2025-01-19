import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 URL Base da API
API_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/serviceLocal.xml"

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Locais que precisamos buscar
desired_locations = ["Suzano Fábrica B", "São José dos Pinhais", "CD Campinas", "Limeira"]
locations_data = []

# 🔹 Função para buscar os IDs filtrando pela descrição
def get_location_id_by_description(description):
    print(f"🔍 Buscando ID para: {description}...")

    params = {"description": description}
    response = requests.get(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        root = ET.fromstring(response.text)
        entries = root.find("entries")

        if entries is not None and len(entries.findall("entry")) > 0:
            entry = entries.find("entry")  # Pegamos o primeiro resultado
            location_id = entry.get("id")
            print(f"✅ Encontrado! {description} → ID: {location_id}")
            return {"description": description, "id": location_id}
        else:
            print(f"⚠ Nenhum resultado encontrado para {description}.")
            return {"description": description, "id": "Não encontrado"}

    else:
        print(f"❌ Erro ao buscar {description}: {response.status_code}")
        return {"description": description, "id": "Erro na API"}

# 🔹 Buscar IDs para cada local desejado
for location in desired_locations:
    result = get_location_id_by_description(location)
    locations_data.append(result)

# 🔹 Criar DataFrame com os IDs encontrados
df = pd.DataFrame(locations_data)

# 🔹 Exibir DataFrame
# import ace_tools as tools
# tools.display_dataframe_to_user(name="IDs dos Locais", dataframe=df)

# Alternative: Display DataFrame using built-in Pandas method
print("\n🔹 IDs dos Locais DataFrame:")
print(df)

# 🔹 Exibir os IDs no terminal
print("\n🔹 IDs dos locais encontrados:")
for loc in locations_data:
    print(f"{loc['description']} → ID: {loc['id']}")
