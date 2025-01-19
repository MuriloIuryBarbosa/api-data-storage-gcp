import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 URLs da API
API_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/serviceLocal.xml"
API_DETAIL_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/serviceLocal/{{}}.xml"

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Função para buscar IDs paginados por data
def get_location_ids():
    location_ids = set()  # Usamos um set para evitar duplicatas
    batch_size = 30  # Dias por busca (ajustável)
    end_date = datetime.now()  # Data mais recente (hoje)
    start_date = end_date - timedelta(days=batch_size)  # Intervalo inicial

    print("🔄 Iniciando busca pelos locais de atendimento com paginação por data...")

    while True:
        print(f"📅 Buscando locais entre {start_date} e {end_date}...")
        params = {
            "initialInsertDateTime": start_date.strftime("%Y-%m-%d 00:00:00"),
            "finalInsertDateTime": end_date.strftime("%Y-%m-%d 23:59:59")
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            entries = root.find("entries")

            if entries is not None:
                new_count = 0
                for entry in entries.findall("entry"):
                    location_id = entry.get("id")
                    if location_id not in location_ids:
                        location_ids.add(location_id)
                        new_count += 1
                        print(f"🔹 Novo local encontrado: ID {location_id}")

                print(f"✅ {new_count} novos locais adicionados. Total: {len(location_ids)}")

            # Se nenhum novo ID foi encontrado, paramos
            if new_count == 0:
                print("🚫 Nenhum novo local encontrado. Finalizando busca.")
                break

            # Retrocedemos mais um período para buscar locais mais antigos
            end_date = start_date
            start_date = end_date - timedelta(days=batch_size)

        else:
            print(f"❌ Erro na requisição de IDs: {response.status_code}")
            break

    return list(location_ids)

# 🔹 Função para buscar detalhes de cada local
def get_location_details(location_ids):
    locations_data = []
    total_ids = len(location_ids)

    print("🔄 Iniciando coleta dos detalhes dos locais...")
    for index, location_id in enumerate(location_ids, start=1):
        print(f"🔍 Buscando detalhes do local {index}/{total_ids} (ID: {location_id})...")

        url = API_DETAIL_URL.format(location_id)
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            location_data = {
                "id": root.findtext("id"),
                "description": root.findtext("description"),
                "country": root.findtext("country"),
                "state": root.findtext("state"),
                "city": root.findtext("city"),
                "street": root.findtext("street"),
                "zipCode": root.findtext("zipCode"),
                "geoCoordinate": root.findtext("geoCoordinate"),
                "active": root.findtext("active")
            }
            locations_data.append(location_data)
            print(f"✅ Detalhes coletados: {location_data['description']} (ID: {location_data['id']})")
        else:
            print(f"⚠ Erro ao buscar detalhes do ID {location_id}: {response.status_code}")

        time.sleep(0.5)  # Pequeno delay para evitar bloqueio da API

    print(f"✅ Coleta finalizada! Total de locais detalhados: {len(locations_data)}")
    return locations_data

# 🔹 Executando o processo completo
print("🚀 Iniciando processo de coleta de dados da API Herco...")
location_ids = get_location_ids()
if location_ids:
    print("\n✅ IDs coletados! Agora buscando detalhes...\n")
    locations_data = get_location_details(location_ids)

    # 🔹 Criar DataFrame com os dados coletados
    df = pd.DataFrame(locations_data)

    # 🔹 Exibir DataFrame
    df.to_csv("locais_de_atendimento.csv", index=False)
    print("✅ Dados salvos em 'locais_de_atendimento.csv'")
else:
    print("❌ Nenhum ID foi coletado. O processo será encerrado.")
import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 URLs da API
API_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/serviceLocal.xml"
API_DETAIL_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/serviceLocal/{{}}.xml"

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Função para buscar IDs paginados por data
def get_location_ids():
    location_ids = set()  # Usamos um set para evitar duplicatas
    batch_size = 30  # Dias por busca (ajustável)
    end_date = datetime.now()  # Data mais recente (hoje)
    start_date = end_date - timedelta(days=batch_size)  # Intervalo inicial

    print("🔄 Iniciando busca pelos locais de atendimento com paginação por data...")

    while True:
        print(f"📅 Buscando locais entre {start_date} e {end_date}...")
        params = {
            "initialInsertDateTime": start_date.strftime("%Y-%m-%d 00:00:00"),
            "finalInsertDateTime": end_date.strftime("%Y-%m-%d 23:59:59")
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            entries = root.find("entries")

            if entries is not None:
                new_count = 0
                for entry in entries.findall("entry"):
                    location_id = entry.get("id")
                    if location_id not in location_ids:
                        location_ids.add(location_id)
                        new_count += 1
                        print(f"🔹 Novo local encontrado: ID {location_id}")

                print(f"✅ {new_count} novos locais adicionados. Total: {len(location_ids)}")

            # Se nenhum novo ID foi encontrado, paramos
            if new_count == 0:
                print("🚫 Nenhum novo local encontrado. Finalizando busca.")
                break

            # Retrocedemos mais um período para buscar locais mais antigos
            end_date = start_date
            start_date = end_date - timedelta(days=batch_size)

        else:
            print(f"❌ Erro na requisição de IDs: {response.status_code}")
            break

    return list(location_ids)

# 🔹 Função para buscar detalhes de cada local
def get_location_details(location_ids):
    locations_data = []
    total_ids = len(location_ids)

    print("🔄 Iniciando coleta dos detalhes dos locais...")
    for index, location_id in enumerate(location_ids, start=1):
        print(f"🔍 Buscando detalhes do local {index}/{total_ids} (ID: {location_id})...")

        url = API_DETAIL_URL.format(location_id)
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            location_data = {
                "id": root.findtext("id"),
                "description": root.findtext("description"),
                "country": root.findtext("country"),
                "state": root.findtext("state"),
                "city": root.findtext("city"),
                "street": root.findtext("street"),
                "zipCode": root.findtext("zipCode"),
                "geoCoordinate": root.findtext("geoCoordinate"),
                "active": root.findtext("active")
            }
            locations_data.append(location_data)
            print(f"✅ Detalhes coletados: {location_data['description']} (ID: {location_data['id']})")
        else:
            print(f"⚠ Erro ao buscar detalhes do ID {location_id}: {response.status_code}")

        time.sleep(0.5)  # Pequeno delay para evitar bloqueio da API

    print(f"✅ Coleta finalizada! Total de locais detalhados: {len(locations_data)}")
    return locations_data

# 🔹 Executando o processo completo
print("🚀 Iniciando processo de coleta de dados da API Herco...")
location_ids = get_location_ids()
if location_ids:
    print("\n✅ IDs coletados! Agora buscando detalhes...\n")
    locations_data = get_location_details(location_ids)

    # 🔹 Criar DataFrame com os dados coletados
    df = pd.DataFrame(locations_data)

    # 🔹 Exibir DataFrame
    print(df)
else:
    print("❌ Nenhum ID foi coletado. O processo será encerrado.")
