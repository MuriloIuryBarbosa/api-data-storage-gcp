import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 Configurações
LOCATION_ID = "69187752"  # ID do CD Campinas
SEARCH_DATE = "2025-01-17"  # Data fixa para consulta
API_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/schedule.xml"
PAGE_SIZE = 20  # Número de registros retornados por requisição

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Função para buscar todas as tarefas com paginação
def get_all_schedules():
    print(f"🔍 Iniciando busca paginada para o CD Campinas (ID {LOCATION_ID}) na data {SEARCH_DATE}...")

    schedules = []
    offset = 0

    while True:
        print(f"📥 Buscando tarefas a partir do offset {offset}...")

        params = {
            "serviceLocal": LOCATION_ID,
            "date": SEARCH_DATE,
            "offset": offset
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            entries = root.find("entries")

            if entries is None or len(entries.findall("entry")) == 0:
                print("🚫 Nenhuma nova tarefa encontrada, encerrando paginação.")
                break  # Sai do loop quando não houver mais tarefas

            for entry in entries.findall("entry"):
                schedule_id = entry.get("id")
                link = entry.get("link")
                schedules.append({"schedule_id": schedule_id, "link": link})
                print(f"✅ Tarefa encontrada: ID {schedule_id} | Link: {link}")

            offset += PAGE_SIZE  # Avança para a próxima página de resultados

        else:
            print(f"❌ Erro ao buscar tarefas: {response.status_code}")
            break

    print(f"📊 Total de {len(schedules)} tarefas coletadas.")
    return schedules

# 🔹 Executar busca com paginação
schedules = get_all_schedules()

# 🔹 Criar DataFrame com os dados coletados
df = pd.DataFrame(schedules)

# 🔹 Salvar em CSV
if not df.empty:
    df.to_csv("Tarefas_do_CD_Campinas_17_01_COMPLETO.csv", index=False)
    print("📂 DataFrame salvo como 'Tarefas_do_CD_Campinas_17_01_COMPLETO.csv'.")

    # Exibir os dados no terminal
    print("\n🔹 Primeiras tarefas encontradas:")
    print(df.head())  # Mostra as primeiras linhas
else:
    print("❌ Nenhum dado encontrado, nada foi salvo.")
