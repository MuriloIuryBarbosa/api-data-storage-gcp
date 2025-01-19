import os
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 Configurações
EXPORT_LAYOUT_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/exportLayout.xml"

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Função para buscar os modelos de exportação disponíveis
def get_export_layouts():
    print("🔍 Buscando modelos de exportação disponíveis...")

    response = requests.get(EXPORT_LAYOUT_URL, headers=headers)

    if response.status_code == 200:
        root = ET.fromstring(response.text)
        entries = root.find("entries")

        layouts = []
        if entries is not None:
            for entry in entries.findall("entry"):
                layout_id = entry.get("id")
                link = entry.get("link")
                layouts.append({"layout_id": layout_id, "link": link})
                print(f"✅ Modelo de exportação encontrado: ID {layout_id} | Link: {link}")

        print(f"📊 Total de {len(layouts)} modelos de exportação disponíveis.")
        return layouts

    else:
        print(f"❌ Erro ao buscar modelos de exportação: {response.status_code}")
        return []

# 🔹 Executar busca de modelos de exportação
export_layouts = get_export_layouts()
