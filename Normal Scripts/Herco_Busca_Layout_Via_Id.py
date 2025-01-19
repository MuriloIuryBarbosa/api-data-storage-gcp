import os
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")

# 🔹 Configurações
BASE_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/exportLayout.xml"
DESCRIPTIONS = [
    "Check_List__Campinas",
    "Check_List__Limeira",
    "Check_List__Pinhais",
    "Check_List__Suzano"
]

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Função para buscar os IDs dos modelos de exportação com base na nova descrição
def get_export_layout_id(description):
    print(f"\n🔍 Buscando ID do modelo de exportação para '{description}'...")

    response = requests.get(f"{BASE_URL}?description={description}", headers=headers)

    if response.status_code == 200:
        root = ET.fromstring(response.text)
        entries = root.find("entries")

        if entries is not None and len(entries.findall("entry")) > 0:
            for entry in entries.findall("entry"):
                layout_id = entry.get("id")
                link = entry.get("link")
                print(f"✅ Modelo encontrado: {description} → ID: {layout_id} | Link: {link}")
                return layout_id
        else:
            print(f"⚠️ Nenhum modelo encontrado para '{description}'.")

    else:
        print(f"❌ Erro ao buscar modelo '{description}': {response.status_code}")

    return None

# 🔹 Buscar os IDs dos modelos desejados
export_layouts = {desc: get_export_layout_id(desc) for desc in DESCRIPTIONS}

# 🔹 Exibir resultados finais
print("\n📊 IDs dos modelos de exportação encontrados:")
for desc, layout_id in export_layouts.items():
    if layout_id:
        print(f"🔹 {desc}: ID {layout_id}")
    else:
        print(f"❌ {desc}: Não encontrado.")

