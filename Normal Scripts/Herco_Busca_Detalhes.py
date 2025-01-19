import os
import requests
import xml.etree.ElementTree as ET
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 🔹 Carregar variáveis do .env
load_dotenv()
API_TOKEN = os.getenv("Token_Herco")
DB_HOST = os.getenv("MYSQL_HOST")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE")

# 🔹 Configurações
START_DATE = datetime.strptime("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
END_DATE = datetime.strptime("2025-01-18 23:59:59", "%Y-%m-%d %H:%M:%S")
TIME_STEP_MINUTES = 180  # 🔹 Consulta a cada 5 minutos
BATCH_PROCESS_HOURS = 1  # 🔹 Processar detalhes a cada 1 hora acumulada

HISTORY_API_URL = f"https://api.umov.me/CenterWeb/api/{API_TOKEN}/activityHistory.xml"

# 🔹 Headers da requisição
headers = {
    "token": API_TOKEN,
    "Cache-Control": "no-cache",
    "User-Agent": "Python-Requests/2.26.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 🔹 Conectar ao MySQL
def conectar_bd():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# 🔹 Buscar IDs das vistorias dentro de um intervalo
def get_activity_ids(start_time, end_time):
    print(f"\n🔍 Buscando IDs de históricos entre {start_time} e {end_time}...")

    params = {
        "initialStartTimeOnSystem": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endStartTimeOnSystem": end_time.strftime("%Y-%m-%d %H:%M:%S")  # Corrigindo o parâmetro
    }

    response = requests.get(HISTORY_API_URL, headers=headers, params=params)

    if response.status_code != 200:
        print(f"❌ Erro ao buscar atividades: {response.status_code} - {response.text}")
        return []

    root = ET.fromstring(response.text)
    entries = root.find("entries")

    if entries is None:
        print(f"⚠ Nenhum dado encontrado. Resposta da API:\n{response.text}")
        return []

    return [
        f"https://api.umov.me/CenterWeb/api/{API_TOKEN}{entry.get('link')}"
        for entry in entries.findall("entry") if entry.get("link")
    ]

# 🔹 Buscar detalhes de uma vistoria utilizando os links corretos
def get_activity_details(activity_url):
    response = requests.get(activity_url, headers=headers)

    if response.status_code == 200:
        print(response.text)  # 🔍 Exibir XML recebido para debug
        root = ET.fromstring(response.text)

        activity_data = {
            "id_vistoria": root.findtext("id", default=None),
            "start_time": root.findtext("startTimeOnSystem", default=None),
            "finish_time": root.findtext("finishTimeOnSystem", default=None),
            "status": root.findtext("status", default=None),
            "execution_export_status": root.findtext("executionExportStatus", default=None),
            "bi_export_status": root.findtext("biExportStatus", default=None),
            "agent_id": root.findtext("schedule/agent/id", default=None),
            "agent_name": root.findtext("schedule/agent/name", default=None),
            "service_local": root.findtext("activity/description", default=None),
            "schedule_id": root.findtext("schedule/id", default=None),
            "schedule_date": root.findtext("schedule/date", default=None),
            "schedule_hour": root.findtext("schedule/hour", default=None),
        }

        print(f"✅ Dados extraídos: {activity_data}")  # 🔍 Exibir os dados extraídos
        return activity_data

    else:
        print(f"❌ Erro ao buscar detalhes: {response.status_code} - {response.text}")
        return None


# 🔹 Inserir DataFrame no MySQL em lote
def inserir_no_banco(df):
    if df.empty:
        return

    conn = conectar_bd()
    cursor = conn.cursor()

    query = """
        INSERT INTO db_guiza.Herco_Vistorias (
            id_vistoria, start_time, finish_time, status, execution_export_status, bi_export_status,
            agent_id, agent_name, service_local, schedule_id, schedule_date, schedule_hour
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            start_time=VALUES(start_time),
            finish_time=VALUES(finish_time),
            status=VALUES(status),
            execution_export_status=VALUES(execution_export_status),
            bi_export_status=VALUES(bi_export_status),
            agent_id=VALUES(agent_id),
            agent_name=VALUES(agent_name),
            service_local=VALUES(service_local),
            schedule_id=VALUES(schedule_id),
            schedule_date=VALUES(schedule_date),
            schedule_hour=VALUES(schedule_hour);
    """

    valores = df.values.tolist()

    try:
        cursor.executemany(query, valores)
        conn.commit()
        print(f"✅ {len(df)} vistorias registradas no banco.")
    except mysql.connector.Error as err:
        print(f"❌ Erro ao inserir no banco: {err}")
    finally:
        cursor.close()
        conn.close()

# 🔹 Fluxo principal: consultar IDs de 5 em 5 minutos e processar a cada 1 hora acumulada
def executar_extracao():
    current_time = START_DATE
    total_vistorias = []
    
    while current_time <= END_DATE:
        next_time = current_time + timedelta(minutes=TIME_STEP_MINUTES)

        # 🔹 Buscar IDs das vistorias no intervalo de 5 minutos
        activity_ids = get_activity_ids(current_time, next_time)
        total_vistorias.extend(activity_ids)

        print(f"📊 Total acumulado de IDs: {len(total_vistorias)}")

        # 🔹 Se atingirmos 1 hora de registros acumulados, buscar detalhes e salvar no banco
        if len(total_vistorias) > 0 and (current_time - START_DATE).seconds % 3600 == 0:
            print("\n🚀 Processando e registrando os detalhes das vistorias...\n")

            df_list = []
            for url in total_vistorias:
                data = get_activity_details(url)
                if data:
                    df_list.append(pd.DataFrame([data]))

            if df_list:
                inserir_no_banco(pd.concat(df_list, ignore_index=True))

            # 🔹 Limpa a memória para manter boa performance
            total_vistorias.clear()
            df_list.clear()

            print("\n✅ Registros salvos, continuando a consulta...\n")

        current_time = next_time

    # 🔹 Salvar registros restantes ao final do período
    if total_vistorias:
        print("\n🚀 Processando registros finais...\n")
        df_list = []
        for url in total_vistorias:
            data = get_activity_details(url)
            if data:
                df_list.append(pd.DataFrame([data]))

        if df_list:
            inserir_no_banco(pd.concat(df_list, ignore_index=True))

    print("\n🚀 Processo finalizado! Todas as vistorias foram registradas no banco de dados.")

# 🔹 Rodar extração
executar_extracao()
