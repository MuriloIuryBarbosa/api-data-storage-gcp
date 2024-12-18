import subprocess
import os

# Diretório base onde os scripts estão localizados
base_dir = os.path.join(os.getcwd(), "Normal Scripts")

# Lista dos nomes dos scripts que você quer executar
script_names = [
    "CargoSnap_BabySitter1101.py",
    "CargoSnap_BabySitter5400.py",
    "CargoSnap_Proex.py",
    "CargoSnap_Inspecao_Insetos.py",
    "CargoSnap_Inspecao_Plataforma.py",
] 

for script_name in script_names:
    script_path = os.path.join(base_dir, script_name)
    try:
        print(f"Executando {script_path}...")
        # Executa o script individualmente
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_path}: {e}")
