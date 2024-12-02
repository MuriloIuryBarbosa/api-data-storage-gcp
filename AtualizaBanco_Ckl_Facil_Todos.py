import subprocess
import os

# Diretório base onde os scripts estão localizados
base_dir = os.path.join(os.getcwd(), "Normal Scripts")

# Lista dos nomes dos scripts que você quer executar
script_names = [
    "Ckl_RelatorioDesvios_New.py",
    "Ckl_AuditoriaComportamental.py",
    "Ckl_AuditoriaProcessos.py",
    "Ckl_Ocorrencias_Acidentes.py",
    "Ckl_CartaoVerde.py",
    "Ckl_ExpurgosTMP.py",
    "Ckl_MapeamentoTombamentos.py",
    "Ckl_ControleDeHorasExtras.py",
    "Ckl_ParadaDeMaquinas.py",
    "Ckl_TemposCarregamentosBensDeConsumo.py",
    "Ckl_AvariaMovimentacao.py",
    "Ckl_RelatorioDeRefugo.py",
]

for script_name in script_names:
    script_path = os.path.join(base_dir, script_name)
    try:
        print(f"Executando {script_path}...")
        # Executa o script individualmente
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_path}: {e}")
