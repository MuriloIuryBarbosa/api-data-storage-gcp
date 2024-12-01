import subprocess

# Lista dos caminhos dos scripts que você quer executar
scripts = [
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\CargoSnap_BabySitter1101.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\CargoSnap_BabySitter5400.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\CargoSnap_Proex.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\CargoSnap_Inspecao_Insetos.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\CargoSnap_Inspecao_Plataforma.py",
]

for script in scripts:
    try:
        print(f"Executando {script}...")
        # Executa o script individualmente
        subprocess.run(["python", script], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script}: {e}")
