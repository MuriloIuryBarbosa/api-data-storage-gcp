import subprocess

# Lista dos caminhos dos scripts que você quer executar
scripts = [
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_RelatorioDesvios_New.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_AuditoriaComportamental.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_AuditoriaProcessos.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_Ocorrencias_Acidentes.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_CartaoVerde.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_ExpurgosTMP.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_MapeamentoTombamentos.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_ControleDeHorasExtras.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_ParadaDeMaquinas.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_TemposCarregamentosBensDeConsumo.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_AvariaMovimentacao.py",
    r"C:\Users\Usuario\OneDrive - Suzano S A\Work\1 - Codigos\Banco-GCP\Normal Scripts\Ckl_RelatorioDeRefugo.py",
]

for script in scripts:
    try:
        print(f"Executando {script}...")
        # Executa o script individualmente
        subprocess.run(["python", script], check=True)
    except subprocess.CalledProcessError as e:
        
        print(f"Erro ao executar {script}: {e}")
