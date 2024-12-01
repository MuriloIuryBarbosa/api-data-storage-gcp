import subprocess
import time
import sys

def run_script(script_name):
    print(f"\n{'='*40}\nExecutando {script_name}...\n{'='*40}")
    process = subprocess.Popen(['python', script_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, text=True)

    while True:
        output = process.stdout.readline()
        if output:
            print(output.strip(), flush=True)
        error = process.stderr.readline()
        if error:
            print(error.strip(), file=sys.stderr, flush=True)
        if output == '' and error == '' and process.poll() is not None:
            break

    return process.returncode

def countdown(seconds):
    for i in range(seconds, 0, -1):
        minutes, sec = divmod(i, 60)
        time_format = f'{minutes:02d}:{sec:02d}'
        print(f"Próxima execução em: {time_format}", end='\r', flush=True)
        time.sleep(1)
    print()

if __name__ == "__main__":
    # Lista de scripts a serem executados
    scripts = [
        'Ckl_MapeamentoTombamentos.py',
        'Ckl_ParadaDeMaquinas.py',
        'Ckl_AuditoriaComportamental.py',
        'Ckl_AuditoriaProcessos.py',
        'Ckl_AvariaMovimentacao.py',
        'Ckl_CartaoVerde.py',
        'Ckl_ControleDeHorasExtras.py',
        'Ckl_RelatorioDesvios.py',
        'Ckl_TemposCarregamentosBensDeConsumo.py',
        'CargoSnap_BabySitter1101 (código antigo funcionando).py',
        'CargoSnap_BabySitter5400 (código antigo funcionando).py',
        'CargoSnap_Proex (código antigo funcionando).py'
    ]

    for i, script in enumerate(scripts):
        returncode = run_script(script)
        if returncode == 0:
            print(f"\n{'-'*20} {script} executado com sucesso {'-'*20}\n")
        else:
            print(f"\n{'-'*20} Erro ao executar {script} {'-'*20}\n")

        if i < len(scripts) - 1:
            print(f"\n{'='*40}\nPróximo script: {scripts[i + 1]}\n{'='*40}")
            countdown(10)  # 120 segundos = 2 minutos
    
    print("\nTodos os scripts foram executados.")
