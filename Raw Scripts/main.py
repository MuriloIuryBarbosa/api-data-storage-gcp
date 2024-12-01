import subprocess
import datetime
import time
import sys
import os

def run_script(script_path):
    script_name = os.path.basename(script_path)
    print(f"\n{'='*40}\nExecutando {script_name}...\n{'='*40}")
    script_name_clean = script_name.replace('.py', '')
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")

    log_filename = f"C:\\Users\\Usuario\\OneDrive - Suzano S A\\Work\\Data Analysis\\Bases\\logs_apis\\{date_str}T{time_str} {script_name_clean}.txt"

    with open(log_filename, 'w') as log_file:
        log_file.write(f"Script: {script_name_clean}\n")
        log_file.write(f"Data de execução: {date_str}\n")
        log_file.write(f"Hora de inicio: {time_str}\n")
        log_file.write("=" * 40 + "\n")
        log_file.write(f"Executando \"{script_name_clean}\"...\n")
        log_file.write("=" * 40 + "\n\n")

        process = subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        while True:
            output = process.stdout.readline()
            if output:
                log_file.write(output)
                print(output.strip())  # Exibir no terminal opcionalmente
            error = process.stderr.readline()
            if error:
                log_file.write(error)
                print(error.strip(), file=sys.stderr)  # Exibir no terminal opcionalmente
            if output == '' and error == '' and process.poll() is not None:
                break

        process.stdout.close()
        process.stderr.close()
        process.wait()

        log_file.write("\n\n")
        log_file.write("=" * 40 + "\n")
        log_file.write(f"Hora de fim: {datetime.datetime.now().strftime('%H-%M-%S')}\n")
        log_file.write("=" * 40 + "\n")

    return process.returncode

def countdown(seconds):
    for i in range(seconds, 0, -1):
        minutes, sec = divmod(i, 60)
        time_format = f'{minutes:02d}:{sec:02d}'
        print(f"Próxima execução em: {time_format}", end='\r', flush=True)
        time.sleep(1)
    print()

if __name__ == "__main__":
    # Diretório atual
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Lista de scripts a serem executados
    scripts = [
        'Raw_Ckl_MapeamentoTombamentos.py',
        'Raw_Ckl_ParadaDeMaquinas.py',
        'Raw_Ckl_AuditoriaComportamental.py',
        'Raw_Ckl_AuditoriaProcessos.py',
        'Raw_Ckl_AvariaMovimentacao.py',
        'Raw_Ckl_CartaoVerde.py',
        'Raw_Ckl_ControleDeHorasExtras.py',
        'Raw_Ckl_RelatorioDesvios.py',
        'Raw_Ckl_TemposCarregamentosBensDeConsumo.py',
        'Raw_Ckl_RelatorioDeRefugo.py',
        'Raw_CargoSnap_Proex.py',
    #    'Raw_CargoSnap_BabySitter1101.py',
        'Raw_CargoSnap_BabySitter5400.py'
    ]

    # Construir caminhos completos para os scripts
    script_paths = [os.path.join(current_dir, script) for script in scripts]

    for i, script_path in enumerate(script_paths):
        returncode = run_script(script_path)
        script_name = os.path.basename(script_path)
        if returncode == 0:
            print(f"\n{'-'*20} {script_name} executado com sucesso {'-'*20}\n")
        else:
            print(f"\n{'-'*20} Erro ao executar {script_name} {'-'*20}\n")

        if i < len(script_paths) - 1:
            next_script_name = os.path.basename(script_paths[i + 1])
            print(f"\n{'='*40}\nPróximo script: {next_script_name}\n{'='*40}")
            countdown(10)  # 120 segundos = 2 minutos
    
    print("\nTodos os scripts foram executados.")
