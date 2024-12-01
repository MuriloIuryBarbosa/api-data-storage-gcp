import subprocess
import datetime
import time
import sys
import os

def run_script(script_name):
    print(f"\n{'='*40}\nExecutando {script_name}...\n{'='*40}")
    script_name_clean = script_name.replace('.py', '')
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")

    # Diretório onde este script está localizado
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Caminho completo para o script a ser executado
    script_path = os.path.join(current_dir, script_name)

    # Corrigir o caminho para o diretório de log
    log_dir = os.path.join(current_dir, "LOGS_APIs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)  # Cria o diretório se não existir

    log_filename = os.path.join(log_dir, f"{date_str}T{time_str} {script_name_clean}.txt")

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
    # Lista de scripts a serem executados
    scripts = [
        'Ckl_MapeamentoTombamentos.py',
        'Ckl_ParadaDeMaquinas.py',
        'Ckl_AuditoriaComportamental.py',
        'Ckl_AuditoriaProcessos.py',
        'Ckl_AvariaMovimentacao.py',
        'Ckl_CartaoVerde.py',
        'Ckl_ControleDeHorasExtras.py',
        'Ckl_ControleDeHorasExtras_Antigo.py',
        'Ckl_RelatorioDesvios_New.py',
        'Ckl_TemposCarregamentosBensDeConsumo.py',
        'Ckl_RelatorioDeRefugo.py',
        'Ckl_ManutencaoPredial.py'
        #'CargoSnap_BabySitter1101 (código antigo funcionando).py',
        #'CargoSnap_BabySitter5400 (código antigo funcionando).py',
        #'CargoSnap_Proex (código antigo funcionando).py'
    ]

    for i, script in enumerate(scripts):
        returncode = run_script(script)
        if returncode == 0:
            print(f"\n{'-'*20} {script} executado com sucesso {'-'*20}\n")
        else:
            print(f"\n{'-'*20} Erro ao executar {script} {'-'*20}\n")

        if i < len(scripts) - 1:
            print(f"\n{'='*40}\nPróximo script: {scripts[i + 1]}\n{'='*40}")
            countdown(80)  # 120 segundos = 2 minutos
    
    print("\nTodos os scripts foram executados.")
