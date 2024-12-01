import tkinter as tk
from tkinter import scrolledtext, Checkbutton, IntVar
from subprocess import Popen, PIPE, STDOUT
import os
import threading  # Importa o módulo threading
import queue  # Importa o módulo queue

# Obtém o diretório atual do script
current_dir = os.path.dirname(__file__)

# Cria uma fila para comunicação entre threads
output_queue = queue.Queue()

# Lista de scripts disponíveis para execução com caminhos relativos
scripts = {
    "Script 1": os.path.join(current_dir, "Ckl_ParadaDeMaquinas.py"),
    "Script 2": os.path.join(current_dir, "Ckl_MapeamentoTombamentos.py"),
    # Adicione mais scripts conforme necessário
}

# Função modificada para ser compatível com threading
def execute_script(script):
    process = Popen(["python", scripts[script]], stdout=PIPE, stderr=STDOUT, bufsize=1, text=True, universal_newlines=True)
    for line in process.stdout:
        output_queue.put(line)  # Coloca a linha na fila
    process.stdout.close()
    process.wait()

def update_output():
    while not output_queue.empty():
        line = output_queue.get()
        output_text.insert(tk.END, line)
        output_text.yview(tk.END)
    root.after(100, update_output)  # Agenda a próxima chamada para daqui a 100ms

def execute_scripts():
    output_text.delete(1.0, tk.END)  # Limpa a saída anterior
    selected_scripts = [script for script, var in checkboxes.items() if var.get() == 1]
    for script in selected_scripts:
        thread = threading.Thread(target=execute_script, args=(script,))
        thread.start()

# Configuração da janela principal
root = tk.Tk()
root.title("Executor de Scripts")
root.geometry("600x400")

# Checkbox para cada script
checkboxes = {}
for script in scripts:
    var = IntVar()
    cb = Checkbutton(root, text=script, variable=var)
    cb.pack(anchor=tk.W)
    checkboxes[script] = var

# Botão para executar os scripts selecionados
execute_button = tk.Button(root, text="Executar Scripts Selecionados", command=execute_scripts)
execute_button.pack(pady=10)

# Área de texto para mostrar a saída dos scripts
output_text = scrolledtext.ScrolledText(root, height=10)
output_text.pack(fill=tk.BOTH, expand=True)

root.after(100, update_output)  # Inicia a verificação da fila para atualizar a saída

root.mainloop()