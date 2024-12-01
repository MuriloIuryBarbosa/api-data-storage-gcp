import pymysql
import time

db_ip_public = os.getenv('MYSQL_HOST')
db_user = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')
max_retries = 5  # Número máximo de tentativas
retry_delay = 10  # Tempo de espera entre tentativas em segundos

def connect_to_db():
    retries = 0
    while retries < max_retries:
        try:
            conexao = pymysql.connect(
                host=db_ip_public,
                user=db_user,
                password=db_password,
                db=db_name,
                connect_timeout=90,  # Aumentar o tempo limite de conexão
                ssl_disabled=True  # Ignorar verificação SSL
            )
            print("Conexão ao MySQL estabelecida com sucesso.")
            return conexao
        except pymysql.MySQLError as err:
            print(f"Erro ao conectar ao MySQL: {err}")
            retries += 1
            if retries < max_retries:
                print(f"Tentando novamente em {retry_delay} segundos... ({retries}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print("Número máximo de tentativas atingido. Abortando.")
                return None

def main():
    conexao = connect_to_db()
    if conexao:
        # Faça algo com a conexão, se necessário
        conexao.close()

if __name__ == "__main__":
    main()
