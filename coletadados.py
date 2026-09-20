import os
import time
import datetime
import psutil
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from getmac import get_mac_address 


# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def obter_conexao():
    """Estabelece e retorna a conexão com o banco de dados MySQL."""
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def coletar_e_salvar_dados(intervalo_segundos=5, limite=5):
    contador = 0
    
    mac_address = get_mac_address()
    if not mac_address:
        mac_address = "DESCONHECIDO" 
    
    print(f"Endereço MAC da máquina: {mac_address}")
    print(f"Iniciando coleta: {limite} leituras a cada {intervalo_segundos} segundos.\n")

    try:
        conexao = obter_conexao()
        cursor = conexao.cursor()

        while contador < limite:
            contador += 1

            cpu_porcentagem = psutil.cpu_percent(interval=1)
            cpu_frequencia = round(psutil.cpu_freq().current) 
            memoria_porcentagem = round(psutil.virtual_memory().percent)
            
            # Pega o caminho raiz do disco automaticamente sem checar o SO
            caminho_disco = os.path.abspath(os.sep)
            disco_porcentagem = psutil.disk_usage(caminho_disco).percent
            
            data_hora_formatada = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

            # Inserindo os dados no banco
            sql_insert = """
                INSERT INTO leituras_sistema (mac_address, data_hora, cpu_porcentagem, cpu_frequencia, memoria_porcentagem, disco_porcentagem)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            valores = (mac_address, data_hora_formatada, cpu_porcentagem, cpu_frequencia, memoria_porcentagem, disco_porcentagem)
            
            cursor.execute(sql_insert, valores)
            conexao.commit()

            print(f'Leitura {contador}/{limite} | {data_hora_formatada} (Salva no banco)')
            print(f'CPU: {cpu_porcentagem}% ({cpu_frequencia} MHz) | Memória: {memoria_porcentagem}% | Disco: {disco_porcentagem}%')
            print('-' * 45)

            if contador < limite:
                time.sleep(max(0, intervalo_segundos - 1))

        # Relatório de Máximos e Mínimos direto do Banco
        print("\n" + "="*40)
        print("RELATÓRIO DE MÁXIMOS E MÍNIMOS (DIRETO DO BANCO)")
        print("="*40)

        sql_estatisticas = """
            SELECT 
                MAX(cpu_porcentagem), MIN(cpu_porcentagem),
                MAX(memoria_porcentagem), MIN(memoria_porcentagem)
            FROM leituras_sistema
        """
        cursor.execute(sql_estatisticas)
        resultado = cursor.fetchone()

        max_cpu, min_cpu, max_mem, min_mem = resultado

        print(f"CPU    -> Máxima: {max_cpu}% | Mínima: {min_cpu}%")
        print(f"Memória -> Máxima: {max_mem}% | Mínima: {min_mem}%")
        print("="*40)

    except Error as e:
        print(f"Erro ao conectar ou operar no MySQL: {e}")

    finally:
        if 'conexao' in locals() and conexao.is_connected():
            cursor.close()
            conexao.close()
            print("Conexão com o banco encerrada.")

# Executa a função
coletar_e_salvar_dados(intervalo_segundos=5, limite=5)