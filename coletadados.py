import psutil
import datetime
import time
import csv
import platform
from jira import JIRA


JIRA_SERVER = 'https://empresa-voltix.atlassian.net'
JIRA_EMAIL = 'ricardo.inoue@sptech.school' 
JIRA_API_TOKEN = 'ATATT3xFfGF0r1e7q9fydoK4QiWywZFKCKoMKqRtGxkD0st54y3M3eeAUfiViZuQXwWfi0PSgX1XrCS8-8daM8g2ysRidqN_HGX7eUJFF_k1-jaLTbrMbN29G2XoLiDv1RnlWcq9y4JJLdSlO57MQUsk_So_QGuAeJfZna8UabI3IgeAHTzgTfw=4FC4BA74'
CHAVE_PROJETO = 'SCRUM' 

try:
    jira_options = {'server': JIRA_SERVER}
    jira_cliente = JIRA(options=jira_options, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))
    print("Conectado com jira\n")
except Exception as erro:
    print(f"Erro de conexão: {erro}")
    exit() 

def criar_ticket_jira(assunto, descricao):
    criar_ticket = {
        'project': {'key': CHAVE_PROJETO},
        'summary': assunto,
        'description': descricao,
        'issuetype': {'name': 'Task'}, 
    }
    try:
        novo_ticket = jira_cliente.create_issue(fields=criar_ticket)
        print(f"Ticket criado: {novo_ticket.key}")
        print("O Slack deve notificar a equipe em instantes!")
    except Exception as e:
        print(f"Erro ao criar o ticket via API: {e}")


def coletar_dados(intervalo_segundos=5, limite=5):
    contador = 0
    LIMITE_CPU = 90.0 
    
    print(f"Sistema detectado: {platform.system()}")
    print(f"Iniciando coleta: {limite} leituras a cada {intervalo_segundos} segundos.\n")

    with open('./leitura.csv', 'w', newline='', encoding='utf-8') as arquivo_csv:
        csv_linha = csv.writer(arquivo_csv, delimiter=';')
        csv_linha.writerow(['Usuario', 'Data Hora', 'CPU%', 'Freq CPU (MHz)', 'Memoria%', 'Disco%'])

        while contador < limite:
            contador += 1
            usuario = "Ricardo"
            cpu_porcentagem = 91
            cpu_frequencia = round(psutil.cpu_freq().current) 
            memoria_porcentagem = round(psutil.virtual_memory().percent)
            caminho_disco = 'C:\\' if platform.system() == "Windows" else '/'
            disco_porcentagem = psutil.disk_usage(caminho_disco).percent
            data_hora_formatada = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
         
            csv_linha.writerow([usuario, data_hora_formatada, cpu_porcentagem, cpu_frequencia, memoria_porcentagem, disco_porcentagem])

            print(f'Leitura {contador}/{limite} | {data_hora_formatada}')
            print(f'CPU: {cpu_porcentagem}% ({cpu_frequencia} MHz) | Memória: {memoria_porcentagem}% | Disco: {disco_porcentagem}%')
            

            if cpu_porcentagem >= LIMITE_CPU:
                print(f"ALERTA: CPU atingiu {cpu_porcentagem}%. ")
                

                assunto_ticket = f"ALERTA CRÍTICO Servidor MDM - CPU em {cpu_porcentagem}%"
                mensagem_ticket = (
                    f"O sistema de monitoramento detectou anomalias no servidor.\n\n"
                    f"*Métricas Atuais do momento do incidente:*\n"
                    f"- *CPU:* {cpu_porcentagem}%\n"
                    f"- *Memória:* {memoria_porcentagem}%\n"
                    f"- *Disco:* {disco_porcentagem}%\n"
                    f"- *Data/Hora:* {data_hora_formatada}\n"
                    f"- *Máquina:* {platform.system()}"
                )
                

                criar_ticket_jira(assunto_ticket, mensagem_ticket)

            print('-' * 45)

            if contador < limite:
                time.sleep(max(0, intervalo_segundos - 1))

coletar_dados(intervalo_segundos=5, limite=5)