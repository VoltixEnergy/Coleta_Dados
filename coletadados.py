import psutil
import datetime
import time
import csv
import platform

def coletar_dados(intervalo_segundos=5, limite=5):
    contador = 0
    
    print(f"Sistema detectado: {platform.system()}")
    print(f"Iniciando coleta: {limite} leituras a cada {intervalo_segundos} segundos.\n")

    with open('./leitura.csv', 'w', newline='', encoding='utf-8') as arquivo_csv:
        csv_linha = csv.writer(arquivo_csv, delimiter=';')
        
        csv_linha.writerow(['Usuario', 'Data Hora', 'CPU%', 'Freq CPU (MHz)', 'Memoria%', 'Disco%'])

        while contador < limite:
            contador += 1

            usuario = "seuNome"
            cpu_porcentagem = psutil.cpu_percent(interval=1)
            cpu_frequencia = round(psutil.cpu_freq().current) 
            memoria_porcentagem = round(psutil.virtual_memory().percent)
            caminho_disco = 'C:\\' if platform.system() == "Windows" else '/'
            disco_porcentagem = psutil.disk_usage(caminho_disco).percent
            data_hora_formatada = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
         
           
            csv_linha.writerow([usuario, 
            data_hora_formatada, 
            cpu_porcentagem, 
            cpu_frequencia, 
            memoria_porcentagem, 
            disco_porcentagem])

       
            print(f'Leitura {contador}/{limite} | {data_hora_formatada}')
            print(f'CPU: {cpu_porcentagem}% ({cpu_frequencia} MHz) | Memória: {memoria_porcentagem}% | Disco: {disco_porcentagem}%')
            print('-' * 45)

            if contador < limite:
                time.sleep(max(0, intervalo_segundos - 1))

coletar_dados(intervalo_segundos=5, limite=5)