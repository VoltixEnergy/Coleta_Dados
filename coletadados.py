import csv
import datetime
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import mysql.connector
import getmac
import psutil
from dotenv import load_dotenv

# Carrega as variáveis de ambiente (.env)
load_dotenv()

# Configurações do Banco de Dados
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 3306)),
}

# Métricas que podem ser coletadas
COMPONENTES_METRICAS = [
    "cpu_use_percent",
    "ram_total_gb",
    "ram_free_gb",
    "disk_free_percent",
    "network_sent",
    "network_received",
    "package_drop_total",
]

# Colunas que serão gravadas no CSV
ALL_COMPONENTS = [
    "instancia_id",
    "cenario",
    "mac_address",
    "created_at",
] + COMPONENTES_METRICAS

INTERVALO_SEGUNDOS = 10
DURACAO_MINUTOS = 5
TOTAL_CICLOS = (DURACAO_MINUTOS * 60) // INTERVALO_SEGUNDOS


def enviar_para_s3(caminho_arquivo_local, nome_arquivo_apenas):

    bucket_name = os.getenv("S3_BUCKET_BRONZE")
    
    if not bucket_name:
        print("[Aviso] Variável 'S3_BUCKET_BRONZE' não configurada no .env. Upload cancelado.")
        return False

    try:
        s3_client = boto3.client("s3")

        chave_s3 = f"raw/{nome_arquivo_apenas}"

        print(f"\n[S3] Enviando arquivo '{nome_arquivo_apenas}' para '{bucket_name}/raw'...")
        
        # Envia o arquivo CSV local para o S3
        s3_client.upload_file(caminho_arquivo_local, bucket_name, chave_s3)
        
        print(f"[S3] Upload realizado com sucesso!")
        print(f"[S3] Caminho no S3: s3://{bucket_name}/{chave_s3}")
        return True

    except (BotoCoreError, ClientError) as err:
        print(f"[Erro S3] Falha ao enviar o arquivo para o S3: {err}")
        return False


def obter_configuracao_instancia(mac_address):
    mac_upper = mac_address.upper()
    mac_limpo = mac_upper.replace(":", "").replace("-", "")

    query = """
        SELECT
            i.id,
            i.cenario
        FROM instancia i
        WHERE (UPPER(i.endereco_mac) = %s OR UPPER(i.endereco_mac) = %s)
          AND i.deletado_em IS NULL
    """

    try:
        with mysql.connector.connect(**DB_CONFIG) as conexao:
            with conexao.cursor(dictionary=True) as cursor:
                cursor.execute(query, (mac_upper, mac_limpo))
                return cursor.fetchone()
    except mysql.connector.Error as err:
        print(f"[Aviso] Erro ao buscar configuração da instância: {err}")
        return None


def obter_metricas_ativas_do_banco(mac_address):
    record = {comp: False for comp in COMPONENTES_METRICAS}
    record["mac_address"] = True
    record["created_at"] = True

    mac_upper = mac_address.upper()
    mac_limpo = mac_upper.replace(":", "").replace("-", "")

    query = """
        SELECT e.nome AS metrica_nome
        FROM metrica m
        JOIN especificacao e ON m.especificacao_id = e.id
        JOIN instancia i ON m.instancia_id = i.id
        WHERE (UPPER(i.endereco_mac) = %s OR UPPER(i.endereco_mac) = %s)
          AND i.deletado_em IS NULL
          AND m.deletado_em IS NULL
    """

    try:
        with mysql.connector.connect(**DB_CONFIG) as conexao:
            with conexao.cursor(dictionary=True) as cursor:
                cursor.execute(query, (mac_upper, mac_limpo))
                resultados = cursor.fetchall()
                for linha in resultados:
                    metrica = linha["metrica_nome"]
                    if metrica in record:
                        record[metrica] = True
    except mysql.connector.Error as err:
        print(
            f"[Aviso] Falha ao conectar ao banco ({err}). "
            "Ativando todas as métricas como fallback."
        )
        for comp in COMPONENTES_METRICAS:
            record[comp] = True

    return record


def obter_regras_cenario(cenario):
    regras = {
        "NORMAL": {
            "incremento_pct": 20,
            "mult_received": 1.0,
            "add_received_mb": 0.0,
            "mult_sent": 1.0,
            "add_sent_mb": 0.0,
            "add_package_drop": 0,
        },
        "ACIMA_DA_MEDIA": {
            "incremento_pct": 40,
            "mult_received": 4.0,
            "add_received_mb": 30.0,
            "mult_sent": 1.2,
            "add_sent_mb": 1.5,
            "add_package_drop": 5,
        },
        "ALTA_DEMANDA": {
            "incremento_pct": 60,
            "mult_received": 8.0,
            "add_received_mb": 80.0,
            "mult_sent": 1.5,
            "add_sent_mb": 4.0,
            "add_package_drop": 25,
        },
    }
    return regras.get(cenario, regras["NORMAL"])


def capturar():
    mac_address = getmac.get_mac_address() or "Desconhecido"

    print(
        f"Buscando configurações no banco "
        f"'{DB_CONFIG['database']}' para o MAC: {mac_address}..."
    )

    configuracao = obter_configuracao_instancia(mac_address)

    if configuracao is None:
        print("Instância não encontrada no banco.")
        return

    instancia_id = configuracao["id"]
    cenario = configuracao["cenario"]

    regras = obter_regras_cenario(cenario)
    incremento_pct = regras["incremento_pct"]

    print(f"Instância encontrada: {instancia_id}")
    print(f"Cenário: {cenario}")
    print(f"Acréscimo de Hardware: +{incremento_pct}%")
    print(f"Ingestão de Medidores (Entrada): {regras['mult_received']}x (+{regras['add_received_mb']} MB)")

    record = obter_metricas_ativas_do_banco(mac_address)
    metricas_ativas = [comp for comp, ativo in record.items() if ativo]

    print(f"Métricas ativas carregadas: {', '.join(metricas_ativas)}")

    horario_nome = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    mac_formatado = mac_address.replace(":", "-")
    nome_arquivo_apenas = f"{mac_formatado}--{horario_nome}.csv"
    caminho_arquivo_local = f"./{nome_arquivo_apenas}"

    raiz_disco = os.path.abspath(os.sep)
    rede_anterior = psutil.net_io_counters()

    # 1. Abre e grava o arquivo CSV localmente durante os 5 minutos de captura
    with open(caminho_arquivo_local, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(ALL_COMPONENTS)

        print(
            f"\nIniciando captura de {DURACAO_MINUTOS} minutos "
            f"({TOTAL_CICLOS} ciclos de {INTERVALO_SEGUNDOS}s)...\n"
        )

        for ciclo in range(TOTAL_CICLOS):
            print(f"--- Ciclo {ciclo + 1} de {TOTAL_CICLOS} ---")
            horario_atual = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            cpu_real = psutil.cpu_percent(interval=INTERVALO_SEGUNDOS)
            ram = psutil.virtual_memory()
            dados_rede = psutil.net_io_counters()
            disco = psutil.disk_usage(raiz_disco)

            bytes_enviados_delta = max(dados_rede.bytes_sent - rede_anterior.bytes_sent, 0)
            bytes_recebidos_delta = max(dados_rede.bytes_recv - rede_anterior.bytes_recv, 0)
            pacotes_perdidos_delta = max(
                (dados_rede.dropin + dados_rede.dropout)
                - (rede_anterior.dropin + rede_anterior.dropout),
                0,
            )
            rede_anterior = dados_rede

            mb_enviados_real = bytes_enviados_delta / 1_048_576
            mb_recebidos_real = bytes_recebidos_delta / 1_048_576

            cpu = min(cpu_real + incremento_pct, 100.0)

            ram_total = ram.total / 1_073_741_824
            ram_uso_simulado = min(ram.percent + incremento_pct, 99.0)
            ram_free_simulado = max(ram_total * (1 - (ram_uso_simulado / 100.0)), 0.0)

            disk_use_simulado = min(disco.percent + incremento_pct, 100.0)
            disk_free_pct = max(100.0 - disk_use_simulado, 0.0)

            network_received = (mb_recebidos_real * regras["mult_received"]) + regras["add_received_mb"]
            network_sent = (mb_enviados_real * regras["mult_sent"]) + regras["add_sent_mb"]
            package_drop = pacotes_perdidos_delta + regras["add_package_drop"]

            valores_ciclo = {
                "instancia_id": instancia_id,
                "cenario": cenario,
                "mac_address": mac_address,
                "created_at": horario_atual,
                "cpu_use_percent": f"{cpu:.2f}" if record["cpu_use_percent"] else "",
                "ram_total_gb": f"{ram_total:.2f}" if record["ram_total_gb"] else "",
                "ram_free_gb": f"{ram_free_simulado:.2f}" if record["ram_free_gb"] else "",
                "disk_free_percent": f"{disk_free_pct:.2f}" if record["disk_free_percent"] else "",
                "network_sent": f"{network_sent:.2f}" if record["network_sent"] else "",
                "network_received": f"{network_received:.2f}" if record["network_received"] else "",
                "package_drop_total": f"{package_drop:.2f}" if record["package_drop_total"] else "",
            }

            linha_dados = [valores_ciclo.get(col, "") for col in ALL_COMPONENTS]
            writer.writerow(linha_dados)
            csvfile.flush()

            print(
                f"[{horario_atual}] "
                f"CPU: {cpu:.2f}% | "
                f"Leituras Recebidas: {network_received:.2f} MB | "
                f"Respostas Enviadas: {network_sent:.2f} MB"
            )

    print("\nCaptura local finalizada com sucesso.")

    # 2. Envia o arquivo gravado para o S3 (após fechar o bloco 'with open')
    enviar_para_s3(caminho_arquivo_local, nome_arquivo_apenas)


if __name__ == "__main__":
    capturar()