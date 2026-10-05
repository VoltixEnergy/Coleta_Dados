import csv
import datetime
import os
import subprocess
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
    "mac_address",
    "created_at",
] + COMPONENTES_METRICAS

INTERVALO_SEGUNDOS = 10
DURACAO_MINUTOS = 5
TOTAL_CICLOS = (DURACAO_MINUTOS * 60) // INTERVALO_SEGUNDOS

# Impacto de cada Docker nas métricas
IMPACTO_DOCKER_PCT = 2


def enviar_para_s3(caminho_arquivo_local, nome_arquivo_apenas):

    bucket_name = os.getenv("S3_BUCKET_BRONZE")

    if not bucket_name:
        print(
            "[Aviso] Variável 'S3_BUCKET_BRONZE' "
            "não configurada no .env. Upload cancelado."
        )
        return False

    try:
        s3_client = boto3.client("s3")

        chave_s3 = f"raw/{nome_arquivo_apenas}"

        print(
            f"\n[S3] Enviando arquivo '{nome_arquivo_apenas}' "
            f"para '{bucket_name}/raw'..."
        )

        s3_client.upload_file(
            caminho_arquivo_local,
            bucket_name,
            chave_s3
        )

        print("[S3] Upload realizado com sucesso!")
        print(f"[S3] Caminho no S3: s3://{bucket_name}/{chave_s3}")

        return True

    except (BotoCoreError, ClientError) as err:
        print(
            f"[Erro S3] Falha ao enviar o arquivo para o S3: {err}"
        )
        return False


def obter_configuracao_instancia(mac_address):

    mac_upper = mac_address.upper()
    mac_limpo = mac_upper.replace(":", "").replace("-", "")

    query = """
        SELECT
            i.id
        FROM instancia i
        WHERE (UPPER(i.endereco_mac) = %s OR UPPER(i.endereco_mac) = %s)
          AND i.deletado_em IS NULL
    """

    try:
        with mysql.connector.connect(**DB_CONFIG) as conexao:

            with conexao.cursor(dictionary=True) as cursor:

                cursor.execute(
                    query,
                    (mac_upper, mac_limpo)
                )

                return cursor.fetchone()

    except mysql.connector.Error as err:

        print(
            f"[Aviso] Erro ao buscar configuração "
            f"da instância: {err}"
        )

        return None


def obter_metricas_ativas_do_banco(mac_address):

    record = {
        comp: False
        for comp in COMPONENTES_METRICAS
    }

    record["mac_address"] = True
    record["created_at"] = True

    mac_upper = mac_address.upper()
    mac_limpo = mac_upper.replace(":", "").replace("-", "")

    query = """
        SELECT e.nome AS metrica_nome
        FROM metrica m
        JOIN especificacao e
            ON m.especificacao_id = e.id
        JOIN instancia i
            ON m.instancia_id = i.id
        WHERE (UPPER(i.endereco_mac) = %s OR UPPER(i.endereco_mac) = %s)
          AND i.deletado_em IS NULL
          AND m.deletado_em IS NULL
    """

    try:

        with mysql.connector.connect(**DB_CONFIG) as conexao:

            with conexao.cursor(dictionary=True) as cursor:

                cursor.execute(
                    query,
                    (mac_upper, mac_limpo)
                )

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


def obter_quantidade_docker():

    try:

        output = subprocess.check_output(
            "docker ps",
            shell=True
        ).decode("utf-8")

        quantidade_docker = len(
            output.split("\n")
        ) - 2

        if quantidade_docker < 0:
            quantidade_docker = 0

        return quantidade_docker

    except subprocess.CalledProcessError:

        print(
            "[Aviso] Não foi possível acessar o Docker."
        )

        return 0


def capturar():

    mac_address = (
        getmac.get_mac_address()
        or "Desconhecido"
    )

    print(
        f"Buscando configurações no banco "
        f"'{DB_CONFIG['database']}' "
        f"para o MAC: {mac_address}..."
    )

    configuracao = obter_configuracao_instancia(
        mac_address
    )

    if configuracao is None:

        print(
            "Instância não encontrada no banco."
        )

        return

    instancia_id = configuracao["id"]

    # Quantidade de Docker em execução
    quantidade_docker = obter_quantidade_docker()

    # total causado pelos Docker
    impacto_docker_pct = (
        quantidade_docker * IMPACTO_DOCKER_PCT
    )

    print(
        f"Instância encontrada: {instancia_id}"
    )

    print(
        f"Docker em execução: {quantidade_docker}"
    )

    print(
        f"Impacto dos Docker: "
        f"+{impacto_docker_pct}%"
    )

    record = obter_metricas_ativas_do_banco(
        mac_address
    )

    metricas_ativas = [
        comp
        for comp, ativo in record.items()
        if ativo
    ]

    print(
        f"Métricas ativas carregadas: "
        f"{', '.join(metricas_ativas)}"
    )

    horario_nome = datetime.datetime.now().strftime(
        "%d-%m-%Y-%H-%M-%S"
    )

    mac_formatado = mac_address.replace(
        ":",
        "-"
    )

    nome_arquivo_apenas = (
        f"{mac_formatado}--{horario_nome}.csv"
    )

    caminho_arquivo_local = (
        f"./data/{nome_arquivo_apenas}"
    )

    raiz_disco = os.path.abspath(os.sep)

    rede_anterior = psutil.net_io_counters()

    # Abre e grava o arquivo CSV localmente
    with open(
        caminho_arquivo_local,
        "w",
        newline="",
        encoding="utf-8"
    ) as csvfile:

        writer = csv.writer(
            csvfile,
            delimiter=";"
        )

        writer.writerow(
            ALL_COMPONENTS
        )

        print(
            f"\nIniciando captura de "
            f"{DURACAO_MINUTOS} minutos "
            f"({TOTAL_CICLOS} ciclos de "
            f"{INTERVALO_SEGUNDOS}s)...\n"
        )

        for ciclo in range(TOTAL_CICLOS):

            print(
                f"--- Ciclo {ciclo + 1} "
                f"de {TOTAL_CICLOS} ---"
            )

            horario_atual = (
                datetime.datetime.now().strftime(
                    "%d/%m/%Y %H:%M:%S"
                )
            )

            # ==========================
            # COLETA DOS DADOS REAIS
            # ==========================

            cpu_real = psutil.cpu_percent(
                interval=INTERVALO_SEGUNDOS
            )

            ram = psutil.virtual_memory()

            dados_rede = psutil.net_io_counters()

            disco = psutil.disk_usage(
                raiz_disco
            )

            # ==========================
            # REDE
            # ==========================

            bytes_enviados_delta = max(
                dados_rede.bytes_sent
                - rede_anterior.bytes_sent,
                0
            )

            bytes_recebidos_delta = max(
                dados_rede.bytes_recv
                - rede_anterior.bytes_recv,
                0
            )

            pacotes_perdidos_delta = max(
                (
                    dados_rede.dropin
                    + dados_rede.dropout
                )
                - (
                    rede_anterior.dropin
                    + rede_anterior.dropout
                ),
                0
            )

            rede_anterior = dados_rede

            mb_enviados_real = (
                bytes_enviados_delta
                / 1_048_576
            )

            mb_recebidos_real = (
                bytes_recebidos_delta
                / 1_048_576
            )

            # ==========================
            # CPU
            # ==========================

            cpu = min(
                cpu_real + impacto_docker_pct,
                100.0
            )

            # ==========================
            # RAM
            # ==========================

            ram_total = (
                ram.total
                / 1_073_741_824
            )

            ram_uso_simulado = min(
                ram.percent + impacto_docker_pct,
                99.0
            )

            ram_free_simulado = max(
                ram_total
                * (
                    1
                    - (
                        ram_uso_simulado
                        / 100.0
                    )
                ),
                0.0
            )

            # ==========================
            # DISCO
            # ==========================

            disk_use_simulado = min(
                disco.percent + impacto_docker_pct,
                100.0
            )

            disk_free_pct = max(
                100.0 - disk_use_simulado,
                0.0
            )

            # ==========================
            # REDE
            # ==========================

            network_received = mb_recebidos_real

            network_sent = mb_enviados_real

            # ==========================
            # PACOTES PERDIDOS
            # ==========================

            package_drop = pacotes_perdidos_delta

            # ==========================
            # VALORES DO CICLO
            # ==========================

            valores_ciclo = {

                "instancia_id":
                    instancia_id,

                "mac_address":
                    mac_address,

                "created_at":
                    horario_atual,

                "cpu_use_percent":
                    f"{cpu:.2f}"
                    if record["cpu_use_percent"]
                    else "",

                "ram_total_gb":
                    f"{ram_total:.2f}"
                    if record["ram_total_gb"]
                    else "",

                "ram_free_gb":
                    f"{ram_free_simulado:.2f}"
                    if record["ram_free_gb"]
                    else "",

                "disk_free_percent":
                    f"{disk_free_pct:.2f}"
                    if record["disk_free_percent"]
                    else "",

                "network_sent":
                    f"{network_sent:.2f}"
                    if record["network_sent"]
                    else "",

                "network_received":
                    f"{network_received:.2f}"
                    if record["network_received"]
                    else "",

                "package_drop_total":
                    f"{package_drop:.2f}"
                    if record["package_drop_total"]
                    else "",
            }

            linha_dados = [
                valores_ciclo.get(
                    col,
                    ""
                )
                for col in ALL_COMPONENTS
            ]

            writer.writerow(
                linha_dados
            )

            csvfile.flush()

            print(
                f"[{horario_atual}] "
                f"Docker: {quantidade_docker} | "
                f"CPU: {cpu:.2f}% | "
                f"RAM livre: "
                f"{ram_free_simulado:.2f} GB | "
                f"Disco livre: "
                f"{disk_free_pct:.2f}% | "
                f"Recebidos: "
                f"{network_received:.2f} MB | "
                f"Enviados: "
                f"{network_sent:.2f} MB"
            )

    print(
        "\nCaptura local finalizada com sucesso."
    )

    # Envia o arquivo para o S3
    enviar_para_s3(
        caminho_arquivo_local,
        nome_arquivo_apenas
    )


if __name__ == "__main__":
    capturar()