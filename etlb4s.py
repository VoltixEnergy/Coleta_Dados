import os
import io
import datetime
import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# ==============================================================================
# 0. CONFIGURAÇÕES INICIAIS E VARIÁVEIS DE AMBIENTE
# ==============================================================================

load_dotenv()

BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE")
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")


# ==============================================================================
# 1. EXTRACT
# ==============================================================================
def carregar_csvs_da_bronze(s3_client):
    """
    Lê todos os CSVs que estão na pasta 'raw/' do Bucket Bronze.
    Como os arquivos já processados são movidos para 'processed/',
    tudo que estiver em 'raw/' é novo.

    Retorna:
        (pd.DataFrame, list[str]): os dados lidos e a lista com o nome (chave)
        de cada arquivo lido.
    """
    print(f"[EXTRACT] Lendo arquivos da pasta 'raw/' no bucket '{BUCKET_BRONZE}'...")

    lista_dfs = []
    chaves_lidas = []  # guarda os nomes dos arquivos para movê-los depois

    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_BRONZE, Prefix="raw/"):
        for obj in page.get("Contents", []):
            chave = obj["Key"]

            if chave.endswith(".csv"):
                response = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=chave)
                conteudo = response["Body"].read().decode("utf-8")

                df_temp = pd.read_csv(io.StringIO(conteudo), sep=";", dtype=str)
                lista_dfs.append(df_temp)
                chaves_lidas.append(chave)

    if not lista_dfs:
        print("[Aviso] Nenhum arquivo CSV novo encontrado na pasta 'raw/'.")
        return pd.DataFrame(), []

    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"[EXTRACT] Total de registros brutos extraídos: {len(df_consolidado)}")
    return df_consolidado, chaves_lidas


def mover_para_processed(s3_client, chaves):
    """
    Move cada arquivo de 'raw/' para 'processed/' dentro do Bucket Bronze.
    No S3 não existe "mover": primeiro copia para o novo lugar, depois apaga o original.
    """
    for chave in chaves:
        nova_chave = chave.replace("raw/", "processed/", 1)

        # 1) copia para processed/
        s3_client.copy_object(
            Bucket=BUCKET_BRONZE,
            CopySource={"Bucket": BUCKET_BRONZE, "Key": chave},
            Key=nova_chave,
        )
        # 2) apaga o original de raw/
        s3_client.delete_object(Bucket=BUCKET_BRONZE, Key=chave)

    print(f"[MOVE] {len(chaves)} arquivo(s) movido(s) para 'processed/'.")


# ==============================================================================
# 2. DATA CLEANING
# ==============================================================================
def limpar_e_enriquecer_dados(df):
    """
    Realiza a higienização dos dados brutos:
    - Substituição de valores vazios por Nulo (NaN)
    - Remoção de duplicatas e dados inválidos
    - Conversão de tipos (Texto para Data/Hora e Numérico)
    - Criação de colunas calculadas por linha (ex: % de RAM e Disco usados)
    """
    print("\n[DATA CLEAN] Iniciando limpeza e conversão de tipos...")

    df = df.replace(r"^\s*$", None, regex=True)

    df = df.dropna(subset=["instancia_id", "created_at"])

    linhas_antes = len(df)
    df = df.drop_duplicates(subset=["instancia_id", "created_at"])
    print(f"[CLEAN] Duplicatas removidas: {linhas_antes - len(df)}")

    df["mac_address"] = df["mac_address"].str.upper().str.strip()

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
    )

    colunas_numericas = [
        "cpu_use_percent",
        "ram_total_gb",
        "ram_free_gb",
        "disk_free_percent",
        "network_sent",
        "network_received",
        "package_drop_total",
    ]

    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ram_used_gb"] = (df["ram_total_gb"] - df["ram_free_gb"]).round(2)
    df["ram_use_percent"] = ((df["ram_used_gb"] / df["ram_total_gb"]) * 100).round(2)
    df["disk_use_percent"] = (100.0 - df["disk_free_percent"]).round(2)
    df["network_total_mb"] = (df["network_sent"] + df["network_received"]).round(2)

    return df


# ==============================================================================
# 3. DATA TRANSFORM
# ==============================================================================
def gerar_estatisticas_agregadas(df):
    """
    Agrupa os dados limpos por Instância, Cenário e Endereço MAC para calcular
    métricas agregadas (Média, Mediana, Desvio Padrão, Picos, Totais).
    """
    print("[TRANSFORM] Calculando estatísticas agregadas por Instância e Cenário...")

    df["flag_alta_carga"] = (
        (df["cpu_use_percent"] > 80) | (df["ram_use_percent"] > 85)
    ).astype(int)

    agregado = df.groupby(["instancia_id", "mac_address"]).agg(
        total_ciclos_analisados=("created_at", "count"),

        cpu_media=("cpu_use_percent", "mean"),
        cpu_mediana=("cpu_use_percent", "median"),
        cpu_pico_maximo=("cpu_use_percent", "max"),
        cpu_desvio_padrao=("cpu_use_percent", "std"),

        ram_uso_percent_medio=("ram_use_percent", "mean"),
        ram_uso_percent_mediana=("ram_use_percent", "median"),
        ram_uso_percent_pico=("ram_use_percent", "max"),

        disco_uso_percent_medio=("disk_use_percent", "mean"),
        disco_livre_percent_minimo=("disk_free_percent", "min"),

        rede_recebida_total_mb=("network_received", "sum"),
        rede_enviada_total_mb=("network_sent", "sum"),
        rede_trafego_total_mb=("network_total_mb", "sum"),
        rede_trafego_medio_por_ciclo=("network_total_mb", "mean"),
        rede_pico_mb_ciclo=("network_total_mb", "max"),

        pacotes_perdidos_total=("package_drop_total", "sum"),
        pacotes_perdidos_medio=("package_drop_total", "mean"),

        qtd_ciclos_alta_carga=("flag_alta_carga", "sum"),
    ).reset_index()

    colunas_float = agregado.select_dtypes(include=["float64", "float32"]).columns
    agregado[colunas_float] = agregado[colunas_float].round(2)

    agregado["processado_em"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return agregado


# ==============================================================================
# 4. LOAD
# ==============================================================================
def salvar_csv_no_s3(s3_client, df, chave_s3):
    """
    Converte o DataFrame em CSV e envia direto para o S3 Silver
    através de um buffer de memória.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=";", encoding="utf-8")

    s3_client.put_object(
        Bucket=BUCKET_SILVER,
        Key=chave_s3,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    print(f"[LOAD] Arquivo CSV salvo com sucesso em: s3://{BUCKET_SILVER}/{chave_s3}")


# ==============================================================================
# 5. FLUXO PRINCIPAL
# ==============================================================================
def executar_etl():
    """
    Pipeline ETL:
    Extração (raw/) -> Limpeza -> Transformação -> Carga (Silver) -> Move para processed/
    """
    try:
        s3_client = boto3.client("s3")

        # 1. Extração: lê só o que está em raw/ (ou seja, só os novos)
        df_bruto, chaves_lidas = carregar_csvs_da_bronze(s3_client)
        if df_bruto.empty:
            print("[Info] Fluxo interrompido: nenhum dado para processar.")
            return

        # 2. Limpeza
        df_limpo = limpar_e_enriquecer_dados(df_bruto)

        # 3. Agregações
        df_resumo = gerar_estatisticas_agregadas(df_limpo)

        # 4. Carga
        # Data + hora no nome, para uma execução não sobrescrever a anterior
        carimbo = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")

        chave_detalhada = f"silver/detalhado/telemetria_limpa_{carimbo}.csv"
        salvar_csv_no_s3(s3_client, df_limpo, chave_detalhada)

        chave_resumo = f"silver/agregado/telemetria_resumo_estatistico_{carimbo}.csv"
        salvar_csv_no_s3(s3_client, df_resumo, chave_resumo)

        # 5. Só move para processed/ DEPOIS de salvar tudo com sucesso.
        # Se algo der erro antes, os arquivos continuam em raw/ e serão lidos de novo.
        mover_para_processed(s3_client, chaves_lidas)

        print("\n[SUCESSO] Pipeline ETL executado com êxito!")

    except (BotoCoreError, ClientError) as err:
        print(f"[ERRO AWS] Falha na comunicação com o serviço S3: {err}")
    except Exception as err:
        print(f"[ERRO ETL] Falha durante a execução do pipeline: {err}")


if __name__ == "__main__":
    executar_etl()
