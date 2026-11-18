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
    print(f"[EXTRACT] Lendo arquivos da pasta 'raw/logs/' no bucket '{BUCKET_BRONZE}'...")

    lista_dfs = []
    chaves_lidas = []  # guarda os nomes dos arquivos para movê-los depois

    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_BRONZE, Prefix="raw/logs/"):
        for obj in page.get("Contents", []):
            chave = obj["Key"]

            if chave.endswith(".csv"):
                response = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=chave)
                conteudo = response["Body"].read().decode("utf-8")

                df_temp = pd.read_csv(io.StringIO(conteudo), sep=";", dtype=str)
                lista_dfs.append(df_temp)
                chaves_lidas.append(chave)

    if not lista_dfs:
        print("[Aviso] Nenhum arquivo CSV novo encontrado na pasta 'raw/logs/'.")
        return pd.DataFrame(), []

    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"[EXTRACT] Total de registros brutos extraídos: {len(df_consolidado)}")
    return df_consolidado, chaves_lidas


def mover_para_processed(s3_client, chaves):
    """
    Move cada arquivo de 'raw/logs/' para 'processed/logs/' dentro do Bucket Bronze.
    No S3 não existe "mover": primeiro copia para o novo lugar, depois apaga o original.
    """
    for chave in chaves:
        nova_chave = chave.replace("raw/logs/", "processed/logs/", 1)

        # 1) copia para processed/logs/
        s3_client.copy_object(
            Bucket=BUCKET_BRONZE,
            CopySource={"Bucket": BUCKET_BRONZE, "Key": chave},
            Key=nova_chave,
        )
        # 2) apaga o original de raw/logs/
        s3_client.delete_object(Bucket=BUCKET_BRONZE, Key=chave)

    print(f"[MOVE] {len(chaves)} arquivo(s) movido(s) para 'processed/logs/'.")


# ==============================================================================
# 2. DATA CLEANING AND DATA TRANSFORM
# ==============================================================================
def limpar_e_transformar_dados(df):
    """
    Realiza a higienização dos dados brutos:
    - Substituição de valores vazios por Nulo (NaN)
    - Remoção de duplicatas e dados inválidos
    - Conversão de tipos (Texto para Data/Hora e Numérico)
    - Criação de colunas calculadas por linha (ex: % de RAM e Disco usados)
    """
    print("\n[DATA CLEAN] Iniciando limpeza e conversão de tipos...")

    df = df.replace(r"^\s*$", None, regex=True)

    df = df.dropna(subset=["mac_address", "container_name", "level", "ts", "scope", "message"])

    df["mac_address"] = df["mac_address"].str.upper().str.strip()

    df["created_at"] = pd.to_datetime(
        df["ts"],
        errors="coerce",
        unit="ms"
    ).dt.strftime("%d/%m/%Y %H:%M:%S")

    df.drop(columns=['ts'], inplace=True)

    return df

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

        # 2. Limpeza e transformar
        df_limpo = limpar_e_transformar_dados(df_bruto)

        # 3. Carga
        # Data + hora no nome, para uma execução não sobrescrever a anterior
        carimbo = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")

        chave_detalhada = f"logs/cleaned-{carimbo}.csv"
        salvar_csv_no_s3(s3_client, df_limpo, chave_detalhada)

        # 4. Só move para processed/ DEPOIS de salvar tudo com sucesso.
        # Se algo der erro antes, os arquivos continuam em logs/ e serão lidos de novo.
        mover_para_processed(s3_client, chaves_lidas)

        print("\n[SUCESSO] Pipeline ETL executado com êxito!")

    except (BotoCoreError, ClientError) as err:
        print(f"[ERRO AWS] Falha na comunicação com o serviço S3: {err}")
    except Exception as err:
        print(f"[ERRO ETL] Falha durante a execução do pipeline: {err}")


if __name__ == "__main__":
    executar_etl()
