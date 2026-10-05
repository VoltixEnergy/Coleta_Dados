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

# Carrega as variáveis definidas no arquivo .env (ex: S3_BUCKET_BRONZE, S3_BUCKET_SILVER)
load_dotenv()

# Nome dos Buckets AWS S3 recuperados do .env (com valores padrão como fallback)
BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "meu-bucket-bronze")
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "meu-bucket-silver")


# ==============================================================================
# 1. EXTRACT 
# ==============================================================================
def carregar_csvs_da_bronze(s3_client):
    """
    Função responsável por listar e ler todos os arquivos CSV brutos
    presentes na pasta 'raw/' do Bucket Bronze.
    
    Retorna:
        pd.DataFrame: Dataframe contendo a união de todos os CSVs brutos encontrados.
    """
    print(f"[EXTRACT] Lendo arquivos da pasta 'raw/' no bucket '{BUCKET_BRONZE}'...")
    
    lista_dfs = []
    
    # O 'paginator' evita problemas se estiver centenas/milhares de arquivos na pasta
    paginator = s3_client.get_paginator("list_objects_v2")
    
    # Percorre todas as páginas de resultados dentro da pasta 'raw/'
    for page in paginator.paginate(Bucket=BUCKET_BRONZE, Prefix="raw/"):
        for obj in page.get("Contents", []):
            chave = obj["Key"]
            
            # Processa apenas arquivos que possuem extensão .csv
            if chave.endswith(".csv"):
                # Busca o conteúdo do arquivo no S3 sem precisar baixar para o disco
                response = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=chave)
                conteudo = response["Body"].read().decode("utf-8")
                
                # io.StringIO transforma a string em um "arquivo em memória" para o Pandas ler
                # dtype=str força a leitura inicial como texto para evitar perda de caracteres ou erros de formato
                df_temp = pd.read_csv(io.StringIO(conteudo), sep=";", dtype=str)
                lista_dfs.append(df_temp)

    # Caso nenhum arquivo CSV seja localizado, encerra o fluxo com um DataFrame vazio
    if not lista_dfs:
        print("[Aviso] Nenhum arquivo CSV encontrado na pasta 'raw/'.")
        return pd.DataFrame()

    # Unifica todos os DataFrames da lista em um único grande DataFrame
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"[EXTRACT] Total de registros brutos extraídos: {len(df_consolidado)}")
    return df_consolidado


# ==============================================================================
# 2. DATA CLEANING 
# ==============================================================================
def limpar_e_enriquecer_dados(df):
    """
    Realiza a higienização dos dados brutos:
    - Substituição de valores vazios por Nulo (NaN)
    - Remocão de duplicatas e dados inválidos
    - Conversão de tipos (Texto para Data/Hora e Numérico)
    - Criação de colunas calculadas por linha (ex: % de RAM e Disco usados)
    """
    print("\n[DATA CLEAN] Iniciando limpeza e conversão de tipos...")

    # 2.1 Substituir strings vazias, hífens soltos ou múltiplos espaços por None/NaN
    df = df.replace(r"^\s*$", None, regex=True)

    # 2.2 Eliminar linhas onde os campos cruciais (ID da Instância ou Data da Leitura) estejam nulos
    df = df.dropna(subset=["instancia_id", "created_at"])

    # 2.3 Remover linhas duplicadas exatamente iguais para a mesma instância no mesmo segundo
    linhas_antes = len(df)
    df = df.drop_duplicates(subset=["instancia_id", "created_at"])
    print(f"[CLEAN] Duplicatas removidas: {linhas_antes - len(df)}")

    # 2.4 Padronizar textos (Garantir maiúsculas e sem espaços nas pontas)
    df["mac_address"] = df["mac_address"].str.upper().str.strip()
    df["cenario"] = df["cenario"].str.upper().str.strip()

    # 2.5 Converter a coluna 'created_at' (String "DD/MM/YYYY HH:MM:SS") em tipo Datetime
    df["created_at"] = pd.to_datetime(
        df["created_at"], 
        format="%d/%m/%Y %H:%M:%S", 
        errors="coerce"  # Converte erros para NaT (Not a Time) em vez de quebrar o código
    )

    # 2.6 Mapear e converter colunas numéricas de texto para float
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
            # errors='coerce' transforma textos inválidos em NaN sem interromper a execução
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 2.7 CÁLCULOS EM NÍVEL DE LINHA 
    # RAM Usada (GB) = Total - Livre
    df["ram_used_gb"] = (df["ram_total_gb"] - df["ram_free_gb"]).round(2)
    
    # % Uso de RAM = (RAM Usada / RAM Total) * 100
    df["ram_use_percent"] = ((df["ram_used_gb"] / df["ram_total_gb"]) * 100).round(2)
    
    # % Uso de Disco = 100 - % Disco Livre
    df["disk_use_percent"] = (100.0 - df["disk_free_percent"]).round(2)
    
    # Tráfego Total da Rede por Ciclo (MB) = Enviado + Recebido 
    df["network_total_mb"] = (df["network_sent"] + df["network_received"]).round(2)

    return df


#DATA TRANSFORM

def gerar_estatisticas_agregadas(df):
    """
    Agrupa os dados limpos por Instância, Cenário e Endereço MAC para calcular
    métricas agregadas (Média, Mediana, Desvio Padrão, Picos, Totais).
    
    Retorna:
        pd.DataFrame: Resumo executivo consolidado.
    """
    print("[TRANSFORM] Calculando estatísticas agregadas por Instância e Cenário...")

    # Cria uma flag condicional: marca '1' se o ciclo esteve sob estresse (CPU > 80% ou RAM > 85%)
    df["flag_alta_carga"] = (
        (df["cpu_use_percent"] > 80) | (df["ram_use_percent"] > 85)
    ).astype(int)

    # Agrupamento e aplicação de funções agregadoras (.agg)
    agregado = df.groupby(["instancia_id", "cenario", "mac_address"]).agg(
        # Métricas de Volume
        total_ciclos_analisados=("created_at", "count"),
        
        # CPU: Média, Mediana, Pico Máximo e Desvio Padrã
        cpu_media=("cpu_use_percent", "mean"),
        cpu_mediana=("cpu_use_percent", "median"),
        cpu_pico_maximo=("cpu_use_percent", "max"),
        cpu_desvio_padrao=("cpu_use_percent", "std"),

        # RAM: Média, Mediana e Pico de Uso 
        ram_uso_percent_medio=("ram_use_percent", "mean"),
        ram_uso_percent_mediana=("ram_use_percent", "median"),
        ram_uso_percent_pico=("ram_use_percent", "max"),

        # Disco: Média de Uso (%) e Menor % de Espaço Livre registrado
        disco_uso_percent_medio=("disk_use_percent", "mean"),
        disco_livre_percent_minimo=("disk_free_percent", "min"),

        # Rede: Totais Acumulados, Média por ciclo e Picos de Tráfego (MB)
        rede_recebida_total_mb=("network_received", "sum"),
        rede_enviada_total_mb=("network_sent", "sum"),
        rede_trafego_total_mb=("network_total_mb", "sum"),
        rede_trafego_medio_por_ciclo=("network_total_mb", "mean"),
        rede_pico_mb_ciclo=("network_total_mb", "max"),

        # Qualidade da Rede: Perda de Pacotes Total e Média por ciclo
        pacotes_perdidos_total=("package_drop_total", "sum"),
        pacotes_perdidos_medio=("package_drop_total", "mean"),

        # Alertas: Total de ciclos em estresse de hardware
        qtd_ciclos_alta_carga=("flag_alta_carga", "sum")
    ).reset_index()

    # Arredonda todas as colunas de ponto flutuante para 2 casas decimais
    colunas_float = agregado.select_dtypes(include=["float64", "float32"]).columns
    agregado[colunas_float] = agregado[colunas_float].round(2)

    # Adiciona carimbo de data/hora de quando a ETL gerou os cálculos
    agregado["processado_em"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return agregado



# 4. LOAD 
def salvar_csv_no_s3(s3_client, df, chave_s3):
    """
    Converte o DataFrame em texto CSV e realiza o upload diretamente para o S3 Silver
    através de um buffer de memória (sem criar arquivos locais temporários).
    """
    # Buffer de memória que simula um arquivo de texto
    csv_buffer = io.StringIO()
    
    # Exporta o DataFrame para o buffer mantendo o delimitador ';' e sem o índice numérico do pandas
    df.to_csv(csv_buffer, index=False, sep=";", encoding="utf-8")

    # Faz o envio direto para o S3
    s3_client.put_object(
        Bucket=BUCKET_SILVER,
        Key=chave_s3,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    print(f"[LOAD] Arquivo CSV salvo com sucesso em: s3://{BUCKET_SILVER}/{chave_s3}")


# ==============================================================================
# 5. ORQUESTRAÇÃO / FLUXO PRINCIPAL
# ==============================================================================
def executar_etl():
    """
    Orquestra todas as etapas do Pipeline ETL:
    Extração (Bronze) -> Limpeza -> Transformação Estatística -> Carga (Silver)
    """
    try:
        # Inicializa o cliente do S3 usando as credenciais ativas (~/.aws/credentials ou .env)
        s3_client = boto3.client("s3")

        # 1. Extração
        df_bruto = carregar_csvs_da_bronze(s3_client)
        if df_bruto.empty:
            print("[Info] Fluxo interrompido: nenhum dado para processar.")
            return

        # 2. Data Cleaning & Cálculos Linha a Linha
        df_limpo = limpar_e_enriquecer_dados(df_bruto)

        # 3. Data Transform & Agregações Estatísticas
        df_resumo = gerar_estatisticas_agregadas(df_limpo)

        # 4. Carga dos dois arquivos de saída no S3 Silver
        data_hoje = datetime.datetime.now().strftime("%Y-%m-%d")

        # Arquivo 1: Dados ciclo a ciclo limpos
        chave_detalhada = f"silver/detalhado/telemetria_limpa_{data_hoje}.csv"
        salvar_csv_no_s3(s3_client, df_limpo, chave_detalhada)

        # Arquivo 2: Resumo estatístico agregado (Média, Mediana, Desvio Padrão, etc.)
        chave_resumo = f"silver/agregado/telemetria_resumo_estatistico_{data_hoje}.csv"
        salvar_csv_no_s3(s3_client, df_resumo, chave_resumo)

        print("\n[SUCESSO] Pipeline ETL executado com êxito!")

    except (BotoCoreError, ClientError) as err:
        print(f"[ERRO AWS] Falha na comunicação com o serviço S3: {err}")
    except Exception as err:
        print(f"[ERRO ETL] Falha durante a execução do pipeline: {err}")


if __name__ == "__main__":
    executar_etl()