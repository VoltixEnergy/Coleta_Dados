import os
import io
import json
import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Teste local: lê as variáveis do arquivo .env (pip install python-dotenv).
# Na AWS o pacote não existe e as variáveis vêm da configuração da Lambda
# (Configuration > Environment variables).
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import mysql.connector   # pip install mysql-connector-python (na AWS vai junto no pacote/layer da Lambda)


# ==============================================================================
# ETL DA PREVISÃO (Silver -> previsao/<id_da_empresa>.json)
# É uma Lambda separada da Gold: lê o Silver e gera só o histórico que a página de
# previsão usa, com um ponto a cada 12 horas por servidor.
# ==============================================================================

# ==============================================================================
# 0. CONFIGURAÇÕES
# ==============================================================================
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")     # a saída fica no bucket Gold, na pasta previsao/

PREFIXO_SILVER = "silver/detalhado/"          # de onde lemos
PASTA_PREVISAO = "previsao/"                  # saída -> previsao/<id_da_empresa>.json

FUSO_COLETA = "America/Sao_Paulo"

BLOCO_HORAS = 12              # cada ponto resume 12 horas (blocos começam às 00h e 12h locais)
PREV_MAX_BLOCOS = 28          # guarda no máximo 28 blocos (14 dias) por servidor
PREV_HORAS_RELEITURA = 30     # a cada execução relê só as últimas 30 h do Silver
PREV_DIAS_INICIAL = 8         # na primeira execução (pasta previsao/ vazia) relê os últimos 8 dias

COLUNAS_OBRIGATORIAS = ["mac_address", "created_at", "cpu_use_percent", "ram_use_percent"]
COLUNAS_METRICAS = ["cpu_use_percent", "ram_use_percent"]

# Preenchido na execução: a empresa dona de cada servidor (mac -> id da empresa).
EMPRESAS = {}
# Preenchido na execução: limite máximo de cada servidor (mac -> {"cpu": 80.0, "ram": 85.0}).
LIMITES = {}


# ==============================================================================
# 1. FUNÇÕES AUXILIARES
# ==============================================================================
def meio_termo_por_grupo(df, chaves, colunas):
    """
    Ponto médio entre a MÉDIA e a MEDIANA de cada grupo (cada servidor em cada bloco).
    A média sofre com picos isolados; a mediana ignora o comportamento extremo.
    """
    grupos = df.groupby(chaves)[colunas]
    return (grupos.mean() + grupos.median()) / 2


def iso_utc(timestamp):
    """Formata como 2026-10-10T15:00:00Z."""
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def nome_servidor(mac_address):
    """Nome mostrado na página: o próprio MAC, que é o que identifica o servidor."""
    return str(mac_address)


def chave_da_empresa(empresa_id):
    """previsao/<id_da_empresa>.json"""
    return f"{PASTA_PREVISAO}{empresa_id}.json"


# ==============================================================================
# 2. EXTRACT
# ==============================================================================
def conectar_banco():
    """Conecta no MySQL com as variáveis de ambiente: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE", "voltix"),
        connection_timeout=10,
    )


def formatar_mac(mac):
    """E0D55DAA58E3 -> E0:D5:5D:AA:58:E3 (o mesmo formato que as Lambdas usam)."""
    if isinstance(mac, (bytes, bytearray)):
        mac = mac.decode()
    mac = str(mac).replace(":", "").replace("-", "").upper()
    return ":".join(mac[i:i + 2] for i in range(0, len(mac), 2))


def carregar_empresas():
    """Lê do banco a empresa dona de cada servidor (tabela instancia): {mac: id_da_empresa}."""
    print(f"[BANCO] Conectando em '{os.getenv('DB_HOST')}' (banco '{os.getenv('DB_DATABASE', 'voltix')}')...")
    conexao = conectar_banco()
    try:
        cursor = conexao.cursor()
        cursor.execute("SELECT endereco_mac, empresa_id FROM instancia WHERE deletado_em IS NULL")
        empresas = {formatar_mac(mac): str(empresa_id) for mac, empresa_id in cursor.fetchall()}

        # limite máximo de CPU e RAM de cada servidor (tabela metrica), que a página mostra como reta no gráfico
        cursor.execute("""
            SELECT i.endereco_mac, e.nome, m.maximo
            FROM metrica m
            JOIN especificacao e ON e.id = m.especificacao_id
            JOIN instancia i ON i.id = m.instancia_id
            WHERE m.deletado_em IS NULL
              AND i.deletado_em IS NULL
              AND e.nome IN ('cpu_use_percent', 'ram_use_percent')
              AND m.maximo IS NOT NULL
        """)
        for mac, nome_metrica, maximo in cursor.fetchall():
            chave = "cpu" if nome_metrica == "cpu_use_percent" else "ram"
            LIMITES.setdefault(formatar_mac(mac), {})[chave] = float(maximo)
    finally:
        conexao.close()
    print(f"[BANCO] {len(empresas)} servidor(es) com empresa cadastrada e {len(LIMITES)} com limites.")
    return empresas


def previsao_ja_existe(s3_client):
    """True se já há algum arquivo na pasta previsao/ (se não, é a primeira execução)."""
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_GOLD, Prefix=PASTA_PREVISAO, MaxKeys=1)
    return resposta.get("KeyCount", 0) > 0


def carregar_silver(s3_client, desde):
    """Lê os CSVs detalhados enviados ao Silver a partir de 'desde' (None = lê todos)."""
    print(f"[EXTRACT] Listando '{PREFIXO_SILVER}' no bucket '{BUCKET_SILVER}'...")

    arquivos = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_SILVER, Prefix=PREFIXO_SILVER):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".csv"):
                continue
            if desde is not None and obj["LastModified"] < desde:
                continue
            arquivos.append(obj)

    if not arquivos:
        print("[Aviso] Nenhum CSV encontrado no Silver para esse período.")
        return pd.DataFrame()

    lista_dfs = []
    for obj in arquivos:
        resposta = s3_client.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])
        lista_dfs.append(pd.read_csv(io.BytesIO(resposta["Body"].read()), sep=";"))

    df = pd.concat(lista_dfs, ignore_index=True)
    print(f"[EXTRACT] {len(arquivos)} arquivo(s) lido(s), {len(df)} registros.")
    return df


def carregar_previsao_existente(s3_client, empresa_id):
    """Lê o JSON atual da empresa e devolve {mac: {horario: (cpu, ram)}}. Sem arquivo, devolve {}."""
    try:
        resposta = s3_client.get_object(Bucket=BUCKET_GOLD, Key=chave_da_empresa(empresa_id))
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise

    dados = json.loads(resposta["Body"].read().decode("utf-8"))
    historico = {}
    for servidor in dados.get("servidores", []):
        pontos = zip(servidor["labels"], servidor["cpu"], servidor["ram"])
        historico[servidor["id"]] = {label: (cpu, ram) for label, cpu, ram in pontos}
    return historico


# ==============================================================================
# 3. TRANSFORM
# ==============================================================================
def preparar_dados(df):
    """Confere as colunas, converte tipos e ajusta o fuso."""
    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas ausentes no Silver: {faltando}")

    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["mac_address", "created_at"])

    # O MAC identifica o servidor: padroniza (maiúsculas e ":" como separador).
    df["mac_address"] = (
        df["mac_address"].astype(str).str.upper().str.strip().str.replace("-", ":", regex=False)
    )

    # O CSV não tem fuso: dizemos que é o horário local da coleta e convertemos para UTC.
    df["created_at"] = (
        df["created_at"]
        .dt.tz_localize(FUSO_COLETA, ambiguous="NaT", nonexistent="NaT")
        .dt.tz_convert("UTC")
    )
    df = df.dropna(subset=["created_at"])

    for coluna in COLUNAS_METRICAS:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df = df.drop_duplicates(subset=["mac_address", "created_at"])
    print(f"[TRANSFORM] {len(df)} registros válidos.")
    return df


def calcular_blocos(df, agora, desde=None):
    """
    Resume as leituras (de 5 em 5 minutos) em blocos de BLOCO_HORAS horas por servidor:
    CPU usada e RAM usada (meio-termo entre média e mediana, em %).
    Só devolve blocos COMPLETOS: o bloco que ainda está acontecendo fica de fora,
    porque a média parcial puxaria a reta da previsão para um lado só.
    Com 'desde' (início da janela de leitura), blocos que começaram antes dela também ficam
    de fora: só temos parte das leituras deles, e a média parcial estragaria o valor já salvo.
    """
    vazio = pd.DataFrame(columns=["mac_address", "bloco"] + COLUNAS_METRICAS)
    if df.empty:
        return vazio

    df = df.copy()
    local = df["created_at"].dt.tz_convert(FUSO_COLETA)
    df["bloco"] = local.dt.floor(f"{BLOCO_HORAS}h").dt.tz_convert("UTC")
    df = df[df["bloco"] + pd.Timedelta(hours=BLOCO_HORAS) <= agora]
    if desde is not None:
        df = df[df["bloco"] >= desde]
    if df.empty:
        return vazio

    blocos = meio_termo_por_grupo(df, ["mac_address", "bloco"], COLUNAS_METRICAS).reset_index()
    # A página precisa de CPU e RAM em todos os pontos; bloco sem uma das duas é descartado.
    sem_valor = blocos[COLUNAS_METRICAS].isna().any(axis=1)
    if sem_valor.any():
        print(f"[Aviso] {int(sem_valor.sum())} bloco(s) sem CPU ou RAM foram descartados.")
    return blocos[~sem_valor]


def montar_servidores(historico):
    """Transforma {mac: {horario: (cpu, ram)}} na lista 'servidores' que a página espera."""
    servidores = []
    for mac in sorted(historico):
        horarios = sorted(historico[mac])[-PREV_MAX_BLOCOS:]  # texto ISO ordena como data
        if not horarios:
            continue
        servidores.append({
            "id": mac,
            "nome": nome_servidor(mac),
            "labels": horarios,
            "cpu": [historico[mac][h][0] for h in horarios],
            "ram": [historico[mac][h][1] for h in horarios],
            "limites": LIMITES.get(mac, {}),   # {"cpu": ..., "ram": ...}; vazio se não cadastrado
        })
    return servidores


# ==============================================================================
# 4. LOAD
# ==============================================================================
def salvar_json_no_s3(s3_client, dados, chave_s3):
    s3_client.put_object(
        Bucket=BUCKET_GOLD,
        Key=chave_s3,
        Body=json.dumps(dados, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[LOAD] JSON salvo em: s3://{BUCKET_GOLD}/{chave_s3}")


# ==============================================================================
# 5. ORQUESTRAÇÃO / FLUXO PRINCIPAL
# ==============================================================================
def executar_previsao():
    """
    Silver -> blocos de 12 h por servidor -> previsao/<id_da_empresa>.json.

    Os blocos antigos já ficam salvos no próprio JSON. A cada execução só relemos as
    últimas PREV_HORAS_RELEITURA horas do Silver, calculamos os blocos que fecharam e
    juntamos com o que já estava salvo. Na primeira execução relemos PREV_DIAS_INICIAL dias.
    """
    global EMPRESAS
    try:
        if not BUCKET_SILVER or not BUCKET_GOLD:
            raise ValueError("Defina as variáveis S3_BUCKET_SILVER e S3_BUCKET_GOLD.")

        s3_client = boto3.client("s3")
        EMPRESAS = carregar_empresas()
        if not EMPRESAS:
            raise ValueError("Nenhum servidor cadastrado no banco (tabela instancia).")

        agora = pd.Timestamp.now(tz="UTC")
        if os.getenv("MODO_DEMO") == "1":
            # Para testes e apresentação com dados antigos: lê tudo que existe no Silver
            # e trata a leitura mais recente como "agora".
            desde = None
        elif previsao_ja_existe(s3_client):
            desde = agora - pd.Timedelta(hours=PREV_HORAS_RELEITURA)
        else:
            desde = agora - pd.Timedelta(days=PREV_DIAS_INICIAL)
            print(f"[PREVISÃO] Primeira execução: relendo os últimos {PREV_DIAS_INICIAL} dias do Silver.")

        df = carregar_silver(s3_client, desde)
        if df.empty:
            print("[Info] Fluxo interrompido: nenhum dado para processar.")
            return

        df = preparar_dados(df)
        if os.getenv("MODO_DEMO") == "1":
            agora = df["created_at"].max()
            print("[DEMO] MODO_DEMO=1: a leitura mais recente está sendo tratada como 'agora'.")

        blocos = calcular_blocos(df, agora, desde)

        # Um JSON por empresa: cada um só enxerga os servidores da própria empresa.
        for empresa_id in sorted(set(EMPRESAS.values())):
            macs = {mac for mac, e in EMPRESAS.items() if e == empresa_id}

            # o que já estava salvo (só dos servidores que ainda são desta empresa) + blocos novos
            historico = {
                mac: pontos
                for mac, pontos in carregar_previsao_existente(s3_client, empresa_id).items()
                if mac in macs
            }
            for linha in blocos[blocos["mac_address"].isin(macs)].itertuples():
                historico.setdefault(linha.mac_address, {})[iso_utc(linha.bloco)] = (
                    round(float(linha.cpu_use_percent), 1),
                    round(float(linha.ram_use_percent), 1),
                )

            servidores = montar_servidores(historico)
            if servidores:
                salvar_json_no_s3(s3_client, {"servidores": servidores}, chave_da_empresa(empresa_id))
            else:
                print(f"[PREVISÃO] Empresa {empresa_id}: ainda sem blocos completos de {BLOCO_HORAS} h.")

        print("\n[SUCESSO] Pipeline Silver -> Previsão executado com êxito!")

    except (BotoCoreError, ClientError) as err:
        print(f"[ERRO AWS] Falha na comunicação com o serviço S3: {err}")
        raise  # a Lambda registra a execução como ERRO
    except Exception as err:
        print(f"[ERRO PREVISÃO] Falha durante a execução do pipeline: {err}")
        raise


# ==============================================================================
# 6. PONTO DE ENTRADA DA LAMBDA
# ==============================================================================
def lambda_handler(event, context):
    """Chamada pela AWS (novo CSV no Silver ou agendamento)."""
    executar_previsao()
    return {"statusCode": 200, "body": "Previsão atualizada"}


if __name__ == "__main__":
    executar_previsao()