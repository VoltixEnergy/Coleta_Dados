import os
import io
import json
import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError



# ==============================================================================
# 0. CONFIGURAÇÕES
# ==============================================================================

BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")

PREFIXO_SILVER = "silver/detalhado/"   # de onde lemos
PASTA_GOLD = "gold/"                   # dashboard de cada empresa -> gold/<id_da_empresa>.json
PASTA_ALERTAS = "alerts/"              # alertas de cada empresa  -> alerts/<id_da_empresa>.json
CHAVE_LIMITES = "config/limites.json"  # mínimo/máximo do banco (gerado pelo exportar_limites.py)

FUSO_COLETA = "America/Sao_Paulo"

JANELA_HORAS = 1          # quanto tempo para trás o gráfico mostra
BUCKET_MINUTOS = 5        # cada ponto do gráfico resume 5 minutos
INATIVO_APOS_MIN = 30     # sem leitura há mais que isso = servidor inativo
MAX_ARQUIVOS_SILVER = 20 # lê no máximo os 200 arquivos mais recentes do Silver

# Métricas que têm mínimo e máximo no banco:
#   coluna do CSV: (nome exibido, unidade, mínimo padrão, máximo padrão)
# Os padrões só valem para um MAC que não está no banco.
METRICAS = {
    "cpu_use_percent":    ("CPU", "%", 0, 100),
    "ram_free_gb":        ("RAM livre", "GB", 0, None),  
    "disk_free_percent":  ("Disco livre", "%", 0, 100),
    "network_sent":       ("Rede enviada", "MB", 0, 1000),
    "network_received":   ("Rede recebida", "MB", 0, 1000),
    "package_drop_total": ("Pacotes perdidos", "un", 0, 100),
}

# Preenchido na execução com o conteúdo de config/limites.json:

LIMITES = {}

# Preenchido na execução: a empresa dona de cada servidor.   


EMPRESAS = {}


# ==============================================================================
# 1. FUNÇÕES AUXILIARES
# ==============================================================================
def meio_termo(serie):
    """
    Ponto médio entre a MÉDIA e a MEDIANA.
    A média sofre com picos isolados; a mediana ignora o comportamento extremo.
    O meio-termo entre as duas é mais estável e mais fiel ao que o usuário vê.
    """
    return (serie.mean() + serie.median()) / 2


def meio_termo_por_grupo(df, chaves, colunas):
    """Mesma ideia do meio_termo, mas calculada para cada grupo (cada servidor)."""
    grupos = df.groupby(chaves)[colunas]
    return (grupos.mean() + grupos.median()) / 2


def decimal(valor):
    """Número com até 2 casas (para MB). Valor vazio (NaN) vira 0."""
    return 0 if pd.isna(valor) else round(float(valor), 2)


def inteiro(valor):
    """Arredonda para inteiro. Valor vazio (NaN) vira 0."""
    return 0 if pd.isna(valor) else int(round(float(valor)))


def numero(valor):
    """Número para texto, no formato brasileiro: 95,3 e 80."""
    texto = f"{float(valor):.1f}"
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto.replace(".", ",")


def iso_utc(timestamp):
    """Formata como 2026-10-10T15:00:00Z."""
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def nome_servidor(mac_address):
    """Nome mostrado no dashboard: o próprio MAC, que é o que identifica o servidor."""
    return str(mac_address)


# ==============================================================================
# 2. EXTRACT
# ==============================================================================
def carregar_silver(s3_client):
    """Lê os CSVs detalhados mais recentes da pasta 'silver/detalhado/'."""
    print(f"[EXTRACT] Listando '{PREFIXO_SILVER}' no bucket '{BUCKET_SILVER}'...")

    arquivos = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_SILVER, Prefix=PREFIXO_SILVER):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                arquivos.append(obj)

    if not arquivos:
        print("[Aviso] Nenhum CSV encontrado no Silver.")
        return pd.DataFrame()

    # Os mais recentes primeiro; ficamos só com os últimos N para não crescer sem limite
    arquivos.sort(key=lambda obj: obj["LastModified"], reverse=True)
    arquivos = arquivos[:MAX_ARQUIVOS_SILVER]

    lista_dfs = []
    for obj in arquivos:
        resposta = s3_client.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])
        lista_dfs.append(pd.read_csv(io.BytesIO(resposta["Body"].read()), sep=";"))

    df = pd.concat(lista_dfs, ignore_index=True)
    print(f"[EXTRACT] {len(arquivos)} arquivo(s) lido(s), {len(df)} registros.")
    return df


def carregar_limites(s3_client):
    """
    Lê do config/limites.json (gravado pelo exportar_limites.py):
      - o mínimo e o máximo de cada métrica de cada servidor;
      - a empresa dona de cada servidor.
    Devolve (limites, empresas). Se o arquivo ainda não existe, devolve dois dicionários vazios.
    """
    try:
        resposta = s3_client.get_object(Bucket=BUCKET_GOLD, Key=CHAVE_LIMITES)
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            print(
                f"[Aviso] Limites não encontrados em s3://{BUCKET_GOLD}/{CHAVE_LIMITES}. "
                "Rode o exportar_limites.py. Sem eles, as notas usam faixas padrão e não há alertas."
            )
            return {}, {}
        raise

    dados = json.loads(resposta["Body"].read().decode("utf-8"))
    print(f"[LIMITES] {len(dados['limites'])} servidor(es) no banco (gerado em {dados.get('gerado_em')}).")
    return dados["limites"], dados.get("empresas", {})


# ==============================================================================
# 3. TRANSFORM
# ==============================================================================
COLUNAS_OBRIGATORIAS = [
    "mac_address", "created_at",
    "cpu_use_percent", "ram_use_percent", "ram_free_gb", "ram_total_gb",
    "disk_use_percent", "disk_free_percent",
    "network_sent", "network_received", "package_drop_total",
]


def metricas_disponiveis(df):
    """Colunas numéricas usadas nos cálculos."""
    return [c for c in COLUNAS_OBRIGATORIAS if c not in ("mac_address", "created_at")]


def preparar_dados(df):
    """Confere as colunas, converte tipos e ajusta o fuso."""
    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        raise ValueError(f"Colunas ausentes no Silver: {faltando}")

    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["mac_address", "created_at"])

    # O MAC identifica o servidor: padroniza (maiúsculas e ":" como separador)
    # para "e0-d5-..." e "E0:D5:..." caírem no mesmo grupo.
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

    for coluna in metricas_disponiveis(df):
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df = df.drop_duplicates(subset=["mac_address", "created_at"])
    print(f"[TRANSFORM] {len(df)} registros válidos.")
    return df


def faixa(m, metrica):
    """
    Mínimo e máximo da métrica para cada linha de m: vêm do banco (por MAC)
    ou, se o MAC não está no banco, do padrão da tabela METRICAS.
    """
    _, _, padrao_minimo, padrao_maximo = METRICAS[metrica]
    minimos, maximos = [], []
    for mac in m.index.get_level_values("mac_address"):
        dados = LIMITES.get(mac, {}).get(metrica, {})
        minimos.append(dados.get("minimo", padrao_minimo))
        maximos.append(dados.get("maximo", padrao_maximo))

    minimo = pd.Series(minimos, index=m.index, dtype=float)
    maximo = pd.Series(maximos, index=m.index, dtype=float)
    if metrica == "ram_free_gb":
        maximo = m["ram_total_gb"]  # a RAM livre nunca passa da RAM total da máquina
    return minimo, maximo


def nota(valor, minimo, maximo, sobe_com_valor):
    """
    Transforma o valor em uma nota de 0 a 100 usando a faixa [mínimo, máximo].
    sobe_com_valor=True : no mínimo vale 0 e no máximo vale 100.
    sobe_com_valor=False: no mínimo vale 100 e no máximo vale 0.
    """
    largura = (maximo - minimo).where(maximo > minimo)  # faixa inválida vira NaN (e é ignorada)
    posicao = ((valor - minimo) / largura).clip(0, 1)   # 0 = no mínimo, 1 = no máximo
    return (posicao if sobe_com_valor else 1 - posicao) * 100


def calcular_scores(m):
    """
    Recebe métricas já resumidas (uma linha por servidor, ou por servidor e bucket)
    e acrescenta duas notas de 0 a 100. Cada métrica é medida dentro da faixa
    [mínimo, máximo] do banco:

    - termometro = média de: CPU usada, RAM usada e disco usado.
                   MAIOR = MAIS SOBRECARREGADO (pior).
    - eficiencia = média de: ausência de perda de pacotes e equilíbrio entre os bytes
                   que entram e os bytes que saem.
                   MAIOR = MAIS EFICIENTE (melhor).
    """
    m = m.copy()

    def nota_da(metrica, sobe_com_valor):
        minimo, maximo = faixa(m, metrica)
        return nota(m[metrica], minimo, maximo, sobe_com_valor)

    m["termometro"] = pd.concat(
        [
            nota_da("cpu_use_percent", True),    # mais CPU usada = mais quente
            nota_da("ram_free_gb", False),       # menos RAM livre = mais quente
            nota_da("disk_free_percent", False), # menos disco livre = mais quente
        ],
        axis=1,
    ).mean(axis=1)

    # Equilíbrio da rede: menor valor / maior valor entre bytes enviados e recebidos.
    #   100 = entrada e saída iguais; perto de 0 = só entra ou só sai.
    # Sem nenhum tráfego não há desequilíbrio, então a nota é 100.
    menor = m[["network_sent", "network_received"]].min(axis=1)
    maior = m[["network_sent", "network_received"]].max(axis=1)
    equilibrio = (menor / maior.where(maior > 0)).fillna(1.0) * 100
    equilibrio = equilibrio.where(m[["network_sent", "network_received"]].notna().all(axis=1))

    m["eficiencia"] = pd.concat(
        [nota_da("package_drop_total", False), equilibrio], axis=1
    ).mean(axis=1)

    return m


def montar_graficos(df, metricas, agora):
    """
    Série temporal para os dois gráficos: um ponto a cada BUCKET_MINUTOS, cobrindo
    JANELA_HORAS. Em cada ponto: meio-termo das leituras de cada servidor (MAC) e, depois,
    meio-termo entre os servidores. Pontos sem nenhuma leitura valem 0 (como no exemplo).

    O gráfico termina na última leitura que temos.
    """
    df = df.copy()
    freq = f"{BUCKET_MINUTOS}min"
    df["bucket"] = df["created_at"].dt.floor(freq)

    fim = max(df["bucket"].max(), (agora - pd.Timedelta(minutes=INATIVO_APOS_MIN)).floor(freq))
    inicio = fim - pd.Timedelta(hours=JANELA_HORAS)
    grade = pd.date_range(inicio, fim, freq=freq)

    df = df[(df["bucket"] >= inicio) & (df["bucket"] <= fim)]
    if df.empty:
        serie = pd.DataFrame(0.0, index=grade, columns=["termometro", "eficiencia"])
    else:
        por_servidor = meio_termo_por_grupo(df, ["bucket", "mac_address"], metricas)
        por_servidor = calcular_scores(por_servidor).reset_index()

        grupos = por_servidor.groupby("bucket")[["termometro", "eficiencia"]]
        serie = ((grupos.mean() + grupos.median()) / 2).reindex(grade).fillna(0)

    labels = [iso_utc(ts) for ts in serie.index]
    return {
        "thermometer_health": {"labels": labels, "data": [inteiro(v) for v in serie["termometro"]]},
        "server_efficiency": {"labels": labels, "data": [inteiro(v) for v in serie["eficiencia"]]},
    }


def resumir_instancias(df, metricas, agora):
    """
    Uma linha por servidor (MAC), com as métricas dos últimos BUCKET_MINUTOS minutos dele
    (meio-termo, para um pico isolado não assustar) e se está ativo ou não.
    """
    df = df.copy()
    df["ultima_leitura"] = df.groupby("mac_address")["created_at"].transform("max")
    recentes = df[df["created_at"] > df["ultima_leitura"] - pd.Timedelta(minutes=BUCKET_MINUTOS)]

    m = meio_termo_por_grupo(recentes, ["mac_address"], metricas)
    m = calcular_scores(m)
    m["ultima_leitura"] = df.groupby("mac_address")["created_at"].max()
    m["ativa"] = (agora - m["ultima_leitura"]) <= pd.Timedelta(minutes=INATIVO_APOS_MIN)
    return m.sort_index()


def montar_alertas(m):
    """
    Gera um alerta quando uma métrica de um servidor ATIVO sai da faixa [mínimo, máximo]
    cadastrada no banco. O metric_id é o id da métrica (tabela metrica) daquele servidor.
    """
    alertas = []
    ativos = m[m["ativa"]]
    sem_cadastro = set()

    for metrica, (nome, unidade_padrao, _, _) in METRICAS.items():
        minimo, maximo = faixa(ativos, metrica)

        for mac in ativos.index:
            dados = LIMITES.get(mac, {}).get(metrica)
            if dados is None:
                sem_cadastro.add(mac)  # sem metrica.id não dá para montar o alerta
                continue

            valor = ativos.at[mac, metrica]
            limite_min, limite_max = minimo[mac], maximo[mac]
            largura = limite_max - limite_min
            if pd.isna(valor) or pd.isna(largura) or largura <= 0:
                continue

            if valor > limite_max:
                lado, frase, limite = "acima do máximo", "ultrapassou o limite máximo", limite_max
            elif valor < limite_min:
                lado, frase, limite = "abaixo do mínimo", "ficou abaixo do limite mínimo", limite_min
            else:
                continue

            unidade = dados.get("unidade", unidade_padrao)
            alertas.append({
                "title": f"{nome} {frase}",
                "description": (
                    f"{nome_servidor(mac)}: {nome} em {numero(valor)} {unidade}, "
                    f"{lado} configurado ({numero(limite)} {unidade})."
                ),
                "metric_id": int(dados["metrica_id"]),
                "created_at": iso_utc(ativos.at[mac, "ultima_leitura"]),
                "_servidor": mac,  # só para contar; removido depois
            })

    if sem_cadastro:
        print(f"[Aviso] Servidores sem métricas no banco (alertas ignorados): {sorted(sem_cadastro)}")

    # Mais recentes primeiro
    alertas.sort(key=lambda a: a["created_at"], reverse=True)
    return alertas


def montar_dashboard(df, agora):
    """
    Devolve dois dicionários:
      - dashboard: o que o dashboard consome (chave "dashboard")
      - alertas:   o que o envio de alertas consome (chave "alert_sender_data")
    """
    metricas = metricas_disponiveis(df)

    graficos = montar_graficos(df, metricas, agora)
    m = resumir_instancias(df, metricas, agora)
    alertas = montar_alertas(m)

    instancias = []
    for mac, linha in m.iterrows():
        ativa = bool(linha["ativa"])
        instancias.append({
            "name": nome_servidor(mac),
            "isActive": ativa,
            "cpu_percent": inteiro(linha["cpu_use_percent"]) if ativa else 0,
            "ram_percent": inteiro(linha["ram_use_percent"]) if ativa else 0,
            "disk_percent": inteiro(linha["disk_use_percent"]) if ativa else 0,
            "network_sent": decimal(linha["network_sent"]) if ativa else 0,
            "network_received": decimal(linha["network_received"]) if ativa else 0,
            "package_drop": inteiro(linha["package_drop_total"]) if ativa else 0,
            "lastest_update": iso_utc(linha["ultima_leitura"]),
        })

    ativas = m[m["ativa"]]
    eficiencias = ativas["eficiencia"].dropna()
    kpis = {
        "count_server": int(len(m)),
        "count_server_active": int(len(ativas)),
        "count_server_alert": len({a["_servidor"] for a in alertas}),
        # maior nota do termômetro (o servidor mais "quente") entre os servidores ativos
        "server_termometro": inteiro(ativas["termometro"].max()) if len(ativas) else 0,
        # MAC do servidor ativo com a menor eficiência (null se não há servidor ativo)
        "server_efficiency": eficiencias.idxmin() if len(eficiencias) else None,
    }

    for alerta in alertas:
        del alerta["_servidor"]

    dashboard = {"dashboard": {"charts": graficos, "instances": instancias, "kpis": kpis}}
    return dashboard, {"alert_sender_data": alertas}


# ==============================================================================
# 4. LOAD
# ==============================================================================
def chave_da_empresa(empresa_id):
    """gold/<id_da_empresa>.json (dashboard)"""
    return f"{PASTA_GOLD}{empresa_id}.json"


def chave_alertas_da_empresa(empresa_id):
    """alerts/<id_da_empresa>.json (envio de alertas)"""
    return f"{PASTA_ALERTAS}{empresa_id}.json"


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
def executar_gold():
    """Silver (CSVs detalhados) + limites do banco -> cálculos -> Gold (dashboard + alertas, um JSON de cada por empresa)."""
    global LIMITES, EMPRESAS
    try:
        if not BUCKET_SILVER or not BUCKET_GOLD:
            raise ValueError("Defina as variáveis S3_BUCKET_SILVER e S3_BUCKET_GOLD.")

        s3_client = boto3.client("s3")

        LIMITES, EMPRESAS = carregar_limites(s3_client)
        if not EMPRESAS:
            raise ValueError(
                "O arquivo de limites não tem as empresas. Rode o exportar_limites.py "
                "(versão nova) para gravar config/limites.json."
            )

        df = carregar_silver(s3_client)
        if df.empty:
            print("[Info] Fluxo interrompido: nenhum dado para processar.")
            return

        df = preparar_dados(df)

        agora = pd.Timestamp.now(tz="UTC")
        ultima_leitura = df["created_at"].max()
        if os.getenv("MODO_DEMO") == "1":
            # Para testes e apresentação com dados antigos: a leitura mais recente vira o "agora".
            agora = ultima_leitura
            print("[DEMO] MODO_DEMO=1: a leitura mais recente está sendo tratada como 'agora'.")
        elif agora - ultima_leitura > pd.Timedelta(minutes=INATIVO_APOS_MIN):
            print(
                f"[Aviso] Última leitura: {iso_utc(ultima_leitura)}. Agora: {iso_utc(agora)}. "
                f"Sem dados novos há mais de {INATIVO_APOS_MIN} min: todos os servidores "
                "aparecem como inativos, com valores 0."
            )

        # Servidores que não estão no banco não têm empresa: não dá para saber em qual JSON entram.
        df["empresa_id"] = df["mac_address"].map(EMPRESAS)
        sem_empresa = sorted(df.loc[df["empresa_id"].isna(), "mac_address"].unique())
        if sem_empresa:
            print(f"[Aviso] Servidores sem empresa no banco (ignorados): {sem_empresa}")
        df = df.dropna(subset=["empresa_id"])
        if df.empty:
            print("[Info] Nenhum servidor com empresa cadastrada: nada foi gravado.")
            return

        # Um JSON por empresa: cada um só enxerga os servidores da própria empresa.
        for empresa_id, df_empresa in df.groupby("empresa_id"):
            dashboard, alertas = montar_dashboard(df_empresa.drop(columns="empresa_id"), agora)
            salvar_json_no_s3(s3_client, dashboard, chave_da_empresa(int(empresa_id)))
            salvar_json_no_s3(s3_client, alertas, chave_alertas_da_empresa(int(empresa_id)))

        print("\n[SUCESSO] Pipeline Silver -> Gold executado com êxito!")

    except (BotoCoreError, ClientError) as err:
        print(f"[ERRO AWS] Falha na comunicação com o serviço S3: {err}")
        raise  # a Lambda registra a execução como ERRO
    except Exception as err:
        print(f"[ERRO GOLD] Falha durante a execução do pipeline: {err}")
        raise


# ==============================================================================
# 6. PONTO DE ENTRADA DA LAMBDA
# ==============================================================================
def lambda_handler(event, context):
    """Chamada pela AWS quando um CSV novo chega em silver/detalhado/."""
    executar_gold()
    return {"statusCode": 200, "body": "Gold atualizado"}


if __name__ == "__main__":
    executar_gold()