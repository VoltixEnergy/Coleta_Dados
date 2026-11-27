import os
import io
import json
from datetime import datetime, timedelta, timezone
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
# É uma Lambda separada da Gold. Ela faz tudo da previsão:
#   1) lê o Silver e resume em blocos de 6 horas (CPU, RAM e disco de cada servidor);
#   2) faz a regressão linear para 3 cenários (24 horas, 3 dias e 7 dias);
#   3) grava o resultado pronto no JSON. A página só mostra o que está no JSON.
# ==============================================================================

# ==============================================================================
# 0. CONFIGURAÇÕES
# ==============================================================================
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")     # a saída fica no bucket Gold, na pasta previsao/

PREFIXO_SILVER = "silver/detalhado/"          # de onde lemos
PASTA_PREVISAO = "previsao/"                  # saída -> previsao/<id_da_empresa>.json

FUSO_COLETA = "America/Sao_Paulo"

BLOCO_HORAS = 6               # cada ponto resume 6 horas (blocos começam às 00h, 06h, 12h e 18h locais); a página agrupa em 6 h, 12 h ou 24 h
PREV_MAX_BLOCOS = 56          # guarda no máximo 56 blocos (14 dias) por servidor
PREV_HORAS_RELEITURA = 30     # a cada execução relê só as últimas 30 h do Silver
PREV_DIAS_INICIAL = 14        # na primeira execução (pasta previsao/ vazia) relê os últimos 14 dias

COLUNAS_OBRIGATORIAS = ["mac_address", "created_at", "cpu_use_percent", "ram_use_percent", "disk_use_percent"]
COLUNAS_METRICAS = ["cpu_use_percent", "ram_use_percent", "disk_use_percent"]

# Componentes da previsão: chave no JSON -> (nome mostrado, nome da métrica no banco)
COMPONENTES = {
    "cpu":   ("CPU",   "cpu_use_percent"),
    "ram":   ("RAM",   "ram_use_percent"),
    "disco": ("Disco", "disk_use_percent"),
}

# Cenários da previsão. Cada um junta os blocos de 6 h em passos maiores (média) para o
# gráfico nunca passar de 10 pontos: historico (pontos medidos) + futuros (pontos previstos).
CENARIOS = {
    "dia":  {"nome": "24 horas", "passo_horas": 6,  "historico": 6, "futuros": 4, "periodo": "as próximas 24 horas"},  # 6 + 4 = 10
    "tres": {"nome": "3 dias",   "passo_horas": 12, "historico": 4, "futuros": 6, "periodo": "os próximos 3 dias"},    # 4 + 6 = 10
    "sete": {"nome": "7 dias",   "passo_horas": 24, "historico": 3, "futuros": 7, "periodo": "os próximos 7 dias"},    # 3 + 7 = 10
}
PONTOS_REGRESSAO = 12   # a regressão usa até os últimos 12 pontos do cenário
LIMITE_PADRAO = 80.0    # usado se o servidor não tiver limite cadastrado na tabela metrica

# Preenchido na execução: a empresa dona de cada servidor (mac -> id da empresa).
EMPRESAS = {}
# Preenchido na execução: limite máximo de cada servidor (mac -> {"cpu": 80.0, "ram": 85.0, "disco": 90.0}).
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
              AND e.nome IN ('cpu_use_percent', 'ram_use_percent', 'disk_use_percent')
              AND m.maximo IS NOT NULL
        """)
        chave_da_metrica = {dados[1]: chave for chave, dados in COMPONENTES.items()}
        for mac, nome_metrica, maximo in cursor.fetchall():
            LIMITES.setdefault(formatar_mac(mac), {})[chave_da_metrica[nome_metrica]] = float(maximo)
    finally:
        conexao.close()
    print(f"[BANCO] {len(empresas)} servidor(es) com empresa cadastrada e {len(LIMITES)} com limites.")
    return empresas


def previsao_ja_existe(s3_client):
    """
    True se já há um JSON no formato atual na pasta previsao/ (se não, é a primeira execução).
    Um JSON do formato antigo (sem a chave "previsao" e sem disco) conta como inexistente:
    assim a Lambda relê os últimos dias do Silver e refaz o histórico com o disco.
    """
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_GOLD, Prefix=PASTA_PREVISAO, MaxKeys=1)
    if resposta.get("KeyCount", 0) == 0:
        return False
    chave = resposta["Contents"][0]["Key"]
    texto = s3_client.get_object(Bucket=BUCKET_GOLD, Key=chave)["Body"].read().decode("utf-8")
    return '"previsao"' in texto


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
        if "disco" not in servidor:
            continue  # JSON do formato antigo (sem disco): esse histórico é refeito a partir do Silver
        pontos = zip(servidor["labels"], servidor["cpu"], servidor["ram"], servidor["disco"])
        historico[servidor["id"]] = {label: (cpu, ram, disco) for label, cpu, ram, disco in pontos}
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
    CPU, RAM e disco usados (meio-termo entre média e mediana, em %).
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
    # A previsão precisa de CPU, RAM e disco em todos os pontos; bloco sem uma delas é descartado.
    sem_valor = blocos[COLUNAS_METRICAS].isna().any(axis=1)
    if sem_valor.any():
        print(f"[Aviso] {int(sem_valor.sum())} bloco(s) sem CPU, RAM ou disco foram descartados.")
    return blocos[~sem_valor]


def montar_servidores(historico):
    """Transforma {mac: {horario: (cpu, ram, disco)}} na lista 'servidores' (o histórico de 6 em 6 h)."""
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
            "disco": [historico[mac][h][2] for h in horarios],
            "limites": LIMITES.get(mac, {}),   # {"cpu": ..., "ram": ..., "disco": ...}; vazio se não cadastrado
        })
    return servidores


# ==============================================================================
# 3.5 PREVISÃO (regressão linear)
# ==============================================================================
def ler_data(texto):
    """'2026-11-22T15:00:00Z' -> datetime em UTC."""
    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def texto_data(data):
    """datetime -> '2026-11-22T15:00:00Z'."""
    return data.strftime("%Y-%m-%dT%H:%M:%SZ")


def regressao_linear(ys):
    """
    Regressão linear simples (mínimos quadrados): y = a + b*x, com x = 0, 1, 2, ...
    Devolve a (onde a reta começa), b (inclinação) e r2 (o quanto a reta explica os dados).
    """
    n = len(ys)
    media_x = (n - 1) / 2
    media_y = sum(ys) / n

    soma_xy = 0.0
    soma_xx = 0.0
    for x in range(n):
        soma_xy += (x - media_x) * (ys[x] - media_y)
        soma_xx += (x - media_x) ** 2
    b = 0.0 if soma_xx == 0 else soma_xy / soma_xx
    a = media_y - b * media_x

    # R² = 1 - (erro da reta / variação total dos dados)
    erro_reta = 0.0
    variacao_total = 0.0
    for x in range(n):
        erro_reta += (ys[x] - (a + b * x)) ** 2
        variacao_total += (ys[x] - media_y) ** 2
    r2 = 0.0 if variacao_total == 0 else 1 - erro_reta / variacao_total
    return a, b, r2


def prever(valores, quantidade):
    """Usa os últimos PONTOS_REGRESSAO valores, estica a reta e devolve (previstos, r2, pontos usados)."""
    janela = valores[-PONTOS_REGRESSAO:]
    a, b, r2 = regressao_linear(janela)
    previstos = []
    for k in range(1, quantidade + 1):
        valor = a + b * (len(janela) - 1 + k)
        previstos.append(round(max(0.0, min(100.0, valor)), 1))  # porcentagem fica entre 0 e 100
    return previstos, r2, len(janela)


def agrupar_servidor(servidor, passo_horas):
    """
    Junta os blocos de 6 h do servidor em blocos maiores (12 h, 24 h), tirando a média.
    Os grupos são contados de trás para frente, a partir do último bloco.
    Se faltar algum bloco no meio, preenche com uma reta entre os vizinhos.
    Devolve {"datas": [...], "cpu": [...], "ram": [...], "disco": [...]}.
    """
    base = timedelta(hours=BLOCO_HORAS)
    passo = timedelta(hours=passo_horas)
    por_grupo = round(passo / base)          # quantos blocos de 6 h cabem em um grupo

    datas = [ler_data(texto) for texto in servidor["labels"]]
    ultimo = datas[-1]

    # k = 0 é o grupo mais novo, k = 1 o anterior, e assim por diante
    grupos = {}
    for i in range(len(datas)):
        k = int((ultimo - datas[i]) / passo)
        if k not in grupos:
            grupos[k] = {"n": 0, "cpu": 0.0, "ram": 0.0, "disco": 0.0}
        grupos[k]["n"] += 1
        for comp in COMPONENTES:
            grupos[k][comp] += servidor[comp][i]

    ks = sorted(grupos, reverse=True)        # do mais antigo ao mais novo
    # o grupo mais antigo costuma vir incompleto: descarta se sobrar outro
    if len(ks) > 1 and grupos[ks[0]]["n"] < por_grupo:
        ks = ks[1:]

    serie = {"datas": [], "cpu": [], "ram": [], "disco": []}
    for k in range(ks[0], -1, -1):
        if k in grupos:
            for comp in COMPONENTES:
                serie[comp].append(grupos[k][comp] / grupos[k]["n"])
        else:
            antigo = k + 1
            while antigo not in grupos:
                antigo += 1
            novo = k - 1
            while novo not in grupos:
                novo -= 1
            fracao = (antigo - k) / (antigo - novo)
            for comp in COMPONENTES:
                valor_antigo = grupos[antigo][comp] / grupos[antigo]["n"]
                valor_novo = grupos[novo][comp] / grupos[novo]["n"]
                serie[comp].append(valor_antigo + (valor_novo - valor_antigo) * fracao)
        serie["datas"].append(ultimo - k * passo - (passo - base))   # início do grupo
    return serie


def media_dos_servidores(series):
    """Série "todos": a média dos servidores, ponto a ponto (cortada no menor tamanho)."""
    n = min(len(serie["datas"]) for serie in series)
    resultado = {"datas": series[0]["datas"][-n:], "cpu": [], "ram": [], "disco": []}
    for i in range(n):
        for comp in COMPONENTES:
            soma = 0.0
            for serie in series:
                soma += serie[comp][len(serie[comp]) - n + i]
            resultado[comp].append(soma / len(series))
    return resultado


def montar_visao(serie, limites, cenario):
    """O que a página precisa para desenhar um gráfico: medidos, previstos, limite e R² de cada componente."""
    passo = timedelta(hours=cenario["passo_horas"])
    ultimo = serie["datas"][-1]

    visao = {
        "labels": [texto_data(d) for d in serie["datas"][-cenario["historico"]:]],
        "labels_previsao": [texto_data(ultimo + passo * k) for k in range(1, cenario["futuros"] + 1)],
    }
    for comp in COMPONENTES:
        previstos, r2, usados = prever(serie[comp], cenario["futuros"])
        visao[comp] = {
            "medido": [round(v, 1) for v in serie[comp][-cenario["historico"]:]],
            "previsto": previstos,
            "limite": round(limites[comp], 1),
            "r2": round(r2, 2),
            "pontos_regressao": usados,
        }
    return visao


def montar_previsao(servidores):
    """
    Monta a previsão pronta de cada cenário. Para cada um:
      - "visoes": um gráfico por servidor (pelo MAC) e um "todos" (a média dos servidores);
      - "alertas": o primeiro momento em que cada servidor VAI passar do limite (CPU, RAM ou disco).
                   Só entra quem hoje está abaixo do limite; quem já passou dele é alerta atual (Gold);
      - "servidores_em_alerta" e "total_servidores" (KPI).
    """
    previsao = {}
    for codigo, cenario in CENARIOS.items():
        series = {}
        limites = {}
        for s in servidores:
            series[s["id"]] = agrupar_servidor(s, cenario["passo_horas"])
            limites[s["id"]] = {comp: s["limites"].get(comp, LIMITE_PADRAO) for comp in COMPONENTES}

        series["todos"] = media_dos_servidores([series[s["id"]] for s in servidores])
        limites["todos"] = {
            comp: sum(limites[s["id"]][comp] for s in servidores) / len(servidores) for comp in COMPONENTES
        }

        visoes = {}
        for chave in series:
            visoes[chave] = montar_visao(series[chave], limites[chave], cenario)

        alertas = []
        for s in servidores:
            visao = visoes[s["id"]]
            for comp, (nome, _) in COMPONENTES.items():
                limite = visao[comp]["limite"]
                if s[comp][-1] >= limite:
                    continue   # já está acima do limite agora: isso é um alerta atual (da Gold), não uma previsão
                for i, valor in enumerate(visao[comp]["previsto"]):
                    if valor >= limite:
                        alertas.append({
                            "data": visao["labels_previsao"][i],
                            "servidor": s["nome"],
                            "mensagem": f"{nome} deve passar de {limite:.0f}% (prev. {valor:.1f}%)",
                        })
                        break   # só o primeiro momento em que passa do limite
        alertas.sort(key=lambda a: a["data"])

        previsao[codigo] = {
            "nome": cenario["nome"],
            "passo_horas": cenario["passo_horas"],
            "periodo": cenario["periodo"],
            "alertas": alertas,
            "servidores_em_alerta": len({a["servidor"] for a in alertas}),
            "total_servidores": len(servidores),
            "visoes": visoes,
        }
    return previsao


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
    Silver -> blocos de 6 h por servidor -> regressão dos 3 cenários -> previsao/<id_da_empresa>.json.

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
                    round(float(linha.disk_use_percent), 1),
                )

            servidores = montar_servidores(historico)
            if servidores:
                dados = {
                    "gerado_em": iso_utc(pd.Timestamp.now(tz="UTC")),
                    "servidores": servidores,               # histórico de 6 em 6 h (é a "memória" da ETL)
                    "previsao": montar_previsao(servidores),  # previsão pronta dos 3 cenários (é o que a página usa)
                }
                salvar_json_no_s3(s3_client, dados, chave_da_empresa(empresa_id))
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