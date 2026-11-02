"""
Exporta o MÍNIMO e o MÁXIMO de cada métrica do banco MySQL 'voltix' para o S3.

A Lambda Gold não conversa com o banco: ela lê o arquivo que este script grava em
s3://<S3_BUCKET_GOLD>/config/limites.json

"""
import os
import json
import datetime
import boto3
import mysql.connector as driver_mysql  


try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")
CHAVE_LIMITES = "config/limites.json"

CONSULTA = """
SELECT i.endereco_mac,
       i.empresa_id,
       m.id AS metrica_id,
       e.nome AS metrica,
       m.minimo,
       m.maximo,
       m.unidade_medida
FROM metrica m
JOIN especificacao e ON e.id = m.especificacao_id
JOIN instancia i ON i.id = m.instancia_id
WHERE m.deletado_em IS NULL
  AND i.deletado_em IS NULL
"""


def formatar_mac(mac):
    """E0D55DAA58E3 -> E0:D5:5D:AA:58:E3 (o mesmo formato que a Gold usa)."""
    if isinstance(mac, (bytes, bytearray)):
        mac = mac.decode()
    mac = str(mac).replace(":", "").replace("-", "").upper()
    return ":".join(mac[i:i + 2] for i in range(0, len(mac), 2))


def buscar_limites():
    """
    Consulta o banco e devolve dois dicionários:
      limites  = {mac: {metrica: {metrica_id, minimo, maximo, unidade}}}
      empresas = {mac: id_da_empresa}
    """
    conexao = driver_mysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "voltix"),
    )
    try:
        cursor = conexao.cursor()
        cursor.execute(CONSULTA)
        linhas = cursor.fetchall()
    finally:
        conexao.close()

    limites, empresas = {}, {}
    for mac, empresa_id, metrica_id, metrica, minimo, maximo, unidade in linhas:
        mac = formatar_mac(mac)
        empresas[mac] = int(empresa_id)
        limites.setdefault(mac, {})[metrica] = {
            "metrica_id": int(metrica_id),
            "minimo": float(minimo),
            "maximo": float(maximo),
            "unidade": unidade,
        }
    return limites, empresas


def enviar_para_s3(limites, empresas):
    conteudo = {
        "gerado_em": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "empresas": empresas,
        "limites": limites,
    }
    boto3.client("s3").put_object(
        Bucket=BUCKET_GOLD,
        Key=CHAVE_LIMITES,
        Body=json.dumps(conteudo, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[S3] Limites salvos em s3://{BUCKET_GOLD}/{CHAVE_LIMITES}")


def main():
    if not BUCKET_GOLD:
        raise SystemExit("Defina a variável S3_BUCKET_GOLD.")

    limites, empresas = buscar_limites()
    if not limites:
        raise SystemExit("Nenhuma métrica encontrada no banco: nada foi enviado.")

    for mac, metricas in limites.items():
        print(f"\n{mac} (empresa {empresas[mac]})")
        for nome, dados in metricas.items():
            print(f"  [{dados['metrica_id']}] {nome}: {dados['minimo']} a {dados['maximo']} {dados['unidade']}")

    enviar_para_s3(limites, empresas)


if __name__ == "__main__":
    main()