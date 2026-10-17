import os
import datetime
from dotenv import load_dotenv
import boto3
import io
import json
import pandas as pd
import mysql.connector

load_dotenv()

BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")

DB_CONFIG = {
  "host": os.getenv("DB_HOST"),
  "database": os.getenv("DB_DATABASE"),
  "user": os.getenv("DB_USER"),
  "password": os.getenv("DB_PASSWORD"),
  "port": int(os.getenv("DB_PORT", 3306)),
}

def get_data_to_transform(s3_client):
  print(f"[EXTRACT] Lendo arquivos no bucket '{BUCKET_SILVER}'...")

  df_list = []
  last_file = ""
  last_read = {
    "last_file": "0"
  }

  try:
    res_chk = s3_client.get_object(Bucket=BUCKET_SILVER, Key="checkpoint.json")
    chk = json.loads(res_chk["Body"].read())
    last_read["last_file"] = chk.get("last_file", "0")
  except Exception:
    pass

  paginator = s3_client.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=BUCKET_SILVER, Prefix="silver/detalhado/"):
    for obj in page.get("Contents", []):
      key = obj["Key"]
      last_file = key

      if key > last_read["last_file"] and key.endswith(".csv"):
        response = s3_client.get_object(
          Bucket=BUCKET_SILVER,
          Key=key
        )
        
        res_content = response["Body"].read().decode("utf-8")
        df_response = pd.read_csv(io.StringIO(res_content), sep=";", dtype=str)
        df_list.append(df_response)

  return pd.concat(df_list, ignore_index=True), last_file

# agrupar por mac_address e buscar, pegar campos com 'na' (p/ não buscar no banco) e buscar mínimo 
# e máximo de cada coluna para fazer a parametrização na condição de cada componente
# def get_alerts(df):
#   select m.minimo, m.maximo, e.nome
# 	from metrica m
# 		join instancia i 
# 		on m.instancia_id = i.id
# 			join especificacao e
#             on m.especificacao_id = e.id
# where 
# 	upper(i.endereco_mac)  = "10B5886E4DE9"
#     and e.nome = "cpu_use_percent"
#     and i.deletado_em is null
#     and m.deletado_em is null
#     and i.status_instancia = 1;

def save_last_read(s3_client, last_file):
  buffer = io.StringIO()

  dict_last_file = {
    "last_file": last_file
  }

  json.dump(dict_last_file, buffer, indent=2)
  # s3_client.put_object(
  #   Bucket=BUCKET_SILVER,
  #   Key="checkpoint.json",
  #   Body=buffer.getvalue(),
  #   ContentType="text/json",
  # )

  print(f"[LOAD] Arquivo checkpoint.json salvo com sucesso")

def init():
  try:
    s3_client = boto3.client("s3")

    df_to_tranform, last_file = get_data_to_transform(s3_client)
    if len(df_to_tranform) == 0:
      print("[WARNING] Não há dados há serem transformados")
      return
    
    print(df_to_tranform)

    response = {
      "alert_sender_data": [],
      "dashboard": {}
    }

    # alerts = get_alerts(df_to_tranform)
    # response["alert_sender_data"] = alerts

    save_last_read(s3_client, last_file)

  except Exception as e:
    print(f"[ERROR] {e}")
  
if __name__ == "__main__":
  init()