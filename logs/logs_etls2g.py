import os
import io
import json
import pandas as pd
import boto3
import mysql.connector as driver_mysql  
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")

BUCKET_SILVER_BEFORE_PREFIX = "logs/"
BUCKET_SILVER_AFTER_PREFIX = "logs/processed/"

BUCKET_GOLD_PREFIX = "logs/"

def get_data_from_database(connection):
  query = """
    SELECT empresa_id, endereco_mac, nome
    FROM instancia
    WHERE deletado_em IS NULL
      AND status = 1 
  """

  try:
    cursor = connection.cursor()
    cursor.execute(query)
    database_response = cursor.fetchall()
  finally:
    connection.close()

  return database_response

def organize_data_to_dict(data):
  companies_with_instances = dict()

  for line in data:

    company_id = line[0]
    instance_mac_address = line[1]
    # instance_name = line[2]

    if not company_id in companies_with_instances:
      companies_with_instances[company_id] = [instance_mac_address]
    else:
      companies_with_instances[company_id].append(instance_mac_address)

  return companies_with_instances

def load_csvs_from_silver(s3_client):
  print(f"[EXTRACT] Listando '{BUCKET_SILVER_BEFORE_PREFIX}' no bucket '{BUCKET_SILVER}'...")
  
  files = []
  paginator = s3_client.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=BUCKET_SILVER, Prefix=BUCKET_SILVER_BEFORE_PREFIX):
      for obj in page.get("Contents", []):
          if obj["Key"].endswith(".csv"):
              files.append(obj)

  if not files:
      print("[Aviso] Nenhum CSV encontrado no Silver.")
      return pd.DataFrame()

  files.sort(key=lambda obj: obj["LastModified"], reverse=True)

  df_list = []
  for obj in files:
      resposta = s3_client.get_object(Bucket=BUCKET_SILVER, Key=obj["Key"])
      df_list.append(pd.read_csv(io.BytesIO(resposta["Body"].read()), sep=";"))

  df = pd.concat(df_list, ignore_index=True)
  print(f"[EXTRACT] {len(files)} arquivo(s) lido(s), {len(df)} registros.")
  return df

def main_pipeline():
  try:
    connection = driver_mysql.connect(
      host=os.getenv("DB_HOST", "localhost"),
      port=int(os.getenv("DB_PORT", "3306")),
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      database=os.getenv("DB_NAME", "voltix"),
    )
    database_response = get_data_from_database(connection)
  except:
    print("Erro na conexão com o banco de dados")
  
  companies_with_instances = organize_data_to_dict(database_response)

  try:
    s3_client = boto3.client("s3")
    df = load_csvs_from_silver(s3_client)
  except:
    print("Erro na conexão com o bucket")

  if df.empty:
    print("[INFO] Fluxo interrompido: nenhum dado para processar.")
    return
  
  print(df)

def lambda_handler(event, context):
  main_pipeline()
  return {"statusCode": 200, "body": "Gold atualizado"}

if __name__ == "__main__":
  main_pipeline()