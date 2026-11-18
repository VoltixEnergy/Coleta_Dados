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
  df_list = []
  processed_keys = []

  paginator = s3_client.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=BUCKET_SILVER, Prefix=BUCKET_SILVER_BEFORE_PREFIX):
    for obj in page.get("Contents", []):
      key = obj["Key"]

      if key.endswith(".csv"):
        response = s3_client.get_object(Bucket=BUCKET_SILVER, Key=key)
        content = response["Body"].read().decode("utf-8")

        temp_df = pd.read_csv(io.StringIO(content), sep=";", dtype=str)
        df_list.append(temp_df)
        processed_keys.append(key)

  if not df_list:
    print("[Aviso] Nenhum arquivo CSV novo encontrado no prefixo 'logs/'.")
    return pd.DataFrame(), []

  consolidated_df = pd.concat(df_list, ignore_index=True)

  print(f"[EXTRACT] Total de registros brutos extraídos: {len(consolidated_df)}")
  return consolidated_df, processed_keys

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
    consolidated_df, processed_keys = load_csvs_from_silver(s3_client)
  except:
    print("Erro na conexão com o bucket")


def lambda_handler(event, context):
  main_pipeline()
  return {"statusCode": 200, "body": "Gold atualizado"}

if __name__ == "__main__":
  main_pipeline()