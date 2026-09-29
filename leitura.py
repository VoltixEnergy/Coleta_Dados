import os
import pandas as pd

# 1. Lista todos os ficheiros na pasta atual e filtra apenas os .csv
ficheiros_csv = [
    f for f in os.listdir(".") 
    if f.endswith(".csv") and "dados_completos" not in f
]

print(f"Ficheiros encontrados ({len(ficheiros_csv)}): {ficheiros_csv}")

# 2. Lê e concatena os ficheiros
lista_dataframes = [pd.read_csv(f, sep=";") for f in ficheiros_csv]
df = pd.concat(lista_dataframes, ignore_index=True)

# 3. Tratamento e ordenação
colunas_metricas = [
    "cpu_use_percent",
    "ram_total_gb",
    "ram_free_gb",
    "disk_free_percent",
    "network_sent",
    "network_received",
    "package_drop_total",
]

for col in colunas_metricas:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df["created_at"] = pd.to_datetime(
    df["created_at"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
)
df = df.sort_values(by="created_at").reset_index(drop=True)

df.to_csv("dados_completos.csv", sep=";", index=False)