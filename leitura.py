import csv
import datetime
import time
import pandas as pd
import glob

def leitura_dados():

    arquivos = glob.glob('./data/*.csv')

    lista_tabelas = [pd.read_csv(arq, sep=';') for arq in arquivos]

    tabela_final = pd.concat(lista_tabelas, ignore_index=True)

    return tabela_final


tabela_final = leitura_dados()

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

print("\n")

print(tabela_final)

print("\n")

print("Média de RAM:")
print(tabela_final.groupby("Usuario")["Memoria%"].mean().to_string())

print("\n")

print("Pico de CPU:")
print(tabela_final.groupby("Usuario")["CPU%"].max().to_string())

print("\n")

print("Média de disco:")
print(tabela_final.groupby("Usuario")["Disco%"].max().to_string())

print("\n")

print("Máximo de uso de CPU do Bruno:", tabela_final[tabela_final["Usuario"] == "Bruno"]["CPU%"].max())
print("Máximo de uso de CPU do Heitor:", tabela_final[tabela_final["Usuario"] == "Heitor"]["CPU%"].max())
print("Máximo de uso de CPU do Kevin:", tabela_final[tabela_final["Usuario"] == "Kevin"]["CPU%"].max())
print("Máximo de uso de CPU do Ricardo:", tabela_final[tabela_final["Usuario"] == "Ricardo"]["CPU%"].max())
print("Máximo de uso de CPU do Julia:", tabela_final[tabela_final["Usuario"] == "Julia"]["CPU%"].max())
print("Máximo de uso de CPU do Raissa:", tabela_final[tabela_final["Usuario"] == "Raissa"]["CPU%"].max())