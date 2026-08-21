import csv
import datetime
import time
import pandas as pd
import glob
import os


def leitura_dados():

    arquivos = glob.glob('./data/*.csv')

    lista_tabelas = [pd.read_csv(arq, sep=';') for arq in arquivos]

    tabela_final = pd.concat(lista_tabelas, ignore_index=True)

    return tabela_final


tabela_final = leitura_dados()

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

print(tabela_final)

print("Média de RAM:", tabela_final["Memoria%"].mean())
print("Pico de CPU:", tabela_final["CPU%"].max())
print("Média de disco:", tabela_final["Disco%"].mean())