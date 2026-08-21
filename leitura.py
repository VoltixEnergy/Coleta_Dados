import csv
import datetime
import time
import pandas as pd
import glob

def leitura_dados():

    arquivos = glob.glob('./data/*.csv')
    lista_tabelas = [pd.read_csv(arq) for arq in arquivos]
    tabela_final = pd.concat(lista_tabelas, ignore_index=True)

leitura_dados()