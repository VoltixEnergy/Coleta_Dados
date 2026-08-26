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

print(tabela_final)

print("Média de RAM:", tabela_final["Memoria%"].mean())
print("Pico de CPU:", tabela_final["CPU%"].max())
print("Média de disco:", tabela_final["Disco%"].mean())



print("\n--- Consumo Médio por Usuário ---")
tabela_agrupada_usuarios = tabela_final.groupby("Usuario").mean(numeric_only=True)
print(tabela_agrupada_usuarios)


print("\n--- Usuários com Maior e Menor Uso por Coluna ---")


indice_maximo_memoria = tabela_final["Memoria%"].idxmax()
usuario_maximo_memoria = tabela_final.loc[indice_maximo_memoria, "Usuario"]
valor_maximo_memoria = tabela_final.loc[indice_maximo_memoria, "Memoria%"]

indice_minimo_memoria = tabela_final["Memoria%"].idxmin()
usuario_minimo_memoria = tabela_final.loc[indice_minimo_memoria, "Usuario"]
valor_minimo_memoria = tabela_final.loc[indice_minimo_memoria, "Memoria%"]

print("Memoria% - Maior uso:", usuario_maximo_memoria, "com", valor_maximo_memoria)
print("Memoria% - Menor uso:", usuario_minimo_memoria, "com", valor_minimo_memoria)



indice_maximo_cpu = tabela_final["CPU%"].idxmax()
usuario_maximo_cpu = tabela_final.loc[indice_maximo_cpu, "Usuario"]
valor_maximo_cpu = tabela_final.loc[indice_maximo_cpu, "CPU%"]

indice_minimo_cpu = tabela_final["CPU%"].idxmin()
usuario_minimo_cpu = tabela_final.loc[indice_minimo_cpu, "Usuario"]
valor_minimo_cpu = tabela_final.loc[indice_minimo_cpu, "CPU%"]

print("CPU% - Maior uso:", usuario_maximo_cpu, "com", valor_maximo_cpu)
print("CPU% - Menor uso:", usuario_minimo_cpu, "com", valor_minimo_cpu)



indice_maximo_disco = tabela_final["Disco%"].idxmax()
usuario_maximo_disco = tabela_final.loc[indice_maximo_disco, "Usuario"]
valor_maximo_disco = tabela_final.loc[indice_maximo_disco, "Disco%"]

indice_minimo_disco = tabela_final["Disco%"].idxmin()
usuario_minimo_disco = tabela_final.loc[indice_minimo_disco, "Usuario"]
valor_minimo_disco = tabela_final.loc[indice_minimo_disco, "Disco%"]

print("Disco% - Maior uso:", usuario_maximo_disco, "com", valor_maximo_disco)
print("Disco% - Menor uso:", usuario_minimo_disco, "com", valor_minimo_disco)
