import pandas as pd

def get_dicionarios():
    df_secoes = pd.read_csv('dicionario/dicionario_secoes.csv', delimiter = ';', encoding = 'cp1252', header = 0)
    df_secoes["sie"] = df_secoes["sie"].str.strip()
    df_secoes.to_csv('dicionario/dicionario_secoes_stripped.csv', sep = ';', encoding = 'utf-8', index = False)

    df_linhas = pd.read_csv('dicionario/dicionario_linhas.csv', delimiter = ';', encoding = 'cp1252', header = 0)
    df_linhas["sie"] = df_linhas["sie"].str.strip()
    df_linhas.to_csv('dicionario/dicionario_linhas_stripped.csv', sep = ';', encoding = 'utf-8', index = False)