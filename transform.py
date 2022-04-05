from dicionario import get_dicionarios

def get_deter(ano):
    import os
    import time
    import datetime
    import pandas as pd
    import numpy as np
    from paths import path_raw, path_ref

    if not os.path.exists(path_ref):
        os.mkdir(path_ref)

    if not os.path.exists(f"{path_ref}/csv"):
        os.mkdir(f"{path_ref}/csv")

    if not os.path.exists(f"{path_ref}/parquet"):
        os.mkdir(f"{path_ref}/parquet")

    print("Lendo o arquivo RAW")
    df = pd.read_csv(f'{path_raw}/{ano}.csv', delimiter = ';', encoding = 'cp1252', header = 1)

    df.replace("  ", "", regex = True, inplace = True)

    print("Renomeando colunas")
    if ano == 2020:
        df.rename(columns={
            "Destino Seção" : "des_localidade_nome",
            "Municipio Destino" : "des_municipio_nome",
            "Transportadora" : "empresa",
            "Km" : "km",
            "Origem Seção" : "ori_localidade_nome",
            "Municipio Origem" : "ori_municipio_nome",
            "Passag.Estudante" : "pax_jovem_gratis",
            "Passag.Comum" : "pax_pagantes",
            "Passag.Total" : "pax_total",
            "Linha/Ramal" : "prefixo",
            "Seção" : "sequencial",
            "Operacao" : "servico_ambito"}, inplace=True)

        df.drop(["Itinerario", "Tempo", "Vl.Tarifa"], axis = 1, inplace = True)

    else:
        df.rename(columns={
            "DESTINO" : "des_localidade_nome",
            "MUNICIPIO DESTINO" : "des_municipio_nome",
            "TRANSPORTADORA" : "empresa",
            "KM" : "km",
            "ORIGEM" : "ori_localidade_nome",
            "MUNICIPIO ORIGEM" : "ori_municipio_nome",
            "PASSAG.ESTUDANTE" : "pax_jovem_gratis",
            "PASSAG.COMUM" : "pax_pagantes",
            "PASSAG.TOTAL" : "pax_total",
            "LINHA/RAMAL" : "prefixo",
            "SECAO" : "sequencial",
            "OPERACAO" : "servico_ambito"}, inplace=True)
        
        df.drop(["ITINERARIO", "TEMPO"], axis = 1, inplace = True)

    # cria colunas para uso futuro
    print("Criando novas colunas")

    df["ano"] = ano
    df["codigo"] = np.nan
    df["des_localidade_id"] = np.nan
    df["des_localidade_uf"] = np.nan
    df["des_municipio_id"] = np.nan
    df["empresa_cnpj"] = np.nan
    df["empresa_situacao"] = np.nan
    df["empresa_tipo"] = np.nan
    df["ida_idoso_desconto"] = np.nan
    df["ida_idoso_gratis"] = np.nan
    df["ida_jovem_desconto"] = np.nan
    df["ida_jovem_gratis"] = np.nan
    df["ida_pagantes"] = np.nan
    df["ida_passelivre"] = np.nan
    df["km_total"] = np.nan
    df["linha"] = np.nan
    df["linha_id"] = np.nan
    df["lugares_idas"] = np.nan
    df["lugares_voltas"] = np.nan
    df["mes"] = np.nan
    df["ori_localidade_id"] = np.nan
    df["ori_localidade_uf"] = np.nan
    df["ori_municipio_id"] = np.nan
    df["pax_gratis_descontos"] = np.nan
    df["pax_idoso_desconto"] = np.nan
    df["pax_idoso_gratis"] = np.nan
    df["pax_jovem_desconto"] = np.nan
    df["pax_passelivre"] = np.nan
    df["secao_id"] = np.nan
    df["servico_tipo"] = np.nan
    df["sisdap_fim"] = np.nan
    df["sisdap_inicio"] = np.nan
    df["viagem_idas"] = np.nan
    df["viagem_voltas"] = np.nan
    df["volta_idoso_desconto"] = np.nan
    df["volta_idoso_gratis"] = np.nan
    df["volta_jovem_desconto"] = np.nan
    df["volta_jovem_gratis"] = np.nan
    df["volta_pagantes"] = np.nan
    df["volta_passelivre"] = np.nan

    print("Limpando caracteres - str.strip()")
    df["ori_localidade_nome"] = df["ori_localidade_nome"].str.strip()
    df["des_localidade_nome"] = df["des_localidade_nome"].str.strip()
    df["ori_municipio_nome"] = df["ori_municipio_nome"].str.strip()
    df["des_municipio_nome"] = df["des_municipio_nome"].str.strip()

    print("Limpando colunas numéricas - prefixo, sequencial e km")
    df["prefixo"].replace(" ", "", regex = True, inplace = True)
    df["sequencial"].replace(" ", "", regex = True, inplace = True)
    df["km"].replace(" ", "", regex = True, inplace = True)
    df["pax_jovem_gratis"].replace(" ", "", regex = True, inplace = True)
    df["pax_pagantes"].replace(" ", "", regex = True, inplace = True)
    df["pax_total"].replace(" ", "", regex = True, inplace = True)

    print("Substituindo valores em branco por 0 - colunas pax_jovem_gratis, pax_pagantes e pax_total")
    df["pax_jovem_gratis"].replace("", '0', regex = True, inplace = True)
    df["pax_pagantes"].replace("", '0', regex = True, inplace = True)
    df["pax_total"].replace("", '0', regex = True, inplace = True)

    print("Criando coluna ori_des_localidade_nome")
    # cria coluna com origen-destino localidade
    df["ori_des_localidade_nome"] = (df["ori_localidade_nome"] + "-" + df["des_localidade_nome"])

    print("Reindexando colunas por ordem alfabética")
    # reindexa colunas alfabeticamente
    df = df.reindex(sorted(df.columns), axis = 1)

    print("Verificando o número de colunas do dataset")
    if len(df.columns) == 53:
        print(f"A quantidade de colunas ({len(df.columns)} colunas) do dataset está CORRETA.")
    else:
        print(f"A quantidade de colunas ({len(df.columns)} colunas) do dataset está INCORRETA.")

    df.to_csv(f'{path_ref}/csv/{ano}.csv', sep = ';', encoding = 'utf-8', index = False)
    print(f"Gravação no formato CSV efetuada para o dataset de {ano}")

    df.to_parquet(f'{path_ref}/parquet/{ano}.parquet', compression = 'snappy', index = False)
    print(f"Gravação no formato Parquet efetuada para o dataset de {ano}")

    # end
    print(f"Sucesso para o dataset de {ano}\n")
    time.sleep(3)

if __name__ == "__main__":
    anos = range(2000, 2021, 1)
    for ano in anos:
        get_deter(ano)
    get_dicionarios()