## Extração dos dados

import pandas as pd
import requests
from rocketry import Rocketry
from rocketry.conds import cron
from datetime import datetime as dt
import psutil, csv, os, logging
app = Rocketry()
### Listar as UF's

def get_uf_list():
    response = requests.get('http://servicodados.ibge.gov.br/api/v1/localidades/estados')

    if response.status_code == 200:
        uf_metadata = response.json()
        uf_siglas = [ uf['sigla'] for uf in uf_metadata ]
        
        return uf_siglas
    else:
        raise Exception('Não foi possível obter os dados dos estados brasileiros.')


# ### Área de cada Estado

def get_metadados_estado(uf:str):
    url = f'http://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}/metadados'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None 


def get_uf_area(lista_ufs_siglas:list):
    uf_area = {}

    for uf in lista_ufs_siglas:
        metadados = get_metadados_estado(uf)
        uf_metadado_area = metadados[0]['area']['dimensao']
        uf_area[uf] = uf_metadado_area

    return uf_area


# ### Lista de cidades por Estado

def get_metadados_municipio(uf:str):
    url = f'http://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios'

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return None


def get_municipios(lista_ufs_siglas:list):
    ufs_municipios = {}
    for uf in lista_ufs_siglas:
        metadados = get_metadados_municipio(uf)
        ufs_municipio = []
        for municipio in metadados:
            ufs_municipio.append(municipio['nome'])
        ufs_municipios[uf] = ufs_municipio
    return ufs_municipios 

@app.task(cron('03 17 30 04'))
def task():

    lista_ufs_siglas = get_uf_list()
    lista_ufs_area = get_uf_area(lista_ufs_siglas)
    lista_ufs_municipios = get_municipios(lista_ufs_siglas)

    # ## Transformação dos Dados
    # ### Dados de área por UF

    # Transformando em DataFrame e convertendo os tipos
    uf_area_df = pd.DataFrame(list(lista_ufs_area.items()), columns=['UF','Area'])

    # Conversão dos tipos de dados
    uf_area_df['Area'] = uf_area_df['Area'].astype(float)

    # Ordenação por área
    uf_area_df_ordenada = uf_area_df.sort_values(by='Area', ascending=False)

    # ### Lista de Municipios por UF
    # 
    lista_ufs_municipios_df = pd.DataFrame(lista_ufs_municipios.items(), columns=['UF', 'Municipios'])
    lista_ufs_municipios_df_exploded = lista_ufs_municipios_df.explode('Municipios')

    # # Parte 3: Salvando os dados
    # Salvando lista de UF's em .csv

    uf_area_df_ordenada.to_csv('./data/uf_area.csv', index=False, sep=';', encoding='utf-8')

    # Salvando lista de UF's em Parket particionado por UF
    lista_ufs_municipios_df_exploded.to_parquet('./data/parket/municipios/', index=False, partition_cols=['UF'])

if __name__ == '__main__':
    app.run()