import pandas as pd
import requests
import numpy as np
import re
import os
import logging

from sqlalchemy import create_engine
from datetime import datetime
from bs4 import BeautifulSoup


def data_collection(url, headers):
    url = 'https://www.zapimoveis.com.br/venda/imoveis/pe+olinda/?pagina=1'
    page = requests.get(url, headers=headers)

    soup = BeautifulSoup(page.text, 'html.parser')

    casas = soup.find_all('h1', class_ = 'js-summary-title summary__title heading-regular heading-regular__bold align-left text-margin-zero results__title')
    texto_casas = [p.get_text('strong') for p in casas]

    numero_casas = texto_casas[0].replace('.', '')
    numero_casas = int(re.search('\d+', numero_casas).group(0))

    numero_paginas = numero_casas/24

    # auxiliares
    pagina = range(1, int(numero_paginas)-2, 1)
    dados = pd.DataFrame()


    for i in pagina:
        url = 'https://www.zapimoveis.com.br/venda/imoveis/pe+olinda/?pagina=' + str(i)
        page = requests.get(url, headers=headers)

        soup = BeautifulSoup(page.text, 'html.parser')

        # ID dos Imoveis
        imoveis_lista = soup.find_all('div', class_ = "card-container js-listing-card")
        identif = [p.get('data-id') for p in imoveis_lista]
        
        # Preco dos Imoveis
        imoveis_lista = soup.find_all('p', class_ = 'simple-card__price js-price color-darker heading-regular heading-regular__bolder align-left')
        preco = [p.get_text() for p in imoveis_lista]
        preco = [p.replace('\n', '') for p in preco]
        preco = [p.replace('R$', '') for p in preco]
        preco = [p.strip() for p in preco]

        # coletando outros dados

        imoveis_lista = soup.find_all('div', class_ = 'simple-card__actions')
        dados_aux = [list(filter(None, p.get_text().split('\n'))) for p in imoveis_lista]
        dados_aux = pd.DataFrame(dados_aux)

        # ajeitando tabela

        dados_aux.columns = ['rua', 'drop', 'metro_quadrado', 'drop', 'quartos', 'drop', 'garagens', 'drop', 'banheiros', 'drop']
        dados_aux = dados_aux[['rua', 'metro_quadrado', 'quartos', 'garagens', 'banheiros']]

        # adicionando ID e preco a tabela
        dados_aux['preco'] = preco
        dados_aux['id'] = identif

        dados = dados.append(dados_aux, ignore_index = True)
        dados['scrapy-datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return dados

def data_cleaning(dados):
    # remover "." no preco
    dados['preco'] = dados['preco'].apply(lambda x: x.replace('.', '') if pd.notnull(x) else x)

    # remover m2 no metro_quadrado
    dados['metro_quadrado'] = dados['metro_quadrado'].apply(lambda x: x.replace('m²', '') if pd.notnull(x) else x)

    # datetime
    dados.rename(columns={'scrapy-datetime':'scrapy_datetime'}, inplace=True)

    return dados

def data_insert(dados):
    data_insert = dados[[
        'rua',
        'metro_quadrado',
        'quartos',
        'garagens',
        'banheiros',
        'preco',
        'id',
        'scrapy_datetime'
    ]]

    # create database connection
    conn = create_engine('sqlite:////home/joaohenritm/repos/Olinda-Imoveis/database/database_imoveis.sqlite', echo=False)

    # data insert
    data_insert.to_sql('imoveis', con=conn, if_exists='append', index=False)

    return None

# url
url = 'https://www.zapimoveis.com.br/venda/imoveis/pe+olinda/?pagina=1'

# parameters and constants
headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'}


if __name__ == '__main__':
    # logging
    path = '/home/joaohenritm/repos/Olinda-Imoveis/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')

    logging.basicConfig(
        filename=path + 'Logs/webscraping_olinda.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s -',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger('webscraping_olinda')

    # parameters and constants
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'}

    # url
    url = 'https://www.zapimoveis.com.br/venda/imoveis/pe+olinda/?pagina=1'

    # Data Collection
    data = data_collection(url, headers)
    logger.info('data collection done')

    # Data Cleaning
    data_product_cleaned = data_cleaning(data)
    logger.info('data product cleaned done')

    # Data Insertion
    data_insert(data_product_cleaned)
    logger.info('data insertion done')