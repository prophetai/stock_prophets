#-*- coding: utf-8 -*-
import logging
from pandas_datareader import data
import pandas as pd
import datetime
from utils.extract import db_connection, download_data
import json
import numpy as np


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.DEBUG,
    #filename='log.txt'
)

lista_tickers = pd.read_csv('tickers.csv')[['Ticker']]

with open('creds.txt', encoding='utf-8') as data_file:
    creds = json.loads(data_file.read())

now = datetime.datetime.now()



def get_last_date(table_name, creds):
    """
    Trae la fecha del último precio guardado
    """
    try:
        query = "SELECT MAX(date) FROM {}".format(table_name)
        conn = db_connection(creds)
        df = download_data(conn, query)
        latest_date = str(df[0][0])
    except Exception as e:
        logging.error('Error sacando la última fecha de %s: %s' % (table_name, e))
        lates_date = '2010-01-01'

    return latest_date

def create_new_stock_table(table_name, creds):
    """
    Hace una nueva tabla de stocks según el ticker que le pongas

    Args:
        table_name(str): nombre del ticker
        conn(connection object): objeto de connección a la base de datos
    """
    conn = db_connection(creds)
    query = """CREATE TABLE IF NOT EXISTS {} (id SERIAL,
    date date NOT NULL UNIQUE,
    high float,
    low float,
    open float,
    close float,
    volume float,
    adj_close float, PRIMARY KEY (id));""".format(table_name)
    download_data(conn, query)

def update_in_db(df, table_name, creds):

    conn = db_connection(creds)
    matrix = np.array(df.to_records().view(type=np.matrix))[0]
    data = []

    for i in range(len(matrix)):
        conv_date = pd.to_datetime(matrix[i][0])
        date = "('" + str(conv_date.year) + "-" + str(conv_date.month) + "-" + str(conv_date.day) + "')::date"
        High = str(matrix[i][1])
        Low = str(matrix[i][2])
        Open = str(matrix[i][3])
        Close = str(matrix[i][4])
        Volume = str(matrix[i][5])
        Adj_Clos = str(matrix[i][6])
        prices = "(" + date + ", " + High + ", " + Low + ", " + Open + ", " + Close + ", " + Volume + "," + Adj_Clos +")"
        data.append(prices)

    data = str(data).replace("[", "(").replace("]", ")").replace('(', '', 1)[:-1].replace('"','')
    table_name = table_name.replace('-','_')
    query = """INSERT INTO {} (date, high, low, open, close, volume, adj_close) VALUES{} ON CONFLICT ON CONSTRAINT {}_date_key DO NOTHING;""".format(table_name.upper(), data, table_name.lower())

    try:
        download_data(conn, query)
        logging.info("Se guardó: {}".format(ticker))
    except Exception as error:
        logging.error("Error al tratar de insertar %s: %s" % (table_name,error))


# In[103]:


# User pandas_reader.data.DataReader to load the desired data. As simple as that.
def get_stock_data(lista_tickers, creds, previous_date=False):
    """
    Obtiene los datos de precios de acciones y los guarda en la base de datos

    Args:
        lista_tickers(list): lista de strings con los nombres de los tickers como vienen en Yahoo Finance
        end_date(string): String que indica la fecha de hasta donde recolectar datos
        creds(dict): diccionario de credenciales de la base de datos
    """
    end_date = '{}-{}-{}'.format(now.year, now.month, now.day)
    for ticker in lista_tickers['Ticker']:
        if previous_date:
            start_date = get_last_date(ticker.replace('.','_').replace('-','_'), creds)
            logging.warning("start_date: %s end_date: %s" % (start_date, end_date))
        else:
            start_date = end_date

        try:
            panel_data = data.DataReader(ticker, 'yahoo', '2018-09-30', '2018-09-30')[:-1]
            table_name = ticker.replace('.','_').replace('-','_')
            create_new_stock_table(table_name, creds)
            update_in_db(panel_data, table_name, creds)

        except Exception as error:
            logging.error("Error al tratar de obtener datos de %s: %s" % (ticker,error))


get_stock_data(lista_tickers,creds)
