#-*- coding: utf-8 -*-
"""
Funciones para obtener los datos de twitter en la base de datos
"""
import sys
import json
import pandas as pd
import logging
import twint
import datetime
from utils.extract import db_connection, download_data


def search_tweets(cuenta):
    """
    Obtiene todos los tweets de una cuenta

    Args:
        cuenta(str): nombre de la cuenta a buscar
    Response:
        df(DataFrame): DataFrame con los resultados
    """

    c = twint.Config()
    c.Username = cuenta.replace('@','')
    c.Pandas = True
    c.Limit = 10
    try:
        twint.run.Search(c)
    except asyncio.TimeoutError as error:
        logging.error('Error en la búsqueda de tweets: %s' % (error))

    df = twint.storage.panda.Tweets_df

    return df

def load_tweets(DF, creds):
    """
    Carga los tweets desde un dataframe a una base de datos

    Args:
        df(Dataframe): DataFrame con datos a subir a la base de datos
        creds(dict): Diccionario con las credenciales de la base de datos
    """
    conn = db_connection(creds)
    df = DF.copy()
    logging.warning('COLUMNAS: %s' , (df.columns))
    new_order = ['id', 'user_id', 'date', 'timezone', 'location', 'username', 'tweet', 'hashtags', 'link', 'retweet', 'user_rt', 'mentions']
    df = df[new_order]

    date_time = str(datetime.datetime.now())

    lista_tweets = df.values.tolist()
    lista_tweets = ['('+str(tweet).replace("['","'").replace("']","'")+')' for tweet in lista_tweets]

    print('se van a guardar {} tweets de la cuenta: {}'.format(len(lista_tweets), lista_tweets[0][12]))

    data_ready = []

    for tweet in lista_tweets:
        element_ready = str(element for element in tweet) # WARNING: hay que pensar en los elementos que son listas
        data_ready.append(element_ready)

        data_ready = data_ready + " )"
    data_ready = data_ready.replace('[','').replace(']','').replace('"','').replace(', ,',",'',").replace('None',"''")

    logging.warning(data_ready)

    query= """INSERT INTO tweets VALUES {};""".format(data_ready)

    try:
        download_data(conn,query)
    except Exception as e:
        logging.error('Error al insertar en la base: %s' % (e))

def main():
    """
    Corre la actualización de una lista de tweets
    """
    #credenciales de db, twitter y emisoras
    with open('creds.txt', encoding='utf-8') as data_file:
        creds = json.loads(data_file.read())

    lista_cuentas = pd.read_csv('Cuentas BMBV.csv')['Cuentas']

    for cuenta in lista_cuentas:
        df = search_tweets(cuenta)

        try:
            load_tweets(df, creds)
        except Exception as e:
            logging.error('Error al insertar en la base %s' % (e))

if __name__ == "__main__":
    version = ".".join(str(v) for v in sys.version_info[:2])
    if float(version) < 3.6:
        print("[-] TWINT requires Python version 3.6+.")
        sys.exit(0)

    main()
