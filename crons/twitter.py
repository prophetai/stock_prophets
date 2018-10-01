#-*- coding: utf-8 -*-
"""
Funciones para obtener los datos de twitter en la base de datos
"""
import sys
import json
import pandas as pd
import logging
import twint
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

    twint.run.Search(c)

    df = twint.storage.panda.Tweets_df

    return df

def load_tweets(df, creds):
    """
    Carga los tweets desde un dataframe a una base de datos

    Args:
        df(Dataframe): DataFrame con datos a subir a la base de datos
        creds(dict): Diccionario con las credenciales de la base de datos
    """
    conn = db_connection(creds)

    date_time = str(datetime.now())

    lista_tweets = df.values.tolist()
    lista_tweets = [tuple(tweet.append(date_time)) for tweet in lista_tweets]
    print('se van a guardar {} tweets de la cuenta: {}'.format(len(lista_tweets), lista_tweets[0][12]))

    data_ready = str(lista_tweets[1:-1])

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
        df = obtiene_todos_tweets(cuenta)

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
