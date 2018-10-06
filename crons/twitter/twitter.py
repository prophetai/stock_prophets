#-*- coding: utf-8 -*-
"""
Funciones para obtener los datos de twitter en la base de datos
"""
import sys, opts
import json
import pandas as pd
import logging
import twint
import datetime
sys.path.insert(0, '..')
from utils.extract import db_connection, download_data
import google.cloud.logging

# Instancia un cliente para el logger
#client = google.cloud.logging.Client()

# Connects the logger to the root logging handler; by default this captures
# all logs at INFO level and higher
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.DEBUG,
    #filename='log.txt'
)

#client.setup_logging()

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
    df = DF.copy()

    new_order = ['id', 'user_id', 'date', 'timezone', 'location', 'username', 'tweet', 'hashtags', 'link', 'retweet', 'user_rt', 'mentions']
    df = df[new_order]
    df['hashtags'].replace('[','',inplace=True)
    df['hashtags'].replace(']','',inplace=True)
    date_time = str(datetime.datetime.now())

    lista_tweets = df.values.tolist()

    data_ready = ''

    for i, tweet in enumerate(lista_tweets, start=0):
        tweet_str = []
        for element in tweet:
            element = str(element).replace("'", '')
            transform = "" + str(element).replace("['", '[').replace("']",']')
            tweet_str.append(transform)

        data_ready += "(" + str(tweet_str)[1:-1] + ")"

        if i % 10000 == 0 and data_ready != [] and i > 0:
            try:
                logging.info("Se llega a un número de tweet que es modulo 10K!!!!!!!!!!!!!")
                data_ready = data_ready.replace(")(",'), (')
                query = """INSERT INTO tweets VALUES {} ON CONFLICT (id) DO NOTHING;""".format(data_ready)
                conn = db_connection(creds)
                download_data(conn, query)
                data_ready = ''
                logging.info("Se guardaron tweets ({}-{})".format(i-10000,len(lista_tweets)))
            except Exception as error:
                logging.error("Error al tratar de insertar: %s" % (error))
        elif i == len(lista_tweets)-1 and data_ready != []:
            try:
                logging.info("Se llega a un número de tweet que NO es modulo 10K!!!!!!!!!!!!!")
                data_ready = data_ready.replace(")(",'), (')
                query = """INSERT INTO tweets VALUES {} ON CONFLICT (id) DO NOTHING;""".format(data_ready)
                conn = db_connection(creds)
                download_data(conn, query)
                logging.info("Se guardaron los últimos tweets ({}-{})".format(i - len(lista_tweets) + 1,len(lista_tweets)))
            except Exception as error:
                logging.error("Error al tratar de insertar: %s" % (error))

def main(argv):
    """
    Corre la actualización de una lista de tweets
    """
    opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    #credenciales de db, twitter y emisoras
    try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print 'twitter.py -i <inputfile> -o <outputfile>'
      sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('twitter.py -a <ruta de archivo de cuentas> -c <ruta a creds>')
            sys.exit()
        elif opt in ("-a", "--accounts"):
            inputfile = arg
        elif opt in ("-c", "--creds"):
            creds_file = arg

    try:
        with open(creds_file, encoding='utf-8') as data_file:
            creds = json.loads(data_file.read())
    except Exception e:
        logging.error('No se encuentra el archivo de credenciales: {}'.format(creds_file))
    try:
        lista_cuentas = pd.read_csv(inputfile)['Cuentas']
    except Exception e:
        logging.error('No se encuentra el archivo de cuentas: {}'.format(creds_file))

    try:
        for cuenta in lista_cuentas:
            df = search_tweets(cuenta)
            load_tweets(df, creds)
            twint.storage.panda.Tweets_df = ''
            del df
    except Exception as e:
        logging.error('Error al insertar en la base %s' % (e))

if __name__ == "__main__":
    main(sys.argv[1:])
    version = ".".join(str(v) for v in sys.version_info[:2])
    if float(version) < 3.6:
        print("[-] TWINT requires Python version 3.6+.")
        sys.exit(0)

    main()
