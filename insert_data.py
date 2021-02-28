#!/usr/bin/python
import os
import psycopg2
import paramiko
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from binance.client import Client
from binance.websockets import BinanceSocketManager


def process_message(msg):
    event_type = msg['e']
    # Format to YYYY-MM-DD HH:MM:SS.SSS
    event_time = datetime.fromtimestamp(msg['E'] / 1000)
    symbol = msg['s']
    start_time = datetime.fromtimestamp(msg['k']['t'] / 1000)
    end_time = datetime.fromtimestamp(msg['k']['T'] / 1000)
    opening = msg['k']['o']
    close = msg['k']['c']
    high = msg['k']['h']
    low = msg['k']['l']
    volume = msg['k']['v']

    my_row = (event_type, event_time, symbol, start_time, end_time, opening, close, high, low, volume)
    insert(my_row)


def insert(new_row):
    """ insert a new vendor into the vendors table """
    sql = '''INSERT INTO etheur(event_type, event_time, symbol, start_time, end_time, open, close, high, low, 
    volume) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    ON CONFLICT (start_time) DO UPDATE SET close = EXCLUDED.close, high = EXCLUDED.high, low = EXCLUDED.low , 
    volume = EXCLUDED.volume '''
    conn = None

    try:
        mypkey = paramiko.RSAKey.from_private_key_file('/home/acadoga/.ssh/netcup')

        tunnel = SSHTunnelForwarder(
            ("v2202011116930133496.megasrv.de", 22),
            ssh_username="vj",
            ssh_pkey=mypkey,
            remote_bind_address=('localhost', 5432))

        tunnel.start()

        conn = psycopg2.connect(dbname='Trading', user="postgres", password="postgres", host='127.0.0.1',
                                port=tunnel.local_bind_port)

        cur = conn.cursor()

        # execute the INSERT statement
        cur.execute(sql, new_row)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    client = Client(api_key, api_secret)
    bm = BinanceSocketManager(client)
    bm.start_kline_socket('ETHEUR', process_message, interval="1h")
    # then start the socket manager
    bm.start()
