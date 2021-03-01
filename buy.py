#!/usr/bin/env python3
import os
import sys
from decimal import Decimal

import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
from binance.client import Client


def current_price(asset):
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    client = Client(api_key, api_secret)
    return client.get_symbol_ticker(symbol=asset)


def update_db(asset, quantity, price):
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

        cur.execute(''' SELECT start_time FROM %s ORDER BY start_time DESC LIMIT 1 ''' % asset)
        date = cur.fetchone()
        order = (date, asset, price, quantity)
        sql = '''INSERT INTO current(time, asset, buy_price, quantity) VALUES(%s,%s,%s,%s) '''
        cur.execute(sql, order)

        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    # get current price for asset
    # calculate how many crypto you can buy for that
    # place order -> How hight are the fees? Is the account covered?
    # put in table current
    asset = "ETHEUR"  # sys.argv[1]
    order_size = 100  # sys.argv[2]
    price = Decimal(current_price(asset)['price'])
    quantity = order_size / price
    update_db(asset, quantity, price)
    print("Bought %s of '%s'" % (quantity, asset))
