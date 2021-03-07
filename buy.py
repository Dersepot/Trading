#!/usr/bin/env python3

# Takes arguments "asset" and "order_size"
# Buys for money order_size quantity of asset for current price
# Writes the order into table current

import os
import sys
from datetime import datetime

import paramiko
import psycopg2
from decimal import Decimal
from sshtunnel import SSHTunnelForwarder
from binance.client import Client


# TODO: Check if enough money to buy
# TODO: Transaction Costs

def current_price(asset):
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    client = Client(api_key, api_secret)
    return client.get_symbol_ticker(symbol=asset)


def update_db(asset, quantity, price):
    conn = None
    try:
        # mypkey = paramiko.RSAKey.from_private_key_file('/home/acadoga/.ssh/netcup')
        #
        # tunnel = SSHTunnelForwarder(
        #     ("v2202011116930133496.megasrv.de", 22),
        #     ssh_username="vj",
        #     ssh_pkey=mypkey,
        #     remote_bind_address=('localhost', 5432))
        #
        # tunnel.start()
        #
        # conn = psycopg2.connect(dbname='Trading', user="postgres", password="postgres", host='127.0.0.1',
        #                         port=tunnel.local_bind_port)

        conn = psycopg2.connect(
            host="localhost",
            database="Trading",
            user="postgres",
            password="postgres")

        cur = conn.cursor()

        time = datetime.now()
        sql = '''INSERT INTO current(time, asset, buy_price, quantity) VALUES(%s,%s,%s,%s) '''
        cur.execute(sql, (time, asset, price, quantity))

        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def main(asset, order_size):
    # get current price for asset
    # calculate how many crypto you can buy for that
    # place order -> How hight are the fees? Is the account covered?
    # put in table current
    # asset = sys.argv[1]  # "ETHEUR"
    # order_size = sys.argv[2]  # 100
    price = Decimal(current_price(asset)['price'])
    quantity = order_size / price
    # TODO: Give oder to Binance
    update_db(asset, quantity, price)
    print("Bought %s of '%s'" % (quantity, asset))
