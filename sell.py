import datetime
import os
import sys
import paramiko
import psycopg2
from binance.client import Client
from sshtunnel import SSHTunnelForwarder
from decimal import Decimal


# TODO: Connection to Binance
# TODO: Transaction costs

def get_connection():
    try:
        mypkey = paramiko.RSAKey.from_private_key_file('/home/acadoga/.ssh/netcup')

        tunnel = SSHTunnelForwarder(
            ("v2202011116930133496.megasrv.de", 22),
            ssh_username="vj",
            ssh_pkey=mypkey,
            remote_bind_address=('localhost', 5432))

        tunnel.start()

        return psycopg2.connect(dbname='Trading', user="postgres", password="postgres", host='127.0.0.1',
                                port=tunnel.local_bind_port)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def current_price(asset):
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')
    client = Client(api_key, api_secret)
    return client.get_symbol_ticker(symbol=asset)


def get_asset(asset):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(''' SELECT * FROM current WHERE asset LIKE '%s' ''' % asset)
        bought_asset = cur.fetchone()
        cur.close()
        conn.commit()
        return bought_asset
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_sell(time_bought, time_now, asset, buy_price, price_now, quantity, gain, gain_percent):
    sql = '''INSERT INTO sell(time_bought, time_sold, asset, price_old, price_new, quantity, gain, gain_percent) 
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) '''
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (time_bought, time_now, asset, buy_price, price_now, quantity, gain, gain_percent))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_current(asset):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute('''DELETE FROM current WHERE asset LIKE '%s' ''' % asset)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def main(asset):
    # asset = sys.argv[1]
    # Check if Asset in table current
    bought_asset = get_asset(asset)
    if bought_asset is None:
        print("There is no such asset: %s" % asset)
        sys.exit(1)
    # Get Information: time_bought, buy_price, quantity
    time_bought = bought_asset[0]
    quantity = Decimal(bought_asset[3])
    time_now = datetime.datetime.now()
    buy_price = Decimal(bought_asset[2]) * quantity
    price_now = Decimal(current_price(asset)['price']) * quantity
    gain = price_now - buy_price
    gain_percent = ((100 / buy_price) * gain)
    # Give Binance sell order -> Error if no success
    # Update table sell
    update_sell(time_bought, time_now, asset, buy_price, price_now, quantity, gain, gain_percent)
    delete_current(asset)
    print("Bought %.2f %s on %s for %.2f. Sold for %.2f. Gain: %.2f€ or %.2f%%" % (
        quantity, asset, time_bought, buy_price, price_now, gain, gain_percent))
