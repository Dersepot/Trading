# Doku: https://python-tradingview-ta.readthedocs.io/en/latest/usage.html#getting-the-analysis

import time
from datetime import datetime
import paramiko
import psycopg2
import buy
import sell
from tradingview_ta import TA_Handler, Interval
from decimal import Decimal
from sshtunnel import SSHTunnelForwarder


def get_connection():
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
        # return psycopg2.connect(dbname='Trading', user="postgres", password="postgres", host='127.0.0.1',
        #                         port=tunnel.local_bind_port)
        return psycopg2.connect(
            host="localhost",
            database="Trading",
            user="postgres",
            password="postgres")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def get_money():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(''' SELECT price FROM trading ORDER BY time DESC LIMIT 1 ''')
        money = cur.fetchone()
        cur.close()
        conn.commit()
        return Decimal(money[0])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_etheur_buy():
    etheur = TA_Handler(
        symbol='ETHEUR',
        exchange='Binance',
        screener='CRYPTO',
        interval=Interval.INTERVAL_4_HOURS
    )
    signal = etheur.get_analysis().summary.get('RECOMMENDATION')
    if signal == 'BUY' or signal == 'STRONG_BUY':
        return True
    return False


def get_etheur_sell():
    etheur = TA_Handler(
        symbol='ETHEUR',
        exchange='Binance',
        screener='CRYPTO',
        interval=Interval.INTERVAL_4_HOURS
    )
    signal = etheur.get_analysis().summary.get('RECOMMENDATION')
    if signal == 'SELL' or signal == 'STRONG_SELL':
        return True
    return False


def update_trade(asset, trade, price):
    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = '''INSERT INTO trading(time, asset, trade, price) VALUES(%s,%s,%s,%s) '''
        time_now = datetime.now()
        cur.execute(sql, (time_now, asset, trade, price))
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    asset = 'ETHEUR'
    while True:
        # Check if there is money in binance
        money = get_money() or 0 # TODO: Is this clean?
        if money > 0.0:
            # If "Buy" or "Strong Buy"
            if get_etheur_buy():
                buy.main(asset, money)
                update_trade(asset, 'BUY', 0.0)
        else:
            # If "Sell" or "Strong Sell"
            if get_etheur_sell():
                sell.main(asset)
                update_trade(asset, 'SELL', money)
        time.sleep(14400)  # 1h: 3600 ; 4h: 14400
