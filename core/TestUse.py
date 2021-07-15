import csv
import pandas as pd
import os
import logging
from threading import Thread
from kiteconnect import KiteTicker
import time
import datetime
from bs_indicators import INDICATORS


USER_ID = 'FV6492'

'''
dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = '../BNF_data/BNF_IND_Historical_vstop_ATR_29_to_31.csv'
data_file = os.path.join(dir_path, file_path)

df = pd.read_csv(data_file)
'''
tokens = [256265]  # Nifty 50 : 256265  # BANKNIFTY21JANFUT : 12680706

ACCESS_TOKEN = '2g7yewaWbeAf9bIu7p8T8GFq0hwDHPZo'
KITE_CONNECT_API_KEY = ''
last_price = 0


csv_location = "../input/kite_historical/256265_NIFTY 50_EQ_INDICES_NSE_15minute.csv"
df = pd.read_csv(csv_location)
hist_cs_df = df[-200:].copy()
nifty_count = 15727
current_df = pd.DataFrame([])
global_count = 0


def get_api_key_access_token():
    # Initialise
    access_token = ACCESS_TOKEN
    api_key = KITE_CONNECT_API_KEY
    return api_key, access_token


def on_ticks(ws, ticks):
    global current_df, hist_cs_df, global_count
    global_count += 0.1
    print(ticks[0])
    last_price = ticks[0]['last_price'] + global_count
    current_tick = pd.DataFrame([{
        'date': datetime.datetime.now(),
        # 'open': ticks[0]['ohlc']['open'],
        # 'high': ticks[0]['ohlc']['high'],
        # 'low': ticks[0]['ohlc']['low'],
        # 'close': ticks[0]['ohlc']['close'],
        'last_price': last_price,
        'volume': 0,
        'oi': 0,
        'rsi_15_sma': 0
    }])

    current_df = current_df.append(current_tick)

    current_ohlc_df = current_df[['last_price', 'date']].set_index('date')
    candle_stick_df = current_ohlc_df['last_price'].resample('15Min').ohlc()
    candle_stick_df['date'] = candle_stick_df.index
    candle_stick_df.index = range(0, len(candle_stick_df))

    effective_df = pd.concat([hist_cs_df, candle_stick_df], ignore_index=True)
    get_indicator = INDICATORS.create_get_indicator_func(effective_df)
    rsi_15 = get_indicator("rsi", {'length': 15})
    print(effective_df[['date', rsi_15.name]].tail(15))

    return last_price


def on_connect(ws, response):
    # Callback on successful connect.

    # Set IDEA to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, tokens)

    # Set IDEA to tick in `LTP` mode.
    # ws.set_mode(ws.MODE_LTP, [136402436])

    # Set IDEA to tick in `QUOTE` mode.
    # ws.set_mode(ws.MODE_QUOTE, [136402436])


def on_close(ws, code, reason):
    # On connection close stop the event loop.
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()


def initialise_kite_ticker():
    api_key, access_token = get_api_key_access_token()
    kws = KiteTicker(api_key, access_token)

    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    # kws.on_close = on_close

    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    # kws.connect(threaded=True)
    # kws.connect(disable_ssl_verification=True)  # for ubuntu
    kws.connect()
    return kws


def check_handle_kite_connection(kws):
    logging.info("This is main thread. Will subscribe to each token in tokens list with 5s delay")
    count = 0
    try:
        if kws.is_connected():
            logging.info("Subscribing to: {}".format(tokens[count]))
            kws.subscribe([tokens[count]])
            kws.set_mode(kws.MODE_FULL, [tokens[count]])
        else:
            kws.connect(threaded=True)
            # kws.connect()

            # TODO : use this if needed
            # def resubscribe(	self)
            # Resubscribe to all current subscribed tokens.
            #

            logging.info("Connecting to WebSocket...")
    except Exception as e:
        logging.exception('EXCEPTION : {}'.format(e))
        print('Exception : ', e)

    # print('$$$$$$$ ticks is executing $$$$$$$$')
    # time.sleep(5)


class LiveTicks(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.__kws = initialise_kite_ticker()
        self.daemon = True
        self.start()

    def run(self):

        while True:
            # print('\n\n#### LiveTicks is executing #### --> ', datetime.datetime.now())
            # get_live_ticks()
            check_handle_kite_connection(self.__kws)
            time.sleep(1)


LiveTicks()

'''
def get_live_ticks():
    api_key, access_token = get_api_key_access_token()
    kws = KiteTicker(api_key, access_token)

    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    # kws.connect(threaded=True)
    # kws.connect(disable_ssl_verification=True) # for ubuntu
    kws.connect()

    count = 0
    while True:
        logging.info("This is main thread. Will subscribe to each token in tokens list with 5s delay")
        try:
            if kws.is_connected():
                logging.info("Subscribing to: {}".format(tokens[count]))
                kws.subscribe([tokens[count]])
                kws.set_mode(kws.MODE_FULL, [tokens[count]])
            else:
                # kws.connect(threaded=True)
                kws.connect()

                # TODO : use this if needed
                # def resubscribe(	self)
                # Resubscribe to all current subscribed tokens.
                #

                logging.info("Connecting to WebSocket...")
        except Exception as e:
            logging.exception('EXCEPTION : {}'.format(e))

        print('$$$$$$$ ticks is executing $$$$$$$$')
        # time.sleep(5)

    return


get_live_ticks()
'''