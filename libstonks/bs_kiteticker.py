from datetime import datetime
import time
import logging
from threading import Thread
from numpy import clongdouble

import pandas as pd
from kiteconnect import KiteTicker
from pandas.core.frame import DataFrame

from libstonks.bs_kiteconnect import KITE_CONNECT_API_KEY, _read_kite_access_token, make_kiteconnect_api
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_30MINUTE, DATA_INTERVAL_3MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_60MINUTE, DATA_INTERVAL_DAY, INSTRUMENT_KEY_INSTRUMENT_TOKEN, get_instrument_list, DATA_INTERVAL_MINUTE

DATA_INTERVAL_TICK = 'TICK'

logging.basicConfig(level=logging.DEBUG)

class BSKiteTicker(Thread):

    def __init__(self) -> None:
        super().__init__(daemon=True)
        make_kiteconnect_api()
        kiteconnect_access_tokenmap = _read_kite_access_token()
        KITE_ACCESS_TOKEN = kiteconnect_access_tokenmap[KITE_CONNECT_API_KEY]
        # self.instrument_df = get_instrument_list(force_refresh=True)
        # Initialise
        print(f"api:{KITE_CONNECT_API_KEY} accesstoken:{KITE_ACCESS_TOKEN}")
        kws = KiteTicker(KITE_CONNECT_API_KEY, KITE_ACCESS_TOKEN)
        kws.on_ticks = self.on_ticks
        kws.on_connect = self.on_connect
        kws.on_close = self.on_close
        self.kws = kws
        self.ws = None
        self.pending_subscribe = []
        self.pending_set_mode = []
        self.instrument_tick_data_df = {}
        

    # we expect instrument dictionaries
    def subscribe(self, instrument_token, set_mode='full'):
        print(f"SUBBING: {instrument_token} {set_mode}")
        if type(instrument_token) is not list:
            instrument_token = [instrument_token]
        if self.ws is None:
            self.pending_subscribe.append((instrument_token, set_mode))
            return
        self.ws.subscribe(instrument_token)
        if set_mode is not None:
            self.set_mode(instrument_token, set_mode)
    
    def set_mode(self, instrument_token, set_mode):
        print(f"SETMODE: {instrument_token} {set_mode}")
        if type(instrument_token) is not list:
            instrument_token = [instrument_token]
        if self.ws is None:
            self.pending_set_mode.append((instrument_token, set_mode))
            return
        self.ws.set_mode(set_mode, instrument_token)

    def unsubscribe(self, instrument_token):
        self.ws.unsubscribe(instrument_token)

    def on_ticks(self, ws, ticks):
        # Callback to receive ticks.
        # logging.debug("Ticks: {}".format(ticks))
        for tick in ticks:
            tick_token = tick[INSTRUMENT_KEY_INSTRUMENT_TOKEN]
            if tick_token not in self.instrument_tick_data_df:
                self.instrument_tick_data_df[tick_token] = pd.DataFrame([], columns=['date','price','volume'])#, dtype=[('date',datetime), ('price',float), ('volume',int)])
                self.instrument_tick_data_df[tick_token]['date'] = pd.to_datetime(self.instrument_tick_data_df[tick_token]['date'])
                self.instrument_tick_data_df[tick_token]['price'] = pd.to_numeric(self.instrument_tick_data_df[tick_token]['price'])
                self.instrument_tick_data_df[tick_token]['volume'] = pd.to_numeric(self.instrument_tick_data_df[tick_token]['volume'])
            self.instrument_tick_data_df[tick_token].loc[len(self.instrument_tick_data_df[tick_token])] = {'date':tick['last_trade_time'], 'price':tick['last_price'], 'volume':tick['volume']}
            # print(self.instrument_tick_data_df[tick_token])
            # print(self.instrument_tick_data_df[tick_token].dtypes)
        # for tick in ticks:
    @staticmethod
    def get_resample_map():
        return {
            DATA_INTERVAL_TICK: '1S',
            DATA_INTERVAL_MINUTE: '1min',
            DATA_INTERVAL_3MINUTE: '3min',
            DATA_INTERVAL_5MINUTE: '5min',
            DATA_INTERVAL_15MINUTE: '15min',
            DATA_INTERVAL_30MINUTE: '30min',
            DATA_INTERVAL_60MINUTE: '60min',
            DATA_INTERVAL_DAY: '1D'
        }
    def get_instrument_data(self, instrument_token, interval=DATA_INTERVAL_MINUTE):
        resample_map = BSKiteTicker.get_resample_map()
        if instrument_token not in self.instrument_tick_data_df:
            print(f"{instrument_token} not in {self.instrument_tick_data_df.keys()}")
            return None
        
        base_df:DataFrame = self.instrument_tick_data_df[instrument_token]
        
        volume_df:DataFrame = base_df[['date','volume']].set_index('date').resample(resample_map[interval]).apply(lambda x: x.iloc[-1] - x.iloc[0]).reset_index()
        ohlc_df:DataFrame = base_df[['date','price']].set_index('date').resample(resample_map[interval]).ohlc()
        
        ohlc_df.columns = ohlc_df.columns.droplevel()
        ohlc_df.reset_index(inplace=True)
        # print(ohlc_df)
        # print(volume_df)
        merged_df = pd.merge(volume_df, ohlc_df, on='date')
        return merged_df

    def on_connect(self, ws, response):
        print(f"Connected:{response}")
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
        self.ws = ws
        if len(self.pending_subscribe) > 0:
            for pendingsub in self.pending_subscribe:
                self.subscribe(*pendingsub)
        if len(self.pending_set_mode) > 0:
            for pendingmode in self.pending_set_mode:
                self.set_mode(*pendingmode)
        # ws.subscribe([738561, 5633])

        # Set RELIANCE to tick in `full` mode.
        # ws.set_mode(ws.MODE_FULL, [738561])

    def on_error(self, ws, code, reason):
        logging.debug(f"ERROR {code} {reason}")

    def on_close(self, ws, code, reason):
        logging.debug(f"CLOSE  {code} {reason}")
        self.ws = None
    
    def run(self) -> None:
        self.kws.connect(threaded=True)
    

        



# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.

if __name__ == "__main__":
    pass
