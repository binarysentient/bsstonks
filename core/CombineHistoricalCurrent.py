
import pandas as pd
from bs_indicators import INDICATORS
import time
import random
import numpy as np
import datetime


def abc_1():
    csv_location = "../input/kite_historical/256265_NIFTY 50_EQ_INDICES_NSE_15minute.csv"
    df = pd.read_csv(csv_location)
    hist_cs_df = df[-200:].copy()

    nifty_count = 15924.2
    current_df = pd.DataFrame([])
    while True:
        nifty_count = nifty_count + 0.1
        current_tick = pd.DataFrame([{
            'date': datetime.datetime.now(),
            'open': nifty_count,
            'high': nifty_count,
            'low': nifty_count,
            'close': nifty_count,
            'volume': 0,
            'oi': 0
        }])
        current_df = current_df.append(current_tick, ignore_index=True)
        current_ohlc_df = current_df[['close', 'date']].set_index('date')
        candle_stick_df = current_ohlc_df['close'].resample('15Min').ohlc()
        candle_stick_df['date'] = candle_stick_df.index
        candle_stick_df.index = range(0, len(candle_stick_df))

        effective_df = pd.concat([hist_cs_df, candle_stick_df], ignore_index=True)
        get_indicator = INDICATORS.create_get_indicator_func(effective_df)
        rsi_15 = get_indicator("rsi", {'length': 15})
        print(effective_df[['date', rsi_15.name]].tail(15))

        # hv10 = get_indicator("hv", {'length':10})
        # print("----------")
        # print("-- HV length:10 indicator")
        # print(test_df[["date", "close", hv10.name]].tail(21))

        # rsi_15_ema = get_indicator("rsi", {'length': 15, 'mean_function':'ema'})
        # rsi_15_ma = get_indicator("historicalvolatality", {'length': 2, 'source': rsi_15.name})
        # print(hist_cs_df[['date', rsi_15.name, rsi_15_ema.name, rsi_15_ma.name]].tail(15))


if __name__ == "__main__":
    abc_1()
