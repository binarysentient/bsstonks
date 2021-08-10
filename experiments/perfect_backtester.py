
import sys

from dateutil.tz.tz import tzoffset 
sys.path.append('..')
from libstonks.indicators import INDICATORS
from libstonks import kite_historical
from libstonks.trading_strategy import main_test
import matplotlib.pyplot as plt

def run_single_backtest():
    pass

# def main_test():
#     pass

if __name__ == "__main__":
    # ,"NIFTY 50","TATAPOWER","SUZLON"
    interested_stocks = kite_historical.get_instrument_list(symbol_eq=["NIFTY 50", "RELIANCE"], exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE)
    for idx, instrument in interested_stocks.iterrows():
        kite_historical.sync_instrument_history(instrument, data_interval="day")
        kite_historical.sync_instrument_history(instrument, data_interval="15minute")
        kite_historical.sync_instrument_history(instrument, data_interval="5minute")
        kite_historical.sync_instrument_history(instrument, data_interval="minute")
        continue
        instrument_history = kite_historical.get_instrument_history(instrument, parse_date=True)
        get_indicator = INDICATORS.create_get_indicator_func(instrument_history)
        rsi_15 = get_indicator("rsi", {'length':15})
        ema_rsi_lvl1 = get_indicator("ema", {'length':2, 'source':rsi_15.name})
        ema_rsi_lvl2 = get_indicator("ema", {'length':2, 'source':ema_rsi_lvl1.name})
        ema_rsi_lvl3 = get_indicator("ema", {'length':3, 'source':ema_rsi_lvl2.name})
        
        ema_rsi_lvl4 = get_indicator("ema", {'length':4, 'source':ema_rsi_lvl3.name})
        ema_rsi_lvl5 = get_indicator("ema", {'length':7, 'source':ema_rsi_lvl4.name})

        upper_divergence = ema_rsi_lvl5.rolling(3).apply(lambda x: x.iloc[1] if (x.iloc[0] < x.iloc[1]) and (x.iloc[1]>x.iloc[2]) else -1)
        # print(ema_rsi_15)
        plt.plot(instrument_history['date'],rsi_15)
        plt.plot(instrument_history['date'],ema_rsi_lvl5)
        plt.plot(instrument_history['date'][upper_divergence != -1],upper_divergence[upper_divergence!=-1])
        plt.show()
    print(interested_stocks)
    # main_test()