# hack to import parent utilities
import sys 
sys.path.append('..')

import logging
import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv
load_dotenv()
import matplotlib.pyplot as plt
import time
from datetime import datetime, timedelta
import dateutil
import pandas as pd

from libstonks import kite_historical

instruments_df_nse_eq = kite_historical.get_instrument_list(exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE, instrument_type=kite_historical.INSTRUMENT_TYPE_EQ)

instruments_to_focus = instruments_df_nse_eq[instruments_df_nse_eq[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL]=="BHARTIARTL"]
for idx, instrument in instruments_to_focus.iterrows():
    print(instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL])
    print("---------------------")
    instrument_history = kite_historical.get_instrument_history(instrument)
    instrument_history["date"] = instrument_history["date"].apply(lambda x: dateutil.parser.parse(x))
    
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ax1.plot(instrument_history["date"], instrument_history["close"])
    options_df = kite_historical.get_options_for_instrument(instrument)
    if len(options_df)>0:
        for idx_opt, option_instrument in options_df.iterrows():
            option_strike = option_instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL].replace(instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL],"")
            option_strike = int(option_strike[5:][:-2])
            option_instrument_type = option_instrument[kite_historical.INSTRUMENT_KEY_INSTRUMENT_TYPE]

            if option_instrument_type != "CE":
                continue
            
            if option_strike > 510 and option_strike < 550:
                instrument_history = kite_historical.get_instrument_history(option_instrument,option_expiry=kite_historical.OPTION_EXPIRTY_1MONTH)
                instrument_history["date"] = instrument_history["date"].apply(lambda x: dateutil.parser.parse(x))
                ax2.plot(instrument_history["date"], instrument_history["close"], label=f"{option_strike} {option_instrument_type}")
    plt.legend()
    plt.show()