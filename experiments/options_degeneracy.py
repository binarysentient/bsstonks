# hack to import parent utilities
import sys

from dateutil.tz.tz import tzoffset 
sys.path.append('..')

import logging
import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv
load_dotenv()
import matplotlib.pyplot as plt
import time
from datetime import date, datetime, timedelta
import dateutil
import pandas as pd

from libstonks import kite_historical

instruments_df_nse_eq = kite_historical.get_instrument_list(exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE, instrument_type=kite_historical.INSTRUMENT_TYPE_EQ)
start_date=datetime(2021,5,1, tzinfo=tzoffset("india",5.5*60*60))
end_date = datetime(2021,7,21, tzinfo=tzoffset("india",5.5*60*60))
instruments_to_focus = instruments_df_nse_eq[instruments_df_nse_eq[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL]=="BIOCON"]
for idx, instrument in instruments_to_focus.iterrows():
    print(instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL])
    print("---------------------")
    instrument_history = kite_historical.get_instrument_history(instrument)
    instrument_history = instrument_history[(instrument_history['date']>=start_date) & (instrument_history['date']<=end_date)]
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ax1.plot(instrument_history["date"], instrument_history["close"], linestyle="--")
    options_df = kite_historical.get_options_for_instrument(instrument)
    
    if len(options_df)>0:
        processed = set()
        for idx_opt, option_instrument in options_df.iterrows():
            option_strike = option_instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL].replace(instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL],"")
            option_strike = int(option_strike[5:][:-2])
            option_instrument_type = option_instrument[kite_historical.INSTRUMENT_KEY_INSTRUMENT_TYPE]

            if option_instrument_type != "CE":
                continue
            
            if option_strike > 380 and option_strike < 400 and option_strike not in processed:
                processed.add(option_strike)
                instrument_history = kite_historical.get_instrument_history(option_instrument,option_expiry=kite_historical.OPTION_EXPIRTY_1MONTH)
                instrument_history = instrument_history[(instrument_history['date']>=start_date) & (instrument_history['date']<=end_date)]
                ax2.plot(instrument_history["date"], instrument_history["close"], label=f"{option_instrument[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL]}")
    plt.legend()
    plt.show()