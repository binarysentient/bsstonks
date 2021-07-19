import logging

from libstonks.kite_historical import get_instrument_list, sync_instrument_list
from libstonks.kite_historical import INSTRUMENT_KEY_EXPIRTY, INSTRUMENT_EXCHANGE_NSE
from libstonks.kite_historical import *
from libstonks.utils.bs_threading import bs_multiprocessify, bs_threadify


# logging.basicConfig(level=logging.DEBUG)


    
TO_SYNC_NSE_DAILY=True
TO_SYNC_NSE_15MINUTE=False
TO_SYNC_OPTIONS_DAILY=False
# TODO: minute level (15min for example) data has date exceeding 3:15pm for the day it is fetched after 3:15 
#       fix taht
if __name__ == "__main__":
    
    # instrument list functions
    sync_instrument_list()

    all_instruments_nse_eq = get_instrument_list(exchange=INSTRUMENT_EXCHANGE_NSE ,instrument_type=INSTRUMENT_TYPE_EQ)
    
    if TO_SYNC_NSE_DAILY:
        for _, row_nse_eq in all_instruments_nse_eq.iterrows():
            print(f"{row_nse_eq[INSTRUMENT_KEY_TRADINGSYMBOL]} - day")
            sync_instrument_history(row_nse_eq, data_interval="day", fetch_past=False)

    if TO_SYNC_NSE_15MINUTE:
        for _, row_nse_eq in all_instruments_nse_eq.iterrows():
            print(f"{row_nse_eq[INSTRUMENT_KEY_TRADINGSYMBOL]} - 15minute")
            # only fetch when there's daily data available
            if len(get_instrument_history(row_nse_eq, parse_date=False)) > 0:
                sync_instrument_history(row_nse_eq, data_interval="15minute")

    if TO_SYNC_OPTIONS_DAILY:
        all_instruments_options = get_instrument_list(segment=INSTRUMENT_SEGMENT_NFO_OPT)
        for idx, instrument in all_instruments_nse_eq.iterrows():
            if get_instrument_history(instrument) is None or len(get_instrument_history(instrument)) < 10:
                continue
            dfoptions_for_current_instrument = search_options_for_instrument_in_df(all_instruments_options, instrument)
            for optidx, optrow in dfoptions_for_current_instrument.iterrows():
                print(f"{optrow[INSTRUMENT_KEY_TRADINGSYMBOL]} - day")
                sync_instrument_history(optrow, data_interval="day")
            
        
    
    