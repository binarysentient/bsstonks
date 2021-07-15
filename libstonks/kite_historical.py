import os
import time
import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv
import dateutil
import pandas as pd
import ratelimit

from bs_kiteconnect import make_kiteconnect_api

load_dotenv()
logging.basicConfig(level=logging.DEBUG)
BSSTONKS_DIRECTORY = os.getenv("BSSTONKS_DIRECTORY")
KITE_INSTRUMENTS_DIRECTORY = os.path.join(BSSTONKS_DIRECTORY,"input","kite_instruments")
KITE_HISTORICAL_DIRECTORY = os.path.join(BSSTONKS_DIRECTORY,"input","kite_historical")
OPTION_EXPIRTY_1MONTH="exp1month"
OPTION_EXPIRTY_2MONTH="exp2month"
OPTION_EXPIRTY_3MONTH="exp3month"

def kite_instrument_to_filename(instrument_dict, interval="day", option_expiry="1mo"):
    kite_instrument_token = instrument_dict['instrument_token']
    trading_symbol = instrument_dict['tradingsymbol']
    instrument_type = instrument_dict['instrument_type']
    segment = instrument_dict['segment']
    name = instrument_dict['name']
    exchange = instrument_dict['exchange']
    if exchange == "NFO" and (instrument_type == "CE" or instrument_type == "PE"):
        trading_symbol = trading_symbol.replace(name,"")
        trading_symbol = trading_symbol[5:]
        trading_symbol = name + trading_symbol
        trading_symbol += option_expiry
    # we've tested that _ is valid seperator by evaluating for all the instruments (86k)
    result_file = f"{kite_instrument_token}_{trading_symbol}_{instrument_type}_{segment}_{exchange}_{interval}.csv"
    return result_file

def sync_instrument_list():
    kite = make_kiteconnect_api()
    all_instruments = kite.instruments()
    df_all_instruments = pd.DataFrame(all_instruments)
    os.makedirs(KITE_INSTRUMENTS_DIRECTORY, exist_ok=True)
    df_all_instruments.to_csv(os.path.join(KITE_INSTRUMENTS_DIRECTORY,"instrument_list.csv"), index=False)

def get_instrument_list(refresh=False):
    return pd.read_csv(os.path.join(KITE_INSTRUMENTS_DIRECTORY,"instrument_list.csv"))


@ratelimit.sleep_and_retry
@ratelimit.limits(calls=3, period=1)
def throttled_historical_data_api(instrument_token, start_datetime, end_datetime, data_interval, oi=True, continuous=False):
    return make_kiteconnect_api().historical_data(instrument_token, start_datetime.strftime("%Y-%m-%d"), end_datetime.strftime("%Y-%m-%d"), data_interval, oi=oi, continuous=continuous)

def sync_instrument_history(instrument_dict, data_interval="day", fetch_past=True, option_expiry=None):
    interval_data_span_limit = {
        'day': 2000,
        '60minute':400,
        '30minute':200,
        '15minute':200,
        '5minute':100,
        '3minute':100,
        'minute':60,
    }
    interval_span_days = interval_data_span_limit[data_interval]
    
    result_file = kite_instrument_to_filename(instrument_dict, interval=data_interval, option_expiry=option_expiry)
    result_file_path = os.path.join(KITE_HISTORICAL_DIRECTORY,result_file)
    df_to_sync = None
    if not os.path.exists(result_file_path):
        df_to_sync = pd.DataFrame([],columns=["date","open","high","low","close","volume","oi"])
    else:
        df_to_sync = pd.read_csv(result_file_path,index_col=False)

    #     
    # Past direction
    while True and (fetch_past or df_to_sync.shape[0]==0):

        #IMPORTANT: THROTTLE LIMIT MUST BE RESPECTED
        
        end = datetime.now()
        start = end - timedelta(days=interval_span_days-1)

        if df_to_sync.shape[0] > 0:
            end = df_to_sync.iloc[0]["date"]
            if type(end) == str:
                end = dateutil.parser.parse(end)
            end = end - timedelta(days=1)
            #dates are inclusive so start date (historical date) needs to be adjusted
            start = end - timedelta(days=interval_span_days-1)

        historical_data = throttled_historical_data_api(instrument_dict['instrument_token'], start, end, data_interval, oi=True)
        for x in historical_data:
            x['date'] = str(x['date'])

        # WHEN API RETURNS NO DATA; EXIT.
        if len(historical_data) == 0:
            break

        df_to_sync_more_hist = pd.DataFrame(historical_data)
        df_to_sync = df_to_sync_more_hist.append(df_to_sync, ignore_index=True)      
        df_to_sync.to_csv(result_file_path, index=False)

        # if it returns less than possible working days of data then just 
        if len(historical_data) < 3 or dateutil.parser.parse(historical_data[-1]['date']) - dateutil.parser.parse(historical_data[0]['date']) < timedelta(days=int(interval_span_days*0.5 - 15)):
            break

    #     
    # Future direction
    while True:   
        start = datetime.now()
        end = start + timedelta(days=interval_span_days-1)

        if df_to_sync.shape[0] > 0:
            start = df_to_sync.iloc[-1]["date"]
            if type(start) == str:
                start = dateutil.parser.parse(start)
            # what if we run it while the stock market is open, and run it again after it closes
            # today's data must be overwritten/appended
            start = start - timedelta(days=1)
            #dates are inclusive so start date (historical date) needs to be adjusted
            end = start + timedelta(days=interval_span_days-1)
            historical_data = throttled_historical_data_api(instrument_dict['instrument_token'], start, end, data_interval, oi=True)

        
        for x in historical_data:
            x['date'] = str(x['date'])

        if len(historical_data) == 0:
            break
        # WHEN API RETURNS NO DATA; EXIT.


        df_to_sync_more_hist = pd.DataFrame(historical_data)
        # rewrite last few days of new data onto old data; this takes care of running it during trdaing window
        overlaps_count = df_to_sync[df_to_sync['date']>=str(historical_data[0]['date'])].shape[0]
        

        df_to_sync = df_to_sync.iloc[:-overlaps_count]
        
        df_to_sync = df_to_sync.append(df_to_sync_more_hist, ignore_index=True)      
        df_to_sync.to_csv(result_file_path, index=False)

        # if it returns less than possible working days of data then just 
        if len(historical_data) < 3 or dateutil.parser.parse(historical_data[-1]['date']) - dateutil.parser.parse(historical_data[0]['date']) < timedelta(days=int(interval_span_days*0.5 - 15)):
            break





if __name__ == "__main__":
    # sync_instrument_list()
    all_instruments = get_instrument_list()
    
    sync_instrument_history(all_instruments[all_instruments['tradingsymbol']=="NIFTY 50"].iloc[0], data_interval="day")
    
    
