import os
import time
import logging
from datetime import date, datetime, timedelta
import calendar

from dotenv import load_dotenv
import dateutil
import dateutil.parser
import pandas as pd
import ratelimit

from libstonks.bs_kiteconnect import make_kiteconnect_api
from libstonks.utils.bs_dateutil import get_last_thursdays_between_dates

load_dotenv()

BSSTONKS_DIRECTORY = os.getenv("BSSTONKS_DIRECTORY")
KITE_INSTRUMENTS_DIRECTORY = os.path.join(BSSTONKS_DIRECTORY,"input","kite_instruments")
KITE_HISTORICAL_DIRECTORY = os.path.join(BSSTONKS_DIRECTORY,"input","kite_historical")
'''
segment: ['BCD-OPT' 'BCD-FUT' 'BCD' 'BSE' 'INDICES' 'CDS-OPT' 'CDS-FUT' 'MCX-FUT'
 'MCX-OPT' 'NFO-OPT' 'NFO-FUT' 'NSE']
instrument_type: ['CE' 'PE' 'FUT' 'EQ']
exchange: ['BCD' 'BSE' 'MCX' 'NSE' 'CDS' 'NFO']
NFO exchange instrument types: ['CE' 'PE' 'FUT']
'''
INSTRUMENT_KEY_TRADINGSYMBOL = "tradingsymbol"
INSTRUMENT_KEY_INSTRUMENT_TYPE = "instrument_type"
INSTRUMENT_KEY_EXCHANGE = "exchange"
INSTRUMENT_KEY_SEGMENT = "segment"
INSTRUMENT_KEY_EXPIRTY = "expiry"
INSTRUMENT_TYPE_EQ = "EQ"
INSTRUMENT_TYPE_PE = "PE"
INSTRUMENT_TYPE_PE = "FUT"
INSTRUMENT_TYPE_PE = "EQ"
INSTRUMENT_EXCHANGE_NSE = "NSE"
INSTRUMENT_EXCHANGE_BSE = "BSE"
INSTRUMENT_EXCHANGE_NFO = "NFO"
INSTRUMENT_SEGMENT_NSE = "NSE"
INSTRUMENT_SEGMENT_BSE = "BSE"
INSTRUMENT_SEGMENT_INDICES = "INDICES"
INSTRUMENT_SEGMENT_NFO_FUT = "NFO-FUT"
INSTRUMENT_SEGMENT_NFO_OPT = "NFO-OPT"
OPTION_EXPIRTY_1MONTH="exp1month"
OPTION_EXPIRTY_2MONTH="exp2month"
OPTION_EXPIRTY_3MONTH="exp3month"

def kite_instrument_to_filename(instrument_dict, interval="day", option_expiry=OPTION_EXPIRTY_1MONTH):
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
        # figure out option expirty automatically, (while sync we let it automatic)
        # while reading the data we pass this param premade to indicate we're looking for
        # specific series
        if option_expiry is None:
            
            series_expirty_datetime = dateutil.parser.parse(instrument_dict[INSTRUMENT_KEY_EXPIRTY])
            today_datetime = datetime.now()
            last_thursdays = get_last_thursdays_between_dates(today_datetime, series_expirty_datetime)
            option_expiry = OPTION_EXPIRTY_1MONTH.replace("1", str(len(last_thursdays)))
            
        print("orig trading symbol:", instrument_dict[INSTRUMENT_KEY_TRADINGSYMBOL])
        print("orig series expiry:", instrument_dict[INSTRUMENT_KEY_EXPIRTY])
        print("OPT SYMB:",trading_symbol)
        print("OPT EXP:",option_expiry)
        trading_symbol += option_expiry
    # we've tested that _ is valid seperator by evaluating for all the instruments (86k)
    #NOTE: we removed instrument token from here as it'll cause problems with options
    #      options isntrumenttoken changes but the underlying continuous symbols would repeat
    #      and we want to see options as continuous to make most analysis
    result_file = f"{trading_symbol}_{instrument_type}_{segment}_{exchange}_{interval}.csv"
    return result_file



def sync_instrument_list():
    kite = make_kiteconnect_api()
    all_instruments = kite.instruments()
    df_all_instruments = pd.DataFrame(all_instruments)
    os.makedirs(KITE_INSTRUMENTS_DIRECTORY, exist_ok=True)
    df_all_instruments.to_csv(os.path.join(KITE_INSTRUMENTS_DIRECTORY,"instrument_list.csv"), index=False)

'''
segment: ['BCD-OPT' 'BCD-FUT' 'BCD' 'BSE' 'INDICES' 'CDS-OPT' 'CDS-FUT' 'MCX-FUT'
 'MCX-OPT' 'NFO-OPT' 'NFO-FUT' 'NSE']
instrument_type: ['CE' 'PE' 'FUT' 'EQ']
exchange: ['BCD' 'BSE' 'MCX' 'NSE' 'CDS' 'NFO']
NFO exchange instrument types: ['CE' 'PE' 'FUT']
'''
def search_instruments_in_df(instruments_df, search_result_as_copy=True, instrument_type=None, exchange=None, segment=None, force_refresh=False, symbols_contained=None, name_contained=None, symbol_eq=None, name_eq=None):
    thedf = instruments_df
    if instrument_type is not None:
        if type(instrument_type) == str:
            instrument_type = [instrument_type]
        thedf = thedf[thedf['instrument_type'].isin(instrument_type)]

    if exchange is not None:
        if type(exchange) == str:
            exchange = [exchange]
        thedf = thedf[thedf['exchange'].isin(exchange)]
    
    if segment is not None:
        if type(segment) == str:
            segment = [segment]
        thedf = thedf[thedf['segment'].isin(segment)]

    if symbol_eq is not None:
        if type(symbol_eq) == str:
            symbol_eq = [symbol_eq]
        thedf = thedf[thedf['tradingsymbol'].isin(symbol_eq)]
    
    if name_eq is not None:
        if type(name_eq) == str:
            name_eq = [name_eq]
        thedf = thedf[thedf['name'].isin(name_eq)]

    if search_result_as_copy:
        return thedf.copy().reset_index(drop=True)
    else:
        return thedf

_cache_get_instrument_list_df = None
def get_instrument_list(instrument_type=None, exchange=None, segment=None, force_refresh=False, symbols_contained=None, name_contained=None, symbol_eq=None, name_eq=None):
    if force_refresh:
        sync_instrument_list()

    global _cache_get_instrument_list_df
    if _cache_get_instrument_list_df is None or force_refresh:
        _cache_get_instrument_list_df = pd.read_csv(os.path.join(KITE_INSTRUMENTS_DIRECTORY,"instrument_list.csv"))
    thedf = _cache_get_instrument_list_df

    return search_instruments_in_df(thedf, search_result_as_copy=True, instrument_type=instrument_type, exchange=exchange, segment=segment, force_refresh=force_refresh, symbols_contained=symbols_contained, name_contained=name_contained, symbol_eq=symbol_eq, name_eq=name_eq) 

def search_options_for_instrument_in_df(thedf, instrument):
    return search_instruments_in_df(thedf, name_eq=instrument[INSTRUMENT_KEY_TRADINGSYMBOL], segment=INSTRUMENT_SEGMENT_NFO_OPT)

def get_options_for_instrument(instrument):
    return search_options_for_instrument_in_df(get_instrument_list(), instrument)

def get_instrument_by_symbol(symbol):
    return 


@ratelimit.sleep_and_retry
@ratelimit.limits(calls=3, period=1)
def throttled_historical_data_api(instrument_token, start_datetime, end_datetime, data_interval, oi=True, continuous=False):
    return make_kiteconnect_api().historical_data(instrument_token, start_datetime.strftime("%Y-%m-%d"), end_datetime.strftime("%Y-%m-%d"), data_interval, oi=oi, continuous=continuous)

def sync_instrument_history(instrument_dict, data_interval="day", fetch_past=True):
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

    # the continuous param makes api return historical data from same strike and span of expiry
    need_continuous_series = False
    option_expiry = None
    if instrument_dict[INSTRUMENT_KEY_EXCHANGE] == "NFO" and \
        (instrument_dict[INSTRUMENT_KEY_INSTRUMENT_TYPE] == "CE" or instrument_dict[INSTRUMENT_KEY_INSTRUMENT_TYPE] == "PE"):
        if data_interval == 'day':
            need_continuous_series = True

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

        historical_data = throttled_historical_data_api(instrument_dict['instrument_token'], start, end, data_interval, oi=True, continuous=need_continuous_series)
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
            historical_data = throttled_historical_data_api(instrument_dict['instrument_token'], start, end, data_interval, oi=True, continuous=need_continuous_series)

        
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

def get_instrument_history(instrument_dict, data_interval="day", option_expiry=None, force_refresh=False):
    if force_refresh:
        sync_instrument_history(instrument_dict, data_interval="day", fetch_past=False, option_expiry=option_expiry)
    result_file = kite_instrument_to_filename(instrument_dict, interval=data_interval, option_expiry=option_expiry)
    print("RESULTFILEE:", result_file)
    result_file_path = os.path.join(KITE_HISTORICAL_DIRECTORY,result_file)
    df_to_sync = None
    if os.path.exists(result_file_path):
        df_to_sync = pd.read_csv(result_file_path,index_col=False)
    return df_to_sync

if __name__ == "__main__":
    # instrument list functions
    # sync_instrument_list()
    all_instruments = get_instrument_list(segment=INSTRUMENT_EXCHANGE_NSE, instrument_type=INSTRUMENT_TYPE_EQ)
    print("Total instruments:", all_instruments.shape)
    all_instruments_options = get_instrument_list(segment=INSTRUMENT_SEGMENT_NFO_OPT)
    # for idx, instrument in all_instruments.iterrows():
    for idx, instrument in all_instruments.iterrows():
        # print(idx)
        # instrument = all_instruments.iloc[idx]
        dfoptions = search_instruments_in_df(all_instruments_options, name_eq=instrument[INSTRUMENT_KEY_TRADINGSYMBOL])
        for optidx, optrow in dfoptions.iterrows():
            print(optrow[INSTRUMENT_KEY_TRADINGSYMBOL])
            sync_instrument_history(optrow)
            theoptioninstrumentdf = get_instrument_history(optrow, option_expiry=OPTION_EXPIRTY_1MONTH)
            # print(optrow)
            print(theoptioninstrumentdf)
            break
        break
    # nifty_50_instrument = all_instruments[all_instruments['tradingsymbol']=="NIFTY 50"].iloc[0]
    
    # # sync example:
    # sync_instrument_history(nifty_50_instrument, data_interval="day")
    # sync_instrument_history(nifty_50_instrument, data_interval="15minute")

    # # get example:
    # nifty_50_df_day = get_instrument_history(nifty_50_instrument, data_interval="day", force_refresh=True)
    # nifty_50_df_15min = get_instrument_history(nifty_50_instrument, data_interval="15minute")
    # print(nifty_50_df_day.tail(3))
    

    