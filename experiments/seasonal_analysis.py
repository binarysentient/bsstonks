# hack to import parent utilities
from os import name
import sys
from dateutil.tz.tz import tzoffset
sys.path.append('..')
from multiprocessing import Pool


from datetime import datetime, timedelta

from scipy import stats
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from libstonks import kite_historical

def seasonalize_instrument(instrument_row):
    tradingsymbol = instrument_row[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL]
    print(tradingsymbol)
    instrument_history = kite_historical.get_instrument_history(instrument_row,data_interval="day")
    if instrument_history is None or len(instrument_history) == 0:
        return None, None
    year_col = instrument_history['date'].apply(lambda x: x.year)
    instrument_history['year'] = year_col
    month_day_col = instrument_history['date'].apply(lambda x: int(x.strftime('%j')))
    instrument_history['month_day'] = month_day_col
    instrument_history['close_pct'] = instrument_history['close'] + 0
    instrument_history['close_pct_ref_0'] = instrument_history['close'] + 0
    instrument_history['close_pct_ref_50'] = instrument_history['close'] + 0
    
    
    
    for groupidx, group_data in instrument_history.groupby(["year"], as_index=False):
        
        pct_chnge = group_data['close'].pct_change()
        instrument_history.loc[pct_chnge.index, 'close_pct'] = pct_chnge
        # print("GIDX:", groupidx)
        # print(pct_chnge.index)
        # print(group_data.index)
        # input()
        # instrument_history.loc[idxg,'close_pct']
        close_ref_0 = group_data.iloc[0]['close']
        close_ref_50 = group_data.iloc[50]['close']
        for idxg, rowg in group_data.iterrows():
            instrument_history.loc[idxg,'close_pct_ref_0'] = (close_ref_0/rowg['close'] - 1.0)*100.0
            instrument_history.loc[idxg,'close_pct_ref_50'] = (close_ref_50/rowg['close'] - 1.0)*100.0
        
    return tradingsymbol, instrument_history.copy().reset_index(drop=True)

def main():
    instruments_nse_df = kite_historical.get_instrument_list(exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE, segment=kite_historical.INSTRUMENT_SEGMENT_NSE)
    interested_stocks = kite_historical.get_instrument_list(symbol_eq=["KSCL","RELIANCE"], exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE)
    end_date = datetime.now(tzoffset("india",5.5*60*60)) - timedelta(days=20)
    start_date = end_date - timedelta(days=252*2) # about 2 years

    all_instruments=[]
    # instruments_nse_df = instruments_nse_df.iloc[:100]
    for idx_instrument, instrument_row in interested_stocks.iterrows():
        # all_instruments.append(instrument_row.to_dict())
        seasonal_meta, seasonal_df = seasonalize_instrument(instrument_row)
        for price_col in ['close','close_pct','close_pct_ref_0','close_pct_ref_50']:
            fig = plt.figure()
            for groupidx, group_data in seasonal_df.groupby(["year"]):
                plt.plot(group_data['month_day'], group_data[price_col], label=f"{groupidx}")
            plt.legend()
            plt.show()

        
    exit()
    eligible_df_list = {}
    with Pool(None) as p:
        packed_instrument_and_instrumentdf = p.starmap(load_instrument_data_between_dates, zip(all_instruments,[start_date for _ in all_instruments], [end_date for _ in all_instruments]), chunksize=20)
        for instrument,instrumentdf in packed_instrument_and_instrumentdf:
            if instrument is not None:
                eligible_df_list[instrument] = instrumentdf


    print("Mode:", mode, "Mean:", mean, "Stddev:", std)

if __name__ == "__main__":
    main()