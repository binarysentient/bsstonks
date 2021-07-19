# hack to import parent utilities
import sys
from dateutil.tz.tz import tzoffset
sys.path.append('..')
from multiprocessing import Pool


from datetime import datetime, timedelta

from scipy import stats
import pandas as pd
import numpy as np

from libstonks import kite_historical

def load_instrument_data_between_dates(instrument_row,start_date,end_date):
    tradingsymbol = instrument_row[kite_historical.INSTRUMENT_KEY_TRADINGSYMBOL]
    print(tradingsymbol)
    instrument_history = kite_historical.get_instrument_history(instrument_row,data_interval="day")
    if instrument_history is None or len(instrument_history) == 0:
        return None, None
    instrument_history = instrument_history[(instrument_history['date']>=start_date) & (instrument_history['date']<=end_date)]
    return tradingsymbol, instrument_history.copy().reset_index(drop=True)

def main():
    instruments_nse_df = kite_historical.get_instrument_list(exchange=kite_historical.INSTRUMENT_EXCHANGE_NSE, segment=kite_historical.INSTRUMENT_SEGMENT_NSE)
    end_date = datetime.now(tzoffset("india",5.5*60*60)) - timedelta(days=20)
    start_date = end_date - timedelta(days=252*2) # about 2 years

    all_instruments=[]
    # instruments_nse_df = instruments_nse_df.iloc[:100]
    for idx_instrument, instrument_row in instruments_nse_df.iterrows():
        all_instruments.append(instrument_row.to_dict())

    eligible_df_list = {}
    with Pool(None) as p:
        packed_instrument_and_instrumentdf = p.starmap(load_instrument_data_between_dates, zip(all_instruments,[start_date for _ in all_instruments], [end_date for _ in all_instruments]), chunksize=20)
        for instrument,instrumentdf in packed_instrument_and_instrumentdf:
            if instrument is not None:
                eligible_df_list[instrument] = instrumentdf


    mean = np.mean([len(eligible_df_list[key]) for key in eligible_df_list.keys()])
    std = np.std([len(eligible_df_list[key]) for key in eligible_df_list.keys()])
    mode = stats.mode([len(eligible_df_list[key]) for key in eligible_df_list.keys()])
    print("Mode:", mode, "Mean:", mean, "Stddev:", std)

if __name__ == "__main__":
    main()