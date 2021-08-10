from datetime import datetime
import logging
from threading import Thread
import time
import traceback
from typing import Dict

from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_MINUTE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, get_instrument_history

LOGGER = logging.getLogger("kite_historical")

class BSDataOrchestrator():

    def __init__(self, instrument, kite_ticker:BSKiteTicker=None, minimum_granule_interval=DATA_INTERVAL_MINUTE) -> None:
        """The data orchestrator takes care of making the data available to the strategy runner
        - `minimum_granule_interval`: same as what `DATA_INTERVAL_` 
        - `instrument_dict`: is the instrument detail; one row from which `kite_historical.get_instrument_list(...)` returns.
        That row has all the symbol/exchange etc information
        - `kite_ticker`: is optional for backtest, but for live paper test and live real trades it's mandatory
        """
        self.instrument = instrument
        self.minimum_granule_interval = minimum_granule_interval
        self.current_iteration_source = None
        self.current_iteration_index = 0
        self.ohlc_df_buffer = {}
        if kite_ticker:
            self.kite_ticker = kite_ticker
            self.kite_ticker.subscribe(instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN])
    
    def iterator_state_(self):
        print("CALLING ITER")
        return self.__iter__()

    def __iter__(self):
        self.current_iteration_source = get_instrument_history(self.instrument, self.minimum_granule_interval)
        self.current_iteration_index = -1
        return self

    def __next__(self):
        self.current_iteration_index += 1
        if len(self.current_iteration_source) <= self.current_iteration_index:
            raise StopIteration
        else:
            return_val = self.current_iteration_source.iloc[self.current_iteration_index]['date']
            return return_val
        

    def get_ohlc_data_df(self, data_interval=None) -> None:
        
        if data_interval == self.minimum_granule_interval:
            # TODO: get hostorical as well to combine
            if self.kite_ticker is not None:
                return self.kite_ticker.get_instrument_data(self.instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN], self.minimum_granule_interval)
        
        current_data_interval = self.minimum_granule_interval
        if data_interval is not None:
            current_data_interval = data_interval
        
        startdate = self.current_iteration_source.iloc[0]['date']
        enddate = self.current_iteration_source.iloc[self.current_iteration_index]['date']
        
        old_file_chronological_identity = "OldFileIdentity"
        lookup_tuple = (self.instrument, current_data_interval)
        if str(lookup_tuple) in self.ohlc_df_buffer:
            old_file_chronological_identity = self.ohlc_df_buffer[str(lookup_tuple)][1]
        
        
        new_data_df, new_file_chronological_identity = get_instrument_history(lookup_tuple[0], lookup_tuple[1], file_chronological_identity=old_file_chronological_identity)
        if new_file_chronological_identity is not None:
            self.ohlc_df_buffer[str(lookup_tuple)] = (new_data_df, new_file_chronological_identity)

        thedf = self.ohlc_df_buffer[str(lookup_tuple)][0]
        thedf = thedf[thedf['date'] >= startdate]
        thedf = thedf[thedf['date'] <= enddate]
        
        return thedf.copy().reset_index(drop=True)
    