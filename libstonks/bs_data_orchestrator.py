from threading import Thread
import time
from typing import Dict
from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks import kite_historical


class BSDataOrchestrator():

    def __init__(self, instrument_dict:Dict, minimum_granule_interval="minute", use_ticker=False, ) -> None:
        """The data orchestrator takes care of making the data available to the strategy runner
        - `minimum_granule_interval`: same as what `DATA_INTERVAL_` 
        - `instrument_dict`: is the instrument detail; one row from which `kite_historical.get_instrument_list(...)` returns.
        That row has all the symbol/exchange etc information
        """
        self.instrument_dict = instrument_dict
    
    def get_ohlc_data_df(self, data_interval=None) -> None:
        current_data_interval = self.minimum_granule_interval
        if data_interval is not None:
            current_data_interval = data_interval
        return kite_historical.get_instrument_history(self.instrument_dict, current_data_interval)


    