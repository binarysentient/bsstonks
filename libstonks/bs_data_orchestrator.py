from datetime import datetime
import logging
from threading import Thread
import time
import traceback
from typing import Dict

from libstonks.bs_kiteticker import BSKiteTicker, DATA_INTERVAL_TICK
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_30MINUTE, DATA_INTERVAL_3MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_60MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, get_instrument_history, get_instrument_history_chronological_identity

LOGGER = logging.getLogger("kite_historical")

class BSDataOrchestrator():

    def __init__(self, instrument, kite_ticker:BSKiteTicker=None, minimum_granule_interval=DATA_INTERVAL_MINUTE, backtest_start_date:datetime=None) -> None:
        """The data orchestrator takes care of making the data available to the strategy runner
        - `minimum_granule_interval`: same as what `DATA_INTERVAL_` 
        - `instrument_dict`: is the instrument detail; one row from which `kite_historical.get_instrument_list(...)` returns.
        That row has all the symbol/exchange etc information
        - `kite_ticker`: is optional for backtest, but for live paper test and live real trades it's mandatory
        - `backtest_start_date` : we start the iterations from here
        """
        self.instrument = instrument
        self.minimum_granule_interval = minimum_granule_interval

        # iteration related attributes
        self.current_iteration_source = None
        self.current_iteration_index = 0
        self.current_live_minimum_granule_df = None
        
        self.backtest_start_date = backtest_start_date

        self.ohlc_df_buffer = {}
        self.kite_ticker = None
        if kite_ticker:
            self.kite_ticker = kite_ticker
            self.kite_ticker.subscribe(instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN])

        # get_instrument_history(self.instrument, data_interval=self.minimum_granule_interval, force_refresh=True, parse_date=False)
    
    def __iterate_for_backtest(self):
        self.current_iteration_source = self.get_buffered_instrument_history(self.minimum_granule_interval, start_datetime=self.backtest_start_date)
        
        self.current_iteration_index = -1
        
    
    def __iterate_cleanup_backtest(self):
        self.current_iteration_source = None
        self.current_iteration_index = 0
    
    def __iterate_for_live(self):
        self.current_live_minimum_granule_df =  self.kite_ticker.get_instrument_data(self.instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN], self.minimum_granule_interval)
        

    def __iter__(self):
        if self.kite_ticker is not None:
            self.__iterate_for_live()
            return self
        else:
            self.__iterate_for_backtest()
            return self
        

    def __next__(self):
        if self.kite_ticker is not None:
            # live iterations
            while True:
                new_live_df =  self.kite_ticker.get_instrument_data(self.instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN], self.minimum_granule_interval)
                if self.current_live_minimum_granule_df  is None:
                    self.current_live_minimum_granule_df  = new_live_df
                if new_live_df is not None and len(new_live_df) > len(self.current_live_minimum_granule_df) and len(new_live_df) > 1:
                    self.current_live_minimum_granule_df = new_live_df
                    return self.current_live_minimum_granule_df
                time.sleep(1)
            # TODO: raise stopiteration when time exceed today's 3:30
        else:
            # backtest iterations
            self.current_iteration_index += 1
            if len(self.current_iteration_source) <= self.current_iteration_index:
                self.__iterate_cleanup_backtest()
                raise StopIteration
            else:
                return_val = self.current_iteration_source.loc[self.current_iteration_index]['date']
                # return_val = "xx"
                return return_val
        

    def get_latest_granule_date_and_price(self) -> tuple:
        """use this function to get the latest factual price data
        - this is useful because say minimum granule is 1 min and we trade analytics at 15 min then
        the historical backtest data for 15 min will have future info for that timespan (15 min candle is left assembled)
        """
        latest_df = self.get_ohlc_data_df(data_interval=self.minimum_granule_interval)
        latest_row = latest_df.iloc[-1]
        return (latest_row['date'], latest_row['close'])

    def interval_to_seconds(self, data_interval=None):
        if data_interval:
            return {
                DATA_INTERVAL_TICK: 1,
                DATA_INTERVAL_MINUTE: 1*60,
                DATA_INTERVAL_3MINUTE: 3*60,
                DATA_INTERVAL_5MINUTE: 5*60,
                DATA_INTERVAL_15MINUTE: 15*60,
                DATA_INTERVAL_30MINUTE: 30*60,
                DATA_INTERVAL_60MINUTE: 60*60,
                DATA_INTERVAL_DAY: 24*60*60
            }[data_interval]

    def get_buffered_instrument_history(self, data_interval, start_datetime=None):
        
        old_file_chronological_identity = None
        lookup_tuple = (self.instrument, data_interval, start_datetime)
        if str(lookup_tuple) in self.ohlc_df_buffer:
            old_file_chronological_identity = self.ohlc_df_buffer[str(lookup_tuple)][1]
        
        if old_file_chronological_identity is None:
            new_data_df = get_instrument_history(lookup_tuple[0], lookup_tuple[1], start_datetime=lookup_tuple[2])
            new_file_chronological_identity = get_instrument_history_chronological_identity(lookup_tuple[0], lookup_tuple[1])
            if new_file_chronological_identity is not None:
                self.ohlc_df_buffer[str(lookup_tuple)] = (new_data_df, new_file_chronological_identity)
        elif self.kite_ticker is not None:
            # Presence of kite_ticker indicates this is realtime scenario and not a backtest
            # NOTE: checking for file update makes sense for runtime, but for backtest it just slows things down due to lot of io calls
            new_file_chronological_identity = get_instrument_history_chronological_identity(lookup_tuple[0], lookup_tuple[1])
            if old_file_chronological_identity != new_file_chronological_identity:
                new_data_df = get_instrument_history(lookup_tuple[0], lookup_tuple[1], start_datetime=lookup_tuple[2])
                self.ohlc_df_buffer[str(lookup_tuple)] = (new_data_df, new_file_chronological_identity)
        
        thedf = self.ohlc_df_buffer[str(lookup_tuple)][0]
        return thedf

    def get_ohlc_data_df(self, data_interval=None, start_datetime=None) -> None:  
        
        current_data_interval = self.minimum_granule_interval
        if data_interval is not None:
            current_data_interval = data_interval
        
        if self.current_iteration_source is not None:
            # backtest scenario
            if current_data_interval == self.minimum_granule_interval:
                thedf = self.current_iteration_source.loc[:self.current_iteration_index+1]
                #NOTE: the copy was there to serve INDICATOR system, where cascading indicators are possible
                #      this copy would slow things down a lot;so Indicators now will have to change and behave as helper function
                #      rather than full fledged framework
                # return thedf.copy().reset_index(drop=True)
                return thedf #.copy().reset_index(drop=True)
            # for the runtime scenario, when minimum granule is 1 minute and we do strategy on a 15 min, then 15 min has future knowlege
            # as ohlc candles are left labeled
            elif self.interval_to_seconds(self.minimum_granule_interval) < self.interval_to_seconds(current_data_interval):

                thedf = self.get_buffered_instrument_history(current_data_interval, start_datetime = start_datetime)

                # startdate = self.current_iteration_source.loc[0]['date']
                enddate = self.current_iteration_source.loc[self.current_iteration_index]['date']
                
                
                # thedf = thedf[thedf['date'] >= startdate]
                thedf = thedf[thedf['date'] <= enddate]
                
                bigger_last_candle_datetime = thedf['date'].iloc[-1]
                thedf = thedf.set_index('date')
                # only the last index of bigger timeframe needs live data
                livemimicdf = self.current_iteration_source.iloc[:self.current_iteration_index+1]
                livemimicdf = livemimicdf[livemimicdf['date']>=bigger_last_candle_datetime]
                livemimicdf = livemimicdf.set_index('date').resample(BSKiteTicker.get_resample_map()[current_data_interval]).agg({'open':'first','high':'max','low':'min', 'close':'last'}).reset_index()
                thedf.loc[livemimicdf.iloc[-1]['date']] = livemimicdf.iloc[-1]
                thedf = thedf.reset_index()

                return thedf #.copy().reset_index(drop=True)
        

        elif self.kite_ticker is not None:
            # real trade scenario
            
            thedf = self.get_buffered_instrument_history(current_data_interval, start_datetime = start_datetime)

            new_data_df = self.kite_ticker.get_instrument_data(self.instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN], current_data_interval)
            thedf = thedf.append(new_data_df)
        
            return thedf #.copy().reset_index(drop=True)
    