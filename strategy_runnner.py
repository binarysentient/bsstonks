import traceback
from typing import List
from typing_extensions import final
from libstonks.bs_data_orchestrator import BSDataOrchestrator
from threading import Thread
import time
from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_5MINTUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, get_instrument_history, get_instrument_list
from libstonks.trading_strategy import RsiBullishBearishSignalAgent


class StrategyRunnerThread(Thread):

    def __init__(self, instrument, data_orchestrator:BSDataOrchestrator, backtest=True) -> None:
        super().__init__(daemon=True)
        self.data_orchestrator = data_orchestrator
        self.backtest = backtest
        self.instrument = instrument
    
    def run(self) -> None:
        if self.backtest == True:
            
            for idx in self.data_orchestrator:
                print(idx)
            return
            
        while True:
            time.sleep(5)
            try:
                print("---------- day")
                print(self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_DAY))
                print("---------- minute")
                print(self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_5MINTUTE))
            except Exception as ex:
                traceback.print_exc()
                print(ex)
    


def make_runner(instrument, bskiteticker:BSKiteTicker=None) -> Thread:  
    data_orchestrator = BSDataOrchestrator(instrument, kite_ticker=bskiteticker, minimum_granule_interval=DATA_INTERVAL_DAY)
    runnerthread = StrategyRunnerThread(instrument, data_orchestrator)
    runnerthread.start()
    return runnerthread

if __name__ == "__main__":
    instrument_list = get_instrument_list(symbol_eq="RELIANCE", exchange=INSTRUMENT_EXCHANGE_NSE)
    # kite_historical.get_instrument_history(instrument_list.iloc[0]) #, force_refresh=True)
    print(instrument_list)
    # ticker = BSKiteTicker()
    # ticker.start()
    ticker = None
    runners = []
    for idx, instrument in instrument_list.iterrows():
        # print(instrument)
        # print(get_instrument_history(instrument))
        runners.append(make_runner(instrument, ticker))
    
    while len([t for t in runners if t.is_alive()]) > 0:
        time.sleep(2)
    
    