import traceback
from typing_extensions import final
from libstonks.bs_data_orchestrator import BSDataOrchestrator
from threading import Thread
import time
from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, get_instrument_history, get_instrument_list
from libstonks import trading_strategy

class DataOrchestrator():

    def __init__(self, use_ticker=False, ) -> None:
        pass
    
    def get_instrument_data() -> None:
        pass

    # def 

class RunnerThread(Thread):

    def __init__(self, instrument, backtest=True) -> None:
        super().__init__(daemon=True)
        self.ticker = BSKiteTicker()
        self.ticker.start()
        self.ticker.subscribe(instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN])
        self.instrument = instrument
    
    def run(self) -> None:
        while True:
            time.sleep(5)
            try:
                self.ticker.get_instrument_data(self.instrument[INSTRUMENT_KEY_INSTRUMENT_TOKEN], DATA_INTERVAL_MINUTE)
            except Exception as ex:
                traceback.print_exc()
                print(ex)
    


def runner(instrument):
    runnerthread = RunnerThread(instrument)
    runnerthread.start()

if __name__ == "__main__":
    instrument_list = get_instrument_list(symbol_eq="RELIANCE", exchange=INSTRUMENT_EXCHANGE_NSE)
    # kite_historical.get_instrument_history(instrument_list.iloc[0]) #, force_refresh=True)
    print(instrument_list)
    for idx, instrument in instrument_list.iterrows():
        print(instrument)
        print(get_instrument_history(instrument))
        runner(instrument)
    while True:
        time.sleep(10)
    
    