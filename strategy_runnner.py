import traceback
from typing import List
from typing_extensions import final
from libstonks.bs_data_orchestrator import BSDataOrchestrator, LOGGER
from threading import Thread
import time
from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, INSTRUMENT_KEY_TRADINGSYMBOL, get_instrument_history, get_instrument_list
from libstonks.trading_strategy import RsiBullishBearishSignalAgent


class StrategyRunnerThread(Thread):

    def __init__(self, instrument, data_orchestrator:BSDataOrchestrator, backtest=False) -> None:
        super().__init__(daemon=True)
        self.data_orchestrator = data_orchestrator
        self.backtest = backtest
        self.instrument = instrument
    
    def run(self) -> None:
            
        for current_tick_date in self.data_orchestrator:
            print(current_tick_date)
            try:
                # print("---------- day")

                # print(self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_DAY))
                LOGGER.debug(f"---------- 15 minute: {self.instrument[INSTRUMENT_KEY_TRADINGSYMBOL]}")
                LOGGER.debug(self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_15MINUTE).tail(5))
            except Exception as ex:
                traceback.print_exc()
                print(ex)
    


def make_runner(instrument, bskiteticker:BSKiteTicker=None) -> Thread:  
    data_orchestrator = BSDataOrchestrator(instrument, kite_ticker=bskiteticker, minimum_granule_interval=DATA_INTERVAL_MINUTE)
    runnerthread = StrategyRunnerThread(instrument, data_orchestrator)
    runnerthread.start()
    return runnerthread

if __name__ == "__main__":
    NIFTY50_TRADINGSYMBOLS = ['ADANIPORTS','ASIANPAINT','AXISBANK','BAJAJ-AUTO','BAJFINANCE','BAJAJFINSV','BPCL','BHARTIARTL','BRITANNIA','CIPLA','COALINDIA','DIVISLAB','DRREDDY','EICHERMOT','GRASIM','HCLTECH','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDUNILVR','HDFC','ICICIBANK','ITC','IOC','INDUSINDBK','INFY','JSWSTEEL','KOTAKBANK','LT','M&M','MARUTI','NTPC','NESTLEIND','ONGC','POWERGRID','RELIANCE','SBILIFE','SHREECEM','SBIN','SUNPHARMA','TCS','TATACONSUM','TATAMOTORS','TATASTEEL','TECHM','TITAN','UPL','ULTRACEMCO','WIPRO']

    # instrument_list = get_instrument_list(symbol_eq="RELIANCE", exchange=INSTRUMENT_EXCHANGE_NSE)
    instrument_list = get_instrument_list(symbol_eq=NIFTY50_TRADINGSYMBOLS, exchange=INSTRUMENT_EXCHANGE_NSE)
    # kite_historical.get_instrument_history(instrument_list.iloc[0]) #, force_refresh=True)
    print(instrument_list)
    ticker = BSKiteTicker()
    ticker.start()
    # ticker = None
    runners = []
    for idx, instrument in instrument_list.iterrows():
        # print(instrument)
        # print(get_instrument_history(instrument))
        runners.append(make_runner(instrument, ticker))
    
    while len([t for t in runners if t.is_alive()]) > 0:
        time.sleep(2)
    
    