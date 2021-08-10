import traceback
from multiprocessing import Pool
from typing import List
from typing_extensions import final
from libstonks.bs_data_orchestrator import BSDataOrchestrator
from threading import Thread
import time
from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_5MINTUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, INSTRUMENT_KEY_TRADINGSYMBOL, get_instrument_history, get_instrument_list
from libstonks.trading_strategy import RsiBullishBearishSignalAgent

NIFTY50_TRADINGSYMBOLS = ['ADANIPORTS','ASIANPAINT','AXISBANK','BAJAJ-AUTO','BAJFINANCE','BAJAJFINSV','BPCL','BHARTIARTL','BRITANNIA','CIPLA','COALINDIA','DIVISLAB','DRREDDY','EICHERMOT','GRASIM','HCLTECH','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDUNILVR','HDFC','ICICIBANK','ITC','IOC','INDUSINDBK','INFY','JSWSTEEL','KOTAKBANK','LT','M&M','MARUTI','NTPC','NESTLEIND','ONGC','POWERGRID','RELIANCE','SBILIFE','SHREECEM','SBIN','SUNPHARMA','TCS','TATACONSUM','TATAMOTORS','TATASTEEL','TECHM','TITAN','UPL','ULTRACEMCO','WIPRO']

def runner_initializer(func):
    func.someparam = None

def runner_single_instrument_worker(instrument) -> Thread:  
    data_orchestrator = BSDataOrchestrator(instrument, kite_ticker=None, minimum_granule_interval=DATA_INTERVAL_DAY)
    # runnerthread = StrategyRunnerThread(instrument, data_orchestrator)
    # runnerthread.start()
    # return runnerthread
    rsi_bullishbearish_signal_agent = RsiBullishBearishSignalAgent(data_orchestrator)
    idx = 0
    for date in data_orchestrator:
        rsi_bullishbearish_signal_agent.get_signals()
        idx += 1
        if idx % 1000 == 0:
            print(instrument[INSTRUMENT_KEY_TRADINGSYMBOL], date)
    

if __name__ == "__main__":
    instrument_list = get_instrument_list(symbol_eq=NIFTY50_TRADINGSYMBOLS, exchange=INSTRUMENT_EXCHANGE_NSE)
    # kite_historical.get_instrument_history(instrument_list.iloc[0]) #, force_refresh=True)
    print(instrument_list)
    # ticker = BSKiteTicker()
    # ticker.start()
    ticker = None
    instruments_to_focus = []
    for idx, instrument in instrument_list.iterrows():
        instruments_to_focus.append(instrument.to_dict())
    runner_single_instrument_worker(instruments_to_focus[0])
    # with Pool(initializer=runner_initializer, initargs=(runner_single_instrument_worker,)) as p:
    #     p.map(runner_single_instrument_worker, instruments_to_focus[:1], chunksize=10)