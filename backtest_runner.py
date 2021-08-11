from datetime import datetime

from kiteconnect.connect import KiteConnect
from libstonks.paper_kite_account import PaperKiteAccount
import traceback
from multiprocessing import Pool
from typing import List
from typing_extensions import final
from libstonks.bs_data_orchestrator import BSDataOrchestrator
from threading import Thread
import time

import matplotlib.pyplot as plt

from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, INSTRUMENT_KEY_TRADINGSYMBOL, INSTRUMENT_SEGMENT_INDICES, get_instrument_history, get_instrument_list
from libstonks.trading_strategy import RsiMidTennisTradingStrategy

NIFTY50_TRADINGSYMBOLS = ['ADANIPORTS','ASIANPAINT','AXISBANK','BAJAJ-AUTO','BAJFINANCE','BAJAJFINSV','BPCL','BHARTIARTL','BRITANNIA','CIPLA','COALINDIA','DIVISLAB','DRREDDY','EICHERMOT','GRASIM','HCLTECH','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDUNILVR','HDFC','ICICIBANK','ITC','IOC','INDUSINDBK','INFY','JSWSTEEL','KOTAKBANK','LT','M&M','MARUTI','NTPC','NESTLEIND','ONGC','POWERGRID','RELIANCE','SBILIFE','SHREECEM','SBIN','SUNPHARMA','TCS','TATACONSUM','TATAMOTORS','TATASTEEL','TECHM','TITAN','UPL','ULTRACEMCO','WIPRO']

def runner_initializer(func):
    func.someparam = None

def runner_single_instrument_worker(instrument, backtest_start_date) -> Thread:  
    data_orchestrator = BSDataOrchestrator(instrument, kite_ticker=None, minimum_granule_interval=DATA_INTERVAL_5MINUTE, backtest_start_date=backtest_start_date)
    # runnerthread = StrategyRunnerThread(instrument, data_orchestrator)
    # runnerthread.start()
    # return runnerthread
    paper_kite_account = PaperKiteAccount(500000)
    rsi_midtennis_strat = RsiMidTennisTradingStrategy(data_orchestrator, paper_kite_account)
    idx = 0
    for date in data_orchestrator:
        # gdate, price = data_orchestrator.get_latest_granule_date_and_price()
        # print("-")
        # print(data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_DAY).tail(3))
        # print(gdate, '->', price)
        rsi_midtennis_strat.execute_step()

    thetrades = paper_kite_account.get_trades()
    if thetrades[0]['transaction_type'] == KiteConnect.TRANSACTION_TYPE_SELL:
        thetrades[:] = thetrades[1:]
    
    pnl = []
    pnl_cum_points = []
    pnl_cum = 0
    dates = []
    for buyt, sellt in zip(thetrades[:-1], thetrades[1:]):
        pnl.append((sellt['price'] - buyt['price'])/buyt['price']*100)
        pnl_cum += (sellt['price'] - buyt['price'])
        pnl_cum_points.append(pnl_cum)
        dates.append(sellt['transaction_datetime'])
    
    plt.figure()
    plt.title(f"trades:{len(pnl)} , wins:{len([x for x in pnl if x>0])}, losses:{len([x for x in pnl if x<=0])}")
    plt.plot(dates, pnl)
    plt.plot(dates, [0 for x in pnl])
    plt.show()

    plt.figure()
    plt.title(f"trades:{len(pnl)} , wins:{len([x for x in pnl if x>0])}, losses:{len([x for x in pnl if x<=0])}")
    plt.plot(dates, pnl_cum_points)
    plt.show()

    plt.figure()
    plt.title(f"trades:{len(pnl)} , wins:{len([x for x in pnl if x>0])}, losses:{len([x for x in pnl if x<=0])}")
    plt.scatter(dates, pnl_cum_points)
    plt.show()
        
    

if __name__ == "__main__":

    BACKTEST_START_DATE = datetime(2020,10,1)
    # instrument_list = get_instrument_list(symbol_eq=NIFTY50_TRADINGSYMBOLS, exchange=INSTRUMENT_EXCHANGE_NSE)

    instrument_list = get_instrument_list(segment=INSTRUMENT_SEGMENT_INDICES, symbol_eq="NIFTY 50")
    # kite_historical.get_instrument_history(instrument_list.iloc[0]) #, force_refresh=True)
    print(instrument_list)
    # exit()
    # ticker = BSKiteTicker()
    # ticker.start()
    ticker = None
    instruments_to_focus = []
    for idx, instrument in instrument_list.iterrows():
        instruments_to_focus.append((instrument.to_dict(), BACKTEST_START_DATE))
    runner_single_instrument_worker(*instruments_to_focus[0])
    # with Pool(initializer=runner_initializer, initargs=(runner_single_instrument_worker,)) as p:
    #     p.starmap(runner_single_instrument_worker, instruments_to_focus[:1], chunksize=10)