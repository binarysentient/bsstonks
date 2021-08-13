from datetime import datetime
import logging
import os
from multiprocessing import Pool
from typing_extensions import final

from kiteconnect.connect import KiteConnect
import pandas as pd
from libstonks.paper_kite_account import PaperKiteAccount

from libstonks.bs_data_orchestrator import BSDataOrchestrator
from threading import Thread
import time

import matplotlib.pyplot as plt

from libstonks import bs_kiteconnect
from libstonks import bs_kiteticker
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.kite_historical import BSSTONKS_DIRECTORY, DATA_INTERVAL_15MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_INSTRUMENT_TOKEN, INSTRUMENT_KEY_TRADINGSYMBOL, INSTRUMENT_SEGMENT_INDICES, get_instrument_history, get_instrument_list
from libstonks.trading_strategy import BaseTradingStrategy, RsiDualThresholdTradingStrategy, RsiRynerTeo30BuyTradingStrategy, RsiMidTennisTradingStrategy, RsiDayEndNextOpenArbitrageTradingStrategy

logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

NIFTY50_TRADINGSYMBOLS = ['ADANIPORTS','ASIANPAINT','AXISBANK','BAJAJ-AUTO','BAJFINANCE','BAJAJFINSV','BPCL','BHARTIARTL','BRITANNIA','CIPLA','COALINDIA','DIVISLAB','DRREDDY','EICHERMOT','GRASIM','HCLTECH','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDUNILVR','HDFC','ICICIBANK','ITC','IOC','INDUSINDBK','INFY','JSWSTEEL','KOTAKBANK','LT','M&M','MARUTI','NTPC','NESTLEIND','ONGC','POWERGRID','RELIANCE','SBILIFE','SHREECEM','SBIN','SUNPHARMA','TCS','TATACONSUM','TATAMOTORS','TATASTEEL','TECHM','TITAN','UPL','ULTRACEMCO','WIPRO']

BACKTEST_RESULTS_DIRECTORY = os.path.join(BSSTONKS_DIRECTORY, "output","strategy_backtest_results")

def runner_initializer(func):
    func.someparam = None

def runner_single_instrument_worker(instrument, backtest_start_date, minimum_granule_interval) -> Thread:  
    data_orchestrator = BSDataOrchestrator(instrument, kite_ticker=None, minimum_granule_interval=minimum_granule_interval, backtest_start_date=backtest_start_date)
    # runnerthread = StrategyRunnerThread(instrument, data_orchestrator)
    # runnerthread.start()
    # return runnerthread
    paper_kite_account = PaperKiteAccount(500000)
    # rsi_midtennis_strat = RsiMidTennisTradingStrategy()
    # rsi_nextday_strat = RsiDayEndNextOpenArbitrageTradingStrategy()
    rsi_dual_strat = RsiDualThresholdTradingStrategy(data_interval_decision=DATA_INTERVAL_15MINUTE, lower_crossover_buy_threshold=40, upper_crossover_sell_threshold=60)
    # rsi_rynerteo30buy_strat = RsiRynerTeo30BuyTradingStrategy()
    idx = 0
    current_strategy = rsi_dual_strat
    
    # for _ in range(0,3):
    #     starttime = time.time()
    for date in data_orchestrator:
        current_strategy.execute_step(data_orchestrator, paper_kite_account)
        
        # print(f"Took seconds: {time.time()-starttime:.1f}")
    analyze_strategy(current_strategy, data_orchestrator, paper_kite_account)
    
        
def analyze_strategy(thestrategy:BaseTradingStrategy, data_orchestrator:BSDataOrchestrator, trading_account:PaperKiteAccount):
    strategy_path = os.path.join(BACKTEST_RESULTS_DIRECTORY,str(thestrategy))
    os.makedirs(strategy_path,exist_ok=True)

    thetrades = trading_account.get_trades()
    if len(thetrades) < 1:
        return
    if thetrades[0]['transaction_type'] == KiteConnect.TRANSACTION_TYPE_SELL:
        thetrades[:] = thetrades[1:]
    
    pnl = []
    pnl_cum_points = []
    pnl_cum = 0
    dates = []
    thecsv_analysis = []
    for buyt, sellt in zip(thetrades[:-1:2], thetrades[1::2]):
        
        pnl.append((sellt['price'] - buyt['price'])/buyt['price']*100)
        
        curr_pnl_points = (sellt['price'] - buyt['price'])
        pnl_cum += curr_pnl_points
        pnl_cum_points.append(pnl_cum)
        dates.append(sellt['transaction_datetime'])
        thecsv_analysis.append({
            'stocksymbol':data_orchestrator.instrument[INSTRUMENT_KEY_TRADINGSYMBOL],
            'buy_datetime':buyt['transaction_datetime'],
            'sell_datetime':sellt['transaction_datetime'],
            'buy_price':buyt['price'],
            'sell_price':sellt['price'],
            'trade_pnl_points':curr_pnl_points,
            'cumulative_pnl_points':pnl_cum
        })
    print(os.path.join(strategy_path, f"{data_orchestrator}.csv"))
    pd.DataFrame(thecsv_analysis).to_csv(os.path.join(strategy_path, f"{data_orchestrator}.csv"))
    # return
    plt.figure(figsize=(18,12))
    plt.title(f"trades:{len(pnl)} , wins:{len([x for x in pnl if x>0])}, losses:{len([x for x in pnl if x<=0])}")
    plt.plot(dates, pnl)
    plt.plot(dates, [0 for x in pnl])
    plt.savefig(os.path.join(strategy_path, f"per_trade_gain_percent_{data_orchestrator}.png"))

    plt.figure(figsize=(18,12))
    plt.title(f"trades:{len(pnl)} , wins:{len([x for x in pnl if x>0])}, losses:{len([x for x in pnl if x<=0])}")
    plt.plot(dates, pnl_cum_points)
    plt.savefig(os.path.join(strategy_path, f"pnl_points_cumulative_{data_orchestrator}.png"))


if __name__ == "__main__":
    minimum_granule_interval = DATA_INTERVAL_15MINUTE
    BACKTEST_START_DATE = datetime(2021,8,1)
    # BACKTEST_START_DATE = datetime(2021,7,12)
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
        # print(get_instrument_history(instrument, data_interval=DATA_INTERVAL_MINUTE))
        instruments_to_focus.append((instrument.to_dict(), BACKTEST_START_DATE, minimum_granule_interval))
    runner_single_instrument_worker(*instruments_to_focus[0])
    # with Pool(initializer=runner_initializer, initargs=(runner_single_instrument_worker,)) as p:
    #     p.starmap(runner_single_instrument_worker, instruments_to_focus[:1], chunksize=10)