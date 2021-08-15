from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

from kiteconnect.connect import KiteConnect
from libstonks.indicators import INDICATORS
from libstonks.bs_kiteticker import BSKiteTicker
from libstonks.bs_data_orchestrator import BSDataOrchestrator
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_5MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_EXCHANGE_NSE, INSTRUMENT_KEY_TRADINGSYMBOL, INSTRUMENT_SEGMENT_NFO_FUT, INSTRUMENT_SEGMENT_NSE, INSTRUMENT_TYPE_EQ, get_instrument_history, get_instrument_list, search_instruments_in_df
from libstonks.trading_strategy import BaseTradingStrategy
from libstonks.paper_kite_account import PaperKiteAccount
ticker = BSKiteTicker()
ticker.start()


class Rsi45DostStrategy(BaseTradingStrategy):

    def __init__(self, data_orchestrator: BSDataOrchestrator, zeordha_like_trading_account: PaperKiteAccount) -> None:
        super().__init__(data_orchestrator, zeordha_like_trading_account)
    
    def execute_step(self):
        # runnindf = self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_MINUTE)
        df15min = self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_15MINUTE)
        # dfday = self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_DAY)
        rsi15 = INDICATORS.get_rsi_indicator(df15min['close'], length=15)
        print(df15min.tail(3), rsi15.tail(3))
        if len(rsi15) < 2:
            return
        if rsi15.iloc[-2] <= 45 and rsi15.iloc[-1] > 45 and self.get_most_recent_trade_trasaction_type() != KiteConnect.TRANSACTION_TYPE_BUY:
            # print("BUY")
            self.place_order(quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY, verbose=True)
        if rsi15.iloc[-2] >= 45 and rsi15.iloc[-1] < 45 and self.get_most_recent_trade_trasaction_type() == KiteConnect.TRANSACTION_TYPE_BUY:
            # print("SELL")
            self.place_order(quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL, verbose=True)
    
def backtest_experiment():
    instruments = get_instrument_list(exchange=INSTRUMENT_EXCHANGE_NSE, symbol_eq=["RELIANCE","INFY"])
    print(instruments)
    
    for idx, instrument in instruments.iterrows():
        paperkite = PaperKiteAccount(500000)
        # Backtest
        # orchestrator = BSDataOrchestrator(instrument, kite_ticker=None, minimum_granule_interval=DATA_INTERVAL_MINUTE)
        # Real Paper
        orchestrator = BSDataOrchestrator(instrument, kite_ticker=ticker, minimum_granule_interval=DATA_INTERVAL_MINUTE)
        strat = Rsi45DostStrategy(orchestrator, paperkite)
        
        # Real Real
        # strat = Rsi45DostStrategy(orchestrator, kiteconnect)
        for _ in orchestrator:
            strat.execute_step()
            # thedf = orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_15MINUTE)
            # print(thedf.tail(5))
            # input()
    # ticker.start()

def futures_experiment():
    instruments_reliance = get_instrument_list(symbol_contained="RELIANCE")    
    print(instruments_reliance)
    reliance_eq = search_instruments_in_df(instruments_reliance, segment=INSTRUMENT_SEGMENT_NSE)
    reliance_fut = search_instruments_in_df(instruments_reliance, segment=INSTRUMENT_SEGMENT_NFO_FUT)

    
    print(reliance_eq)
    print(reliance_fut)

    focus_df = pd.concat([reliance_eq, reliance_fut], ignore_index=True)
    print(focus_df)
    for idx, foc_instrument in focus_df.iterrows():
        dfh = get_instrument_history(foc_instrument, start_datetime=datetime(2018,1,1))
        plt.plot(dfh['date'], dfh['close'], label=foc_instrument[INSTRUMENT_KEY_TRADINGSYMBOL])
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # backtest_experiment()
    futures_experiment()