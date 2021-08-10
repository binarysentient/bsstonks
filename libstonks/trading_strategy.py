from libstonks.kite_historical import INSTRUMENT_KEY_TRADINGSYMBOL
from typing import Optional
from libstonks.bs_kiteconnect import make_kiteconnect_api
from libstonks.indicators import INDICATORS
from libstonks.bs_data_orchestrator import BSDataOrchestrator
import pandas as pd
from enum import Enum

class BaseTradingSignalAgent():
    """This could very well be a strategy; but some units like say rsi indicator base bull/bear prediction
    could be used in multiple places; to combine signals and generate a strategy on top of it
    """
    def __init__(self, data_orchestrator:BSDataOrchestrator) -> None:
        """some hyper parameters could be added here; say for rsi strategy we want to customize the thresholds
        - `data_orchestrator` makes available live + historical data, the signature is same that of `kite_historical.get_instrument_history(...)`"""
        self.data_orchestrator = data_orchestrator
    
    def get_signals(self):
        """A single """
        ohlc_df = self.data_orchestrator.get_ohlc_data_df()
        # get_indicator = INDICATORS.create_get_indicator_func(ohlc_df)
        pass


class BullishBearishSignal(Enum):
    bearish = -2
    slight_bearish = -1
    neutral = 0
    slight_bullish = 1
    bullish = 2

class RsiBullishBearishSignalAgent(BaseTradingSignalAgent):
    """An RSI based predictor of oversold/overbought
    - concept: when rsi crosses below lower threshold; it's oversold; but that itself doesn't make anything happen
    it's when the rsi start climbing above the lower threshold it's said to have caught bullish momentum
    """
    def __init__(self, data_orchestrator:BSDataOrchestrator, lower_threshold=30, higher_threshold=70) -> None:
        super().__init__(data_orchestrator)

    def get_signals(self) -> pd.Series:
        ohlc_df = self.data_orchestrator.get_ohlc_data_df()
        indicator_func = INDICATORS.create_get_indicator_func(ohlc_df)
        rsi_series = indicator_func("rsi",{'length':5})
        print(ohlc_df)
        

class BaseTradingStrategy():
    """A strategy is responsible for generating buy/sell signals"""
    def __init__(self) -> None:
        """we might want to initialize multiple signal agents, or custom ML classes or some external libs etc.."""    
        pass

    # instrument: the instrument row/instrument information
    #             expected 'tradingsymbol' key
    # instrument_data: expected price action data with 'date', ..ohlc.. , 'volume'
    def get_order_signals(self, instrument, instrument_data):
        pass



def main_test():
    kite = make_kiteconnect_api()
    print(kite)
    print("---- positions ----")
    print(kite.positions())
    print("--- holdings ---")
    print(kite.holdings())

if __name__ == "__main__":
    main_test()
    