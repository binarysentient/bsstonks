from logging import ERROR
from kiteconnect.connect import KiteConnect
from libstonks.paper_kite_account import PaperKiteAccount
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_MINUTE, INSTRUMENT_KEY_EXCHANGE, INSTRUMENT_KEY_TRADINGSYMBOL
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
        print(ohlc_df.tail(1))
        

class BaseTradingStrategy():
    """A strategy is responsible for generating buy/sell signals"""
    def __init__(self, data_orchestrator:BSDataOrchestrator, zeordha_like_trading_account:PaperKiteAccount) -> None:
        """we might want to initialize multiple signal agents, or custom ML classes or some external libs etc.."""    
        """some hyper parameters could be added here; say for rsi strategy we want to customize the thresholds
        - `data_orchestrator` makes available live + historical data, the signature is same that of `kite_historical.get_instrument_history(...)`"""
        self.data_orchestrator = data_orchestrator
        self.trading_account = zeordha_like_trading_account

    def place_order(self, quantity=1, transaction_type=None):
        """place an order
        - `transaction_type`: BUY|SELL, KiteConnect.TRANSACTION_TYPE_BUY
        """
        buydate, price = self.data_orchestrator.get_latest_granule_date_and_price()
        self.trading_account.place_order(KiteConnect.VARIETY_REGULAR, 
                self.data_orchestrator.instrument[INSTRUMENT_KEY_EXCHANGE],
                self.data_orchestrator.instrument[INSTRUMENT_KEY_TRADINGSYMBOL],
                transaction_type,
                quantity,
                KiteConnect.PRODUCT_NRML,
                KiteConnect.ORDER_TYPE_MARKET,
                price=price,
                tag=f"timestamp_{buydate.timestamp()}")
    
    # instrument: the instrument row/instrument information
    #             expected 'tradingsymbol' key
    # instrument_data: expected price action data with 'date', ..ohlc.. , 'volume'
    def execute_step(self):
        """for given timestep execute the strategy based on the latest information the data_orchestrator gives"""
        
        pass


class RsiMidTennisTradingStrategy(BaseTradingStrategy):

    def __init__(self, data_orchestrator: BSDataOrchestrator, zeordha_like_trading_account: PaperKiteAccount) -> None:
        super().__init__(data_orchestrator, zeordha_like_trading_account)
    
    def execute_step(self):
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        datadf = self.data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_15MINUTE)
        rsi15 = INDICATORS.get_rsi_indicator(datadf['close'], length=15, mean_function="ema")

        # NOTE: never do try: catch: here, ideally we want to take care of all the possible scenarios preemptively
        if len(rsi15) <= 1:
            return
        
        last_trade_transaction_type = None
        recent_trades = self.trading_account.get_trades()
        if len(recent_trades) > 0:
            last_trade_transaction_type = recent_trades[-1]['transaction_type']

        if rsi15.iloc[-2] < 45.0 and rsi15.iloc[-1] >= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif rsi15.iloc[-2] > 45.0 and rsi15.iloc[-1] <= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_SELL:
            self.place_order(quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)


class RsiDayEndNextOpenArbitrageTradingStrategy(BaseTradingStrategy):

    def __init__(self, data_orchestrator: BSDataOrchestrator, zeordha_like_trading_account: PaperKiteAccount) -> None:
        super().__init__(data_orchestrator, zeordha_like_trading_account)
        if self.data_orchestrator.minimum_granule_interval != DATA_INTERVAL_MINUTE:
            raise Exception("This trategy only works for Minute interval")
    
    def execute_step(self):
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        # this returns minimum granule
        
        datadf = self.data_orchestrator.get_ohlc_data_df()
        
        current_datetime = datadf['date'].iloc[-1]
        current_threshold = current_datetime.hour + current_datetime.minute/60
        upper_sell_threshold = 9 + 35/60
        lower_buy_threshold = 15 + 25/60
        # print(current_datetime)
        # print(current_threshold)
        # print(upper_sell_threshold)
        # print(lower_buy_threshold)
        last_trade_transaction_type = None
        recent_trades = self.trading_account.get_trades()
        if len(recent_trades) > 0:
            last_trade_transaction_type = recent_trades[-1]['transaction_type']
        if current_threshold >= lower_buy_threshold  and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif len(recent_trades) > 0 and current_threshold <= upper_sell_threshold <= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_SELL:
            # NOTE: len(recent_trades) > 0: this condition makes sure you don't sell if you don't own
            self.place_order(quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)


def main_test():
    kite = make_kiteconnect_api()
    print(kite)
    print("---- positions ----")
    print(kite.positions())
    print("--- holdings ---")
    print(kite.holdings())

if __name__ == "__main__":
    main_test()
    