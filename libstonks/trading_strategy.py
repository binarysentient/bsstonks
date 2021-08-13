from logging import ERROR
from kiteconnect.connect import KiteConnect
from libstonks.paper_kite_account import PaperKiteAccount
from libstonks.kite_historical import DATA_INTERVAL_15MINUTE, DATA_INTERVAL_DAY, DATA_INTERVAL_MINUTE, INSTRUMENT_KEY_EXCHANGE, INSTRUMENT_KEY_TRADINGSYMBOL
from typing import Optional
from libstonks.bs_kiteconnect import make_kiteconnect_api
from libstonks.indicators import INDICATORS
from libstonks.bs_data_orchestrator import BSDataOrchestrator
import pandas as pd
from enum import Enum


class BullishBearishSignal(Enum):
    bearish = -2
    slight_bearish = -1
    neutral = 0
    slight_bullish = 1
    bullish = 2

        

class BaseTradingStrategy():
    """A strategy is responsible for generating buy/sell signals"""
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return f"{self.__class__.__name__}"

    def place_order(self, data_orchestrator, trading_account, quantity=1, transaction_type=None, verbose=True):
        """helper function to place an order
        - `transaction_type`: BUY|SELL, KiteConnect.TRANSACTION_TYPE_BUY
        """
        buydate, price = data_orchestrator.get_latest_granule_date_and_price()
        trading_account.place_order(KiteConnect.VARIETY_REGULAR, 
                data_orchestrator.instrument[INSTRUMENT_KEY_EXCHANGE],
                data_orchestrator.instrument[INSTRUMENT_KEY_TRADINGSYMBOL],
                transaction_type,
                quantity,
                KiteConnect.PRODUCT_NRML,
                KiteConnect.ORDER_TYPE_MARKET,
                price=price,
                tag=f"timestamp_{buydate.timestamp()}", verbose=verbose)
    
    def get_most_recent_trade(self, trading_account):
        recent_trades = trading_account.get_trades()
        if len(recent_trades) > 0:
            return recent_trades[-1]
    
    def get_most_recent_trade_trasaction_type(self, trading_account):
        most_recent_trade = self.get_most_recent_trade(trading_account)
        if most_recent_trade:
            return most_recent_trade['transaction_type']

    def get_pnl_points(self,data_orchestrator, trading_account):
        most_recent_trade = self.get_most_recent_trade(trading_account)
        if most_recent_trade:
            return data_orchestrator.get_latest_granule_date_and_price()[1] - most_recent_trade['price']
        return None
    # instrument: the instrument row/instrument information
    #             expected 'tradingsymbol' key
    # instrument_data: expected price action data with 'date', ..ohlc.. , 'volume'
    def execute_step(self, data_orchestrator:BSDataOrchestrator, trading_account:PaperKiteAccount):
        """for given timestep execute the strategy based on the latest information the data_orchestrator gives
        - `trading_account` : zerodha like trading account
        """
        
        pass


class RsiMidTennisTradingStrategy(BaseTradingStrategy):

    def __init__(self) -> None:
        super().__init__()
    
    def execute_step(self, data_orchestrator: BSDataOrchestrator, trading_account: PaperKiteAccount):
        
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        datadf = data_orchestrator.get_ohlc_data_df(data_interval=DATA_INTERVAL_15MINUTE)
        rsi15 = INDICATORS.get_rsi_indicator(datadf['close'], length=15, mean_function="ema")

        # NOTE: never do try: catch: here, ideally we want to take care of all the possible scenarios preemptively
        if len(rsi15) <= 1:
            return
        
        last_trade_transaction_type = None
        recent_trades = trading_account.get_trades()
        if len(recent_trades) > 0:
            last_trade_transaction_type = recent_trades[-1]['transaction_type']

        if rsi15.iloc[-2] < 45.0 and rsi15.iloc[-1] >= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif rsi15.iloc[-2] > 45.0 and rsi15.iloc[-1] <= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_SELL:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)



class RsiDualThresholdTradingStrategy(BaseTradingStrategy):

    def __init__(self, data_interval_decision:DATA_INTERVAL_15MINUTE, lower_crossover_buy_threshold=40, upper_crossover_sell_threshold=60) -> None:
        super().__init__()
        self.lower_crossover_buy_threshold = lower_crossover_buy_threshold
        self.upper_crossover_sell_threshold = upper_crossover_sell_threshold
        self.data_interval_decision = data_interval_decision

    def __str__(self) -> str:
        return f"{super().__str__()}_{self.lower_crossover_buy_threshold}_{self.upper_crossover_sell_threshold}"

    def execute_step(self, data_orchestrator: BSDataOrchestrator, trading_account: PaperKiteAccount):
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        datadf = data_orchestrator.get_ohlc_data_df(data_interval=self.data_interval_decision)
        rsi14 = INDICATORS.get_rsi_indicator(datadf['close'], length=14, mean_function="sma")

        # NOTE: never do try: catch: here, ideally we want to take care of all the possible scenarios preemptively
        if len(rsi14) <= 1:
            return
        
        last_trade_transaction_type = self.get_most_recent_trade_trasaction_type(trading_account)
        pnl_points = self.get_pnl_points(data_orchestrator, trading_account)
        if rsi14.iloc[-2] <= self.lower_crossover_buy_threshold and rsi14.iloc[-1] > self.lower_crossover_buy_threshold and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif rsi14.iloc[-2] >= self.upper_crossover_sell_threshold and rsi14.iloc[-1] < self.upper_crossover_sell_threshold  and last_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)
        elif rsi14.iloc[-2] >= self.lower_crossover_buy_threshold and rsi14.iloc[-1] < self.lower_crossover_buy_threshold  and last_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)
        elif pnl_points and pnl_points < -30 and last_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)


class RsiDayEndNextOpenArbitrageTradingStrategy(BaseTradingStrategy):

    def __init__(self, data_orchestrator: BSDataOrchestrator, zeordha_like_trading_account: PaperKiteAccount) -> None:
        super().__init__(data_orchestrator, zeordha_like_trading_account)
        if self.data_orchestrator.minimum_granule_interval != DATA_INTERVAL_MINUTE:
            raise Exception("This trategy only works for Minute interval")
    
    def execute_step(self, data_orchestrator: BSDataOrchestrator, trading_account: PaperKiteAccount):
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        # this returns minimum granule
        
        datadf = self.data_orchestrator.get_ohlc_data_df()
        
        current_datetime = datadf['date'].iloc[-1]
        current_threshold = current_datetime.hour + current_datetime.minute/60
        upper_sell_threshold = 9 + 17/60
        lower_buy_threshold = 15 + 29/60
        # print(current_datetime)
        # print(current_threshold)
        # print(upper_sell_threshold)
        # print(lower_buy_threshold)
        last_trade_transaction_type = None
        recent_trades = self.trading_account.get_trades()
        if len(recent_trades) > 0:
            last_trade_transaction_type = recent_trades[-1]['transaction_type']

        elif current_threshold >= lower_buy_threshold  and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif len(recent_trades) > 0 and current_threshold <= upper_sell_threshold <= 45.0 and last_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_SELL:
            # NOTE: len(recent_trades) > 0: this condition makes sure you don't sell if you don't own
            self.place_order(data_orchestrator, trading_account, quantity=50, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)

# 88% winrate rynerteo
# ref: https://www.youtube.com/watch?v=W8ENIXvcGlQ
class RsiRynerTeo30BuyTradingStrategy(BaseTradingStrategy):

    def __init__(self, data_orchestrator: BSDataOrchestrator, zeordha_like_trading_account: PaperKiteAccount) -> None:
        super().__init__(data_orchestrator, zeordha_like_trading_account)
        if self.data_orchestrator.minimum_granule_interval != DATA_INTERVAL_DAY:
            raise Exception("This trategy only works for Minute interval")
    
    def execute_step(self, data_orchestrator: BSDataOrchestrator, trading_account: PaperKiteAccount):
        """the data orchestrator ensures that we never can cheat to look future data in backtest.
        And the super() place order ensures that we use proper instrument that dataorchestrator is based off of and 
        that we use perfect latest available price
        """
        # this returns minimum granule
        
        datadf = data_orchestrator.get_ohlc_data_df()
        rsi10 = INDICATORS.get_rsi_indicator(datadf['close'],length=10, mean_function='ema')
        ma200 = INDICATORS.get_moving_average_indicator(datadf['close'],length=200)
        exitdays = 10
        if len(rsi10) < 2:
            return
        most_recent_trade_transaction_type = self.get_most_recent_trade_trasaction_type(trading_account)
        if datadf['close'].iloc[-1] > ma200.iloc[-1] and rsi10.iloc[-1] <= 30 and most_recent_trade_transaction_type != KiteConnect.TRANSACTION_TYPE_BUY:
            print("Place BUY")
            self.place_order(data_orchestrator, trading_account, quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY)
        elif most_recent_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY and rsi10.iloc[-1] >= 40:
            print("EXIT RSI 40")
            self.place_order(data_orchestrator, trading_account, quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)
        elif len(rsi10)>=exitdays and datadf['close'].iloc[-exitdays] > ma200.iloc[-exitdays] and rsi10.iloc[-exitdays] <= 30 and most_recent_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY:
            print("EXIT 10 days")
            self.place_order(data_orchestrator, trading_account, quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)
        elif datadf['close'].iloc[-1] < ma200.iloc[-1] and most_recent_trade_transaction_type == KiteConnect.TRANSACTION_TYPE_BUY:
            print("EXIT market bearish")
            self.place_order(data_orchestrator, trading_account, quantity=1, transaction_type=KiteConnect.TRANSACTION_TYPE_SELL)


# new strat theory
# first 9:15 to 9:30 observe the first candle (ALL CANDLES ARE THAT OF UNDERLYING)
# 1a if open is high 
# 1b (nobody put buy price above open, that's why high is same as open) 
# 1c (proves trend is bearish)  
# 2a: buy put option as it's bearish (at the money; to leverage alpha, underlying will move for sure)
# 2b: stop loss is 1a open/high, 
# 2c: opportunity (past data figure out what was the next candles average moves after 1a candle)
# (if open and high both are equal; => it'll likely go to red as close will go down)


# Past data figure out average gains per day; plot gaussian/normal distribution; (to plot option spread)
        


def main_test():
    kite = make_kiteconnect_api()
    print(kite)
    print("---- positions ----")
    print(kite.positions())
    print("--- holdings ---")
    print(kite.holdings())

if __name__ == "__main__":
    main_test()
    