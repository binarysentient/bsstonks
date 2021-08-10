# Goal is to create zerodha like account; so that we can backtest things and make them live seamlessly

class PaperKiteAccount():
    balance = 0
    trades = []
    holdings = []
    def __init__(self, initial_balance = 0) -> None:
        self.balance = 0
        # initialize with balance
        pass
    
    def get_balance(self) -> float:
        return self.balance

    def withdraw_funds(self, amount:float) -> float:
        if self.balance >= amount and amount >= 0:
            self.balance -= amount
            return amount
        else:
            return 0
    
    def add_funds(self, amount:float) -> float:
        if amount >= 0:
            self.balance += amount
            return self.balance
    
    def holdings(self):
        """Retrieve the list of equity holdings."""
        # return self._get("portfolio.holdings")
        pass

    def margins(self, segment=None):
        """Get account balance and cash margin details for a particular segment.
        - `segment` is the trading segment (eg: equity or commodity)
        """
        # if segment:
        #     return self._get("user.margins.segment", {"segment": segment})
        # else:
        #     return self._get("user.margins")
        pass

    def orders(self):
        """Get list of orders."""
        # return self._format_response(self._get("orders"))
        pass

    def positions(self):
        """Retrieve the list of positions."""
        # return self._get("portfolio.positions")
        pass
    
    def place_order(self,
                variety,
                exchange,
                tradingsymbol,
                transaction_type,
                quantity,
                product,
                order_type,
                price=None,
                validity=None,
                disclosed_quantity=None,
                trigger_price=None,
                squareoff=None,
                stoploss=None,
                trailing_stoploss=None,
                tag=None):
        """Place an order."""
        # params = locals()
        # del(params["self"])
        # for k in list(params.keys()):
        #     if params[k] is None:
        #         del(params[k])
        # return self._post("order.place", params)["order_id"]
        pass
        
    def save(file):
        # save the account somewhere
        pass

    def load(file):
        # load the account data from somewhere
        pass
