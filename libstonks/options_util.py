import math
import numpy as np
import pandas as pd
import scipy.stats as si
from datetime import datetime
import matplotlib.pyplot as plt


# reference : https://www1.nseindia.com/content/circulars/faop3716.htm
# reference : https://zerodha.com/varsity/chapter/greek-calculator/ (put call parity)
# reference : https://www.linkedin.com/pulse/pricing-options-black-scholes-python-daniel-morales-ch%C3%A1vez/ ( main code from here)
# reference : https://www.khanacademy.org/economics-finance-domain/core-finance/derivative-securities/black-scholes/v/introduction-to-the-black-scholes-formula
# reference : bunch of other links
def black_scholes_put(spotprice, strikeprice, transaction_date, expiration_date, implied_volatility, risk_free_rate_of_interest, dividend=0):
    """
        This function determines the price for a Plain vanilla Call given the     
        following arguments. 
        The function uses the Black & Scholes - Merton model for Option Pricing.
        S = The Stock price at time t
        K = The Strike price
        T = Time to maturity in Years
        rf = Risk-free rate
        q = dividend rate
        vol = volatility (standard deviation of the financial asset)
    """
    
    rf=risk_free_rate_of_interest/100
    q=dividend/100
    vol=implied_volatility/100
    S = spotprice
    K = strikeprice
    T = (expiration_date - transaction_date).total_seconds()/60/60/24/365
    d1= ((np.log(S/K)) + ((rf+0.5*vol**2)*T))/(vol*(T**0.5))
    Nd1= si.norm.cdf(-d1,0.0,1.0)
    d2= d1-(vol*(T**0.5))
    Nd2= si.norm.cdf(-d2,0.0,1.0)
   
    Put= (K*(math.exp(-rf*T))*Nd2) - (S*Nd1) 
    Call = Put - K*math.exp(-rf*T) + S 
    return Call, Put

def black_scholes_call(spotprice, strikeprice, transaction_date, expiration_date, implied_volatility, risk_free_rate_of_interest, dividend=0):
    """
        This function determines the price for a Plain vanilla Call given the     
        following arguments. 
        The function uses the Black & Scholes - Merton model for Option Pricing.
        S = The Stock price at time t
        K = The Strike price
        T = Time to maturity in Years
        rf = Risk-free rate
        q = dividend rate
        vol = volatility (standard deviation of the financial asset)
    """
    
    rf=risk_free_rate_of_interest/100
    q=dividend/100
    vol=implied_volatility/100
    S = spotprice
    K = strikeprice
    T = (expiration_date - transaction_date).total_seconds()/60/60/24/365
    d1= ((np.log(S/K)) + ((rf+0.5*vol**2)*T))/(vol*(T**0.5))
    
    # ND1: what's the probability that d1 will be achieved
    Nd1= si.norm.cdf(d1,0.0,1.0)
    # print("D1,ND1",d1, Nd1)
    d2= d1-(vol*(T**0.5))
    # ND2: what's the probability that d2 will be achieved
    Nd2= si.norm.cdf(d2,0.0,1.0)
    # print("D1,ND1",d2, Nd2)
    Call= (S*Nd1) - (K*(math.exp(-rf*T))*Nd2)
    Put = Call + K*math.exp(-rf*T) - S 
    return Call, Put


# reference: https://www.investopedia.com/ask/answers/032515/what-options-implied-volatility-and-how-it-calculated.asp (iterative search)
# reference: https://en.wikipedia.org/wiki/Implied_volatility (root finding methods)
def black_scholes_volatility(spotprice, strikeprice, transaction_date, expiration_date, premium, risk_free_rate_of_interest, dividend=0):
    # NOTE: interpolation won't work, scrap it
    guesspremium1 = 1
    guesspremium2 = 100
    callprice1, putprice1 = black_scholes_call(spotprice, strikeprice, datetime.now(), datetime(2021,7,29, 15, 00, 00), guesspremium1, 3.44)   
    callprice2, putprice2 = black_scholes_call(spotprice, strikeprice, datetime.now(), datetime(2021,7,29, 15, 00, 00), guesspremium2, 3.44)   

    return guesspremium1 + (premium - callprice1) / (callprice2 - callprice1) * (guesspremium2 - guesspremium1)

# NOTE: we're also building out intuition here by understanding the effects of each parameter
#       we're also understanding how the formula's internal variable changes and what each means
def black_scholes_insight_graphs():
    # Let's figure out how does call price changes at different strikes when volatality changes
    spotprice = 2086
    strikestep = 30
    numberofstrikes = 5
    volatility_range = [idx/2 for idx in range(1,200)]
    strike_prices = [idx for idx in range(spotprice - strikestep*numberofstrikes, spotprice + strikestep*(numberofstrikes+1), strikestep)]
    fig = plt.figure()
    plt.title("Volatility vs Call Price")
    for strikeprice in strike_prices:
        print("Strike:", strikeprice)
        call_prices = []
        for ivolatility in volatility_range:    
            callprice, putprice = black_scholes_call(spotprice, strikeprice, datetime.now(), datetime(2021,7,29, 15, 00, 00), ivolatility, 3.44)
            call_prices.append(putprice)
        plt.plot(volatility_range, call_prices,label=f"{strikeprice}")
    plt.xlabel("Volatility")
    plt.ylabel("Call Price")
    plt.legend()
    plt.show()

    # TODO: we want to figure out rate of change in call price respect to volatility
    #       we've already plotted volatility vs callprice
    #       now let's print the slopes at each volatality and find out 
    #       for which strike price which volatility gives maximum premium change rate
    #       



def black_scholes_formula_test():
    callprice, putprice = black_scholes_call(2099, 2200, datetime.now(), datetime(2021,7,29, 15, 00, 00), 25.84, 3.44)
    print("Call ", callprice, "Put", putprice)
    return
    print("Using Put")
    callprice, putprice = black_scholes_put(2095.20, 2100, datetime.now(), datetime(2021,7,29, 15, 00, 00), 24.75, 3.44)
    print("Call ", callprice, "Put", putprice)
    print("Mid volatility")
    callprice, putprice = black_scholes_call(2095.20, 2100, datetime.now(), datetime(2021,7,29, 15, 00, 00), (24.75 + 22.24)/2.0, 3.44)
    print("Call ", callprice, "Put", putprice)

if __name__ == "__main__":
    # black_scholes_formula_test()
    black_scholes_insight_graphs()