import re
from datetime import datetime

import pandas as pd
import numpy as np

class INDICATORS:
    MOVING_AVERAGE = "ma"
    RELATIVE_STRENGTH_INDEX = "rsi"
    EXPONENTIAL_MOVING_AVERAGE = "ema"
    HISTORICAL_VOLATALITY = "hv"
    
    @staticmethod
    def resolve_indicator_name(indicator_name):
        # valid: moving_average, moving average, ma, MoVingAveRage
        indicator_name_fixed = re.sub("[^a-zA-Z]+", "", indicator_name)
        if indicator_name_fixed.lower() in ['movingaverage', 'ma']:
            return INDICATORS.MOVING_AVERAGE
        if indicator_name_fixed.lower() in ['exponentialmovingaverage', 'ema']:
            return INDICATORS.EXPONENTIAL_MOVING_AVERAGE
        if indicator_name_fixed.lower() in ['relativestrengthindex', 'rsi']:
            return INDICATORS.RELATIVE_STRENGTH_INDEX
        if indicator_name_fixed.lower() in ['historicalvolatality', 'hv']:
            return INDICATORS.HISTORICAL_VOLATALITY
        return indicator_name
    
    @staticmethod
    def get_moving_average_indicator(df, length=5, source='close'):
        return df[source].rolling(length).mean()
    
    @staticmethod
    def get_moving_average_params(indicator_params_dict):
        if indicator_params_dict == None:
            indicator_params_dict = {}
        if 'length' not in indicator_params_dict:
            indicator_params_dict['length'] = 5
        if 'source' not in indicator_params_dict:
            indicator_params_dict['source'] = 'close'
        indicator_params_dict['unique_selector'] = f"{INDICATORS.MOVING_AVERAGE}_{indicator_params_dict['length']}_{indicator_params_dict['source']}"
        return indicator_params_dict
    
    @staticmethod
    def get_exponential_moving_average_indicator(df, length=5, source='close'):
        return df[source].ewm(span=length, adjust=False).mean()
    
    @staticmethod
    def get_exponential_moving_average_params(indicator_params_dict):
        if indicator_params_dict == None:
            indicator_params_dict = {}
        if 'length' not in indicator_params_dict:
            indicator_params_dict['length'] = 5
        if 'source' not in indicator_params_dict:
            indicator_params_dict['source'] = 'close'
        indicator_params_dict['unique_selector'] = f"{INDICATORS.MOVING_AVERAGE}_{indicator_params_dict['length']}_{indicator_params_dict['source']}"
        return indicator_params_dict
    
        
    @staticmethod
    def get_rsi_indicator(df, length=5, source='close', mean_function='ema'):
        delta = df[source].diff()
#         df['diff'] = df[source].diff()
        upm, downm = delta.copy(), delta.copy()
        upm[upm<0] = 0
        downm[downm>0] = 0
        downm = downm.abs()
        if mean_function == 'ma':
            rs = upm.rolling(length).mean()/downm.rolling(length).mean()
        if mean_function == 'ema':
            rs = upm.ewm(span=length, adjust=False).mean()/downm.ewm(span=length, adjust=False).mean()
        if mean_function == 'sma':
            rs = upm.ewm(alpha=1/length, adjust=False).mean()/downm.ewm(alpha=1/length, adjust=False).mean()
        rsi = 100 - 100/(1 + rs)
        return rsi


    @staticmethod
    def get_rsi_params(indicator_params_dict):
        if indicator_params_dict == None:
            indicator_params_dict = {}
        if 'length' not in indicator_params_dict:
            indicator_params_dict['length'] = 5
        if 'mean_function' not in indicator_params_dict:
            indicator_params_dict['mean_function'] = 'sma'
            
        indicator_params_dict['unique_selector'] = f"{INDICATORS.RELATIVE_STRENGTH_INDEX}_{indicator_params_dict['length']}_{indicator_params_dict['mean_function']}"
        return indicator_params_dict

    @staticmethod
    def get_historical_volatality_indicator(df, length=5, source='close'):
        daily_return = df[source].rolling(2).apply(lambda x: x.iloc[1]/x.iloc[0])
        daily_return = np.log(daily_return)
        stddev = daily_return.rolling(length).apply(lambda x: np.std(x))
        dv = stddev * 100
        hv = dv * np.sqrt(365)
        return hv

    @staticmethod
    def get_historical_volatality_params(indicator_params_dict):
        if indicator_params_dict == None:
            indicator_params_dict = {}
        if 'length' not in indicator_params_dict:
            indicator_params_dict['length'] = 5
        if 'source' not in indicator_params_dict:
            indicator_params_dict['source'] = 'close'
        indicator_params_dict['unique_selector'] = f"{INDICATORS.MOVING_AVERAGE}_{indicator_params_dict['length']}_{indicator_params_dict['source']}"
        return indicator_params_dict
    
    @staticmethod
    def create_get_indicator_func(df):
        def get_indicator_func(indicator_name, indicator_params_dict=None):
            indicator_name = INDICATORS.resolve_indicator_name(indicator_name)
            if indicator_name in df.columns:
                return df[indicator_name]
            
            # MA/SMA
            if indicator_name == INDICATORS.MOVING_AVERAGE:    
                the_params_dict = INDICATORS.get_moving_average_params(indicator_params_dict)
                
                column_selector = the_params_dict['unique_selector']
                if column_selector not in df.columns:
                    df[column_selector] = INDICATORS.get_moving_average_indicator(df, length=the_params_dict['length'], source=the_params_dict['source'])
                                
                return df[column_selector]
            
            # EMA
            if indicator_name == INDICATORS.EXPONENTIAL_MOVING_AVERAGE:    
                the_params_dict = INDICATORS.get_exponential_moving_average_params(indicator_params_dict)
                
                column_selector = the_params_dict['unique_selector']
                if column_selector not in df.columns:
                    df[column_selector] = INDICATORS.get_exponential_moving_average_indicator(df, length=the_params_dict['length'], source=the_params_dict['source'])
                                
                return df[column_selector]
            
            # RSI            
            if indicator_name == INDICATORS.RELATIVE_STRENGTH_INDEX:
                the_params_dict = INDICATORS.get_rsi_params(indicator_params_dict)
                
                column_selector = the_params_dict['unique_selector']
                if column_selector not in df.columns:
                    df[column_selector] = INDICATORS.get_rsi_indicator(df, length=the_params_dict['length'], source=the_params_dict['source'], mean_function=indicator_params_dict['mean_function'])
                                
                return df[column_selector]
            
            # RSI            
            if indicator_name == INDICATORS.HISTORICAL_VOLATALITY:
                the_params_dict = INDICATORS.get_historical_volatality_params(indicator_params_dict)
                
                column_selector = the_params_dict['unique_selector']
                if column_selector not in df.columns:
                    df[column_selector] = INDICATORS.get_historical_volatality_indicator(df, length=the_params_dict['length'], source=the_params_dict['source'])
                                
                return df[column_selector]
                
        return get_indicator_func

def test():
    csv_location = "input/kite_historical/895745_TATASTEEL_EQ_NSE_NSE_day.csv"
    testdf = pd.read_csv(csv_location)
    get_indicator = INDICATORS.create_get_indicator_func(testdf)
    hv10 = get_indicator("hv", {'length':15})
    print("----------")
    print("-- HV length:10 indicator")
    print(testdf[["date","close",hv10.name]].tail(21))

if __name__ == "__main__":
    test()
