from multiprocessing.spawn import freeze_support
import alpaca_trade_api as tradeapi
import math
import numpy as np
from numpy.core.numeric import NaN
import pandas as pd
import requests #The requests library for HTTP requests in Python
import xlsxwriter #The XlsxWriter libarary for 
from scipy import stats #The SciPy stats module
import pandas_ta as ta
import asyncio
from secrets import IEX_CLOUD_API_TOKEN
from threading import Thread
import time

API_KEY = 'AK800MFRCDVNA1VJP3PJ'
SECRET_KEY = 'VjkqQrbMqajdeONvxSMP2rGbRNKTb2QJELwM6fHz'
BASE_URL = 'https://api.alpaca.markets'
    

class MomentumCalculator():
    def __init__(self, *args, **kwargs):
        self.tickers = pd.read_csv('sp_500_stocks.csv')
        self.tickers = pd.read_csv('sp_500_stocks.csv')
        self.listSym = self.tickers['Ticker'].tolist()
        self.ticker_groups = list(self.chunks(self.listSym, 100))
        self.symbol_strings = []
        for i in range(0, len(self.ticker_groups)):
            self.symbol_strings.append(','.join(self.ticker_groups[i]))
        self.hqm_columns = [
                    'Ticker', 
                    'Price', 
                    'Six-Month Price Return',
                    'Six-Month Return Percentile',
                    'Three-Month Price Return',
                    'Three-Month Return Percentile',
                    'One-Month Price Return',
                    'One-Month Return Percentile',
                    'Two-Week Price Return', 
                    'Two-Week Return Percentile',
                    'Inverse RSI', 
                    'RSI Percentile',
                    'HQM Score'
                    ]

        self.api = tradeapi.REST(API_KEY, SECRET_KEY, base_url=BASE_URL)

        self.hqm_dataframe = pd.DataFrame(columns=self.hqm_columns)


    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]   
    def prep_data(self, ticker_string: list):
        
        six_month_barset = self.api.get_barset(ticker_string, 'day', limit=106)
        for ticker in ticker_string:
            try: 
                
                hqm_data_map = dict()
                
                hqm_data_map['ticker'] = ticker
                hqm_data_map['price'] = float(six_month_barset[ticker][-1].c)
                hqm_data_map['sixMonth'] = np.divide((np.subtract(six_month_barset[ticker][-1].c, six_month_barset[ticker][0].c)),six_month_barset[ticker][0].c)

                hqm_data_map['threeMonth'] = np.divide((np.subtract(six_month_barset[ticker][-1].c, six_month_barset[ticker][59].c)),six_month_barset[ticker][59].c)

                hqm_data_map['oneMonth'] = np.divide((np.subtract(six_month_barset[ticker][-1].c , six_month_barset[ticker][85].c)),six_month_barset[ticker][85].c)

                hqm_data_map['twoWeek'] = np.divide((np.subtract(six_month_barset[ticker][-1].c , six_month_barset[ticker][95].c)),six_month_barset[ticker][95].c)

                columns = ['open', 'close' ]

                dataframe = pd.DataFrame(columns=columns)

                for barset in six_month_barset[ticker]:
                    dataframe = dataframe.append(pd.Series([barset.o, barset.c], index=columns), ignore_index=True)

                hqm_data_map['inverse_rsi'] = -(ta.momentum.rsi(dataframe['close'], window=14)[len(six_month_barset[ticker]) -1]) 

                self.hqm_dataframe = self.hqm_dataframe.append(pd.Series([hqm_data_map['ticker'],hqm_data_map['price'], hqm_data_map['sixMonth'], 'N/A', hqm_data_map['threeMonth'],'N/A', hqm_data_map['oneMonth'],'N/A', hqm_data_map['twoWeek'], 'N/A', hqm_data_map['inverse_rsi'], 'N/A', 'N/A' ], index=self.hqm_columns), ignore_index=True)
            except IndexError:
                pass


    def main(self):
        
        
        for ticker_string in self.ticker_groups:
            self.prep_data(ticker_string)

     
            

        time_periods = [
                    'Six-Month',
                    'Three-Month',
                    'One-Month',
                    'Two-Week'
                    ]

        for row in self.hqm_dataframe.index:
            for time_period in time_periods:
                self.hqm_dataframe.loc[row, f'{time_period} Return Percentile'] = np.divide(stats.percentileofscore(self.hqm_dataframe[f'{time_period} Price Return'], self.hqm_dataframe.loc[row, f'{time_period} Price Return']),100)
            self.hqm_dataframe.loc[row, f'RSI Percentile'] = np.divide(stats.percentileofscore(self.hqm_dataframe[f'Inverse RSI'], self.hqm_dataframe.loc[row, f'Inverse RSI']),100)

        print('Lining up our ducks... \n')

        from statistics import mean

        for row in self.hqm_dataframe.index:
            momentum_percentiles = []
            for time_period in time_periods:
                momentum_percentiles.append(self.hqm_dataframe.loc[row, f'{time_period} Return Percentile'])
            momentum_percentiles.append(self.hqm_dataframe.loc[row, f'RSI Percentile'])
            self.hqm_dataframe.loc[row, 'HQM Score'] = mean(momentum_percentiles)

        self.hqm_dataframe.to_csv('momentum_startegy.csv')

        print('Done')
        return self.hqm_dataframe

   






if __name__ == '__main__':  

    print('Preparing Data... \n')
    try:
        momCalc = MomentumCalculator()
        momCalc.main()
    except Exception:
        print(Exception.message)





    



    

    