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

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]   

tickers = pd.read_csv('sp_500_stocks.csv')
listSym = tickers['Ticker'].tolist()
ticker_groups = list(chunks(listSym, 100))
    
symbol_strings = []
for i in range(0, len(ticker_groups)):
    symbol_strings.append(','.join(ticker_groups[i]))
   

hqm_columns = [
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





API_KEY = 'AK800MFRCDVNA1VJP3PJ'
SECRET_KEY = 'VjkqQrbMqajdeONvxSMP2rGbRNKTb2QJELwM6fHz'
BASE_URL = 'https://api.alpaca.markets'
    
api = tradeapi.REST(API_KEY, SECRET_KEY, base_url=BASE_URL)

    

hqm_dataframe = pd.DataFrame(columns=hqm_columns)


    
def prep_data(ticker_string: list):
        global hqm_dataframe
        global api
        six_month_barset = api.get_barset(ticker_string, 'day', limit=106)
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

                hqm_dataframe = hqm_dataframe.append(pd.Series([hqm_data_map['ticker'],hqm_data_map['price'], hqm_data_map['sixMonth'], 'N/A', hqm_data_map['threeMonth'],'N/A', hqm_data_map['oneMonth'],'N/A', hqm_data_map['twoWeek'], 'N/A', hqm_data_map['inverse_rsi'], 'N/A', 'N/A' ], index=hqm_columns), ignore_index=True)
            except IndexError:
                pass

def main(ticker_groups):
        global hqm_dataframe
       
        for ticker_string in ticker_groups:
            prep_data(ticker_string)

     
            

        time_periods = [
                    'Six-Month',
                    'Three-Month',
                    'One-Month',
                    'Two-Week'
                    ]

        for row in hqm_dataframe.index:
            for time_period in time_periods:
                hqm_dataframe.loc[row, f'{time_period} Return Percentile'] = np.divide(stats.percentileofscore(hqm_dataframe[f'{time_period} Price Return'], hqm_dataframe.loc[row, f'{time_period} Price Return']),100)
            hqm_dataframe.loc[row, f'RSI Percentile'] = np.divide(stats.percentileofscore(hqm_dataframe[f'Inverse RSI'], hqm_dataframe.loc[row, f'Inverse RSI']),100)

        print('Lining up our ducks... \n')

        from statistics import mean

        for row in hqm_dataframe.index:
            momentum_percentiles = []
            for time_period in time_periods:
                momentum_percentiles.append(hqm_dataframe.loc[row, f'{time_period} Return Percentile'])
            momentum_percentiles.append(hqm_dataframe.loc[row, f'RSI Percentile'])
            hqm_dataframe.loc[row, 'HQM Score'] = mean(momentum_percentiles)

        hqm_dataframe.to_csv('momentum_startegy.csv')

        print('Done')



if __name__ == '__main__':  

    print('Preparing Data... \n')
    try:
    
        main(ticker_groups)
    except Exception:
        print(Exception.message)





    



    

    