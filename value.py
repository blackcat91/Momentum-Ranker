import alpaca_trade_api as tradeapi
import math
import numpy as np
from numpy.core.numeric import NaN
import pandas as pd
import requests #The requests library for HTTP requests in Python
import xlsxwriter #The XlsxWriter libarary for 
from scipy import stats #The SciPy stats module
import pandas_ta as ta
from secrets import IEX_CLOUD_API_TOKEN

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]   
        



if __name__ == '__main__':
    
    tickers = pd.read_csv('sp_500_stocks.csv')
    listSym = tickers['Ticker'].tolist()
    ticker_groups = list(chunks(listSym, 100))
    
    symbol_strings = []
    for i in range(0, len(ticker_groups)):
        symbol_strings.append(','.join(ticker_groups[i]))
   

    value_columns = [
                    'Ticker', 
                    'Price', 
                    'P/EG Ratio',
                    'P/EG Percentile',
                    'P/S Ratio',
                    'P/S Percintile',
                    'P/B Ratio',
                    'P/B Percintile',
                    'EV/EBITDA Ratio', 
                    'EV/EBITDA Percintile',
                    'EV/Revenue Ratio', 
                    'EV/Revenue Percentile',
                    'Target Price',
                    'TP Ratio',
                    'TP Percentile',
                    'RV Score'
                    ]





    API_KEY = 'AK800MFRCDVNA1VJP3PJ'
    SECRET_KEY = 'VjkqQrbMqajdeONvxSMP2rGbRNKTb2QJELwM6fHz'
    BASE_URL = 'https://api.alpaca.markets'
    TICKER_TO_PREDICT = 'TSLA'
    TICKERS_TO_PREDICT = ['TSLA', 'AAL', 'AAPL', 'BLNK']
    api = tradeapi.REST(API_KEY, SECRET_KEY, base_url=BASE_URL)

    

    value_dataframe = pd.DataFrame(columns=value_columns)


    print('Preparing Data... \n')

    for ticker_string in ticker_groups:
        for ticker in ticker_string:
            try: 
                print(ticker)
                url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey=JKIK4GFW7ZXBV05Y'
                data = requests.get(url).json()
                
                
                value_data_map = dict()
                value_data_map['ticker'] = ticker
                value_data_map['price'] = float(six_month_barset[ticker][-1].c)
                try:
                    value_data_map['peg'] = data['PEGRatio']
                except KeyError:
                    print(data)
                    value_data_map['peg'] = np.NaN

                value_data_map['pb'] = data['PriceToBookRatio']
                value_data_map['ps'] = data['PriceToSalesRatioTTM']

                value_data_map['evEBITDA'] = data['EVToEBITDA']
                value_data_map['evRev'] = data['EVToRevenue']
                value_data_map['target'] = float(data['AnalystTargetPrice'])
                value_data_map['tpRatio'] = value_data_map['price'] / value_data_map['target']
                print(value_data_map)
                value_dataframe = value_dataframe.append(pd.Series([value_data_map['ticker'],value_data_map['price'], value_data_map['peg'], 'N/A', value_data_map['pb'],'N/A', value_data_map['ps'],'N/A', value_data_map['evEBITDA'], 'N/A', value_data_map['evRev'], 'N/A', value_data_map['target'],value_data_map['tpRatio'],'N/A', 'N/A' ], index=value_columns), ignore_index=True)
            except IndexError:
                pass
            
                
    for column in ['P/EG Ratio', 'P/B Ratio','P/S Ratio',  'EV/EBITDA Ratio','EV/Revenue Ratio', 'TP Ratio']:
        value_dataframe[column].fillna(value_dataframe[column].mean(), inplace = True)
    time_periods = [
                    'P/EG',   
                    'P/S',  
                    'P/B', 
                    'EV/EBITDA', 
                    'EV/Revenue', 
                    'TP',
                    ]

    for row in value_dataframe.index:
        for time_period in time_periods:
            value_dataframe.loc[row, f'{time_period} Percentile'] = stats.percentileofscore(value_dataframe[f'{time_period} Ratio'], value_dataframe.loc[row, f'{time_period} Ratio'])/100
        

    print('Lining up our ducks... \n')

    from statistics import mean

    for row in value_dataframe.index:
        momentum_percentiles = []
        for time_period in time_periods:
            momentum_percentiles.append(value_dataframe.loc[row, f'{time_period} Percentile'])
        
        value_dataframe.loc[row, 'RV Score'] = mean(momentum_percentiles)

    value_dataframe.to_csv('value_strategy.csv')

    print('Done')