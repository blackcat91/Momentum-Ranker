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
from json.decoder import JSONDecodeError


#Value Calculator
#Uses P/E, P/B and more to calculate a stocks intrinsic value
#API isn't completely free to requests stop sometimes
#



class ValueCalculator():
    def __init__(self, **args):
        self.IEX_SANDBOX = 'https://sandbox.iexapis.com'
        self.TEST_TOKEN = 'Tpk_0e3629e14ac24927b78125d85a218b4a'
        self.TEST_SECRET = 'Tsk_2f2335c5db654db7b8b0d21da312cbaf'
        self.tickers = pd.read_csv('sp_500_stocks.csv')
        self.listSym = self.tickers['Ticker'].tolist()
        self.ticker_groups = list(self.chunks(self.listSym, 100))
        self.symbol_strings = []
        for i in range(0, len(self.ticker_groups)):
            self.symbol_strings.append(','.join(self.ticker_groups[i]))
        self.value_columns = [
                    'Ticker', 
                    'Company',
                    'Price', 
                    'P/EG Ratio',
                    'P/EG Percentile',
                    'P/S Ratio',
                    'P/S Percentile',
                    'P/B Ratio',
                    'P/B Percentile',
                    'EV/EBITDA Ratio', 
                    'EV/EBITDA Percentile',
                    'EV/Revenue Ratio', 
                    'EV/Revenue Percentile',
                    'Target Price',
                    'TP Ratio',
                    'TP Percentile',
                    'RV Score'
                    ]
        self.API_KEY = 'AK800MFRCDVNA1VJP3PJ'
        self.SECRET_KEY = 'VjkqQrbMqajdeONvxSMP2rGbRNKTb2QJELwM6fHz'
        self.BASE_URL = 'https://api.alpaca.markets'
        self.api = tradeapi.REST(self.API_KEY, self.SECRET_KEY, base_url=self.BASE_URL)
        self.value_dataframe = pd.DataFrame(columns=self.value_columns)


    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]   
    def get_ticker_data(self, ticker_string, six_month_barset):
        
        for ticker in ticker_string:
            try: 
                print(ticker)
                financials = f'{self.IEX_SANDBOX}/stable/stock/{ticker}/advanced-stats/?token={self.TEST_TOKEN}'
                peUrl = f'{self.IEX_SANDBOX}/stable/stock/{ticker}/quote/peRatio?token={self.TEST_TOKEN}'
                estimates = f'{self.IEX_SANDBOX}/stable/time-series/CORE_ESTIMATES/{ticker}?token={self.TEST_TOKEN}'
                fData = requests.get(financials).json()
                peRatio = requests.get(peUrl).json()
                eData = requests.get(estimates).json()[0]
                fData['peRatio'] = peRatio
                fData['AnalystTargetPrice'] = eData['marketConsensusTargetPrice']
                
            except JSONDecodeError:
                continue
            except IndexError:
                continue
                    
            try:
                value_data_map = dict()
                value_data_map['ticker'] = ticker
                value_data_map['company'] = fData['companyName']
                try:
                    value_data_map['price'] = float(six_month_barset[ticker][len(six_month_barset[ticker]) - 1].c)
                except IndexError:
                    continue
                try:
                    value_data_map['peg'] = -(fData['pegRatio'])
                    
                except TypeError:
                    print(fData)
                    value_data_map['peg'] = np.NaN
                try:
                    value_data_map['pb'] = -np.abs(fData['priceToBook'])
                except TypeError:
                    value_data_map['pb'] = np.NaN
                try:
                    value_data_map['ps'] = -np.abs(fData['priceToSales'])
                except TypeError:
                    value_data_map['ps'] = np.NaN

                value_data_map['evEBITDA'] = -np.abs(np.divide(fData['enterpriseValue'],fData['EBITDA']))
                value_data_map['evRev'] = -np.abs(fData['enterpriseValueToRevenue'])
                try:
                    value_data_map['target'] = float(fData['AnalystTargetPrice'])
                except KeyError:
                    value_data_map['target'] = np.NaN

                value_data_map['tpRatio'] = -np.abs(np.divide(value_data_map['price'] , value_data_map['target']))
                print(value_data_map)
                self.value_dataframe = self.value_dataframe.append(pd.Series([value_data_map['ticker'],value_data_map['company'],value_data_map['price'], value_data_map['peg'], 'N/A', value_data_map['pb'],'N/A', value_data_map['ps'],'N/A', value_data_map['evEBITDA'], 'N/A', value_data_map['evRev'], 'N/A', value_data_map['target'],value_data_map['tpRatio'],'N/A', 'N/A' ], index=self.value_columns), ignore_index=True)
        
            except TypeError:
                print('Index Error')
        return self.value_dataframe    

    def main(self):
        print('Preparing Data... \n')

        for ticker_string in self.ticker_groups:
            six_month_barset = self.api.get_barset(ticker_string, 'day', limit=10)
            self.get_ticker_data(ticker_string, six_month_barset)
                
                    
        for column in ['P/EG Ratio', 'P/B Ratio','P/S Ratio',  'EV/EBITDA Ratio','EV/Revenue Ratio', 'TP Ratio']:
            self.value_dataframe[column].fillna(self.value_dataframe[column].mean(), inplace = True)
        time_periods = [
                        'P/EG',   
                        'P/S',  
                        'P/B', 
                        'EV/EBITDA', 
                        'EV/Revenue', 
                        'TP',
                        ]

        for row in self.value_dataframe.index:
            for time_period in time_periods:
                self.value_dataframe.loc[row, f'{time_period} Percentile'] = stats.percentileofscore(self.value_dataframe[f'{time_period} Ratio'], np.divide(self.value_dataframe.loc[row, f'{time_period} Ratio']),100)
            

        print('Lining up our ducks... \n')

        from statistics import mean

        for row in self.value_dataframe.index:
            momentum_percentiles = []
            for time_period in time_periods:
                momentum_percentiles.append(self.value_dataframe.loc[row, f'{time_period} Percentile'])
            
            self.value_dataframe.loc[row, 'RV Score'] = mean(momentum_percentiles)

        self.value_dataframe.to_csv('value_strategy.csv')
        print('Done')
        return self.value_dataframe
        

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]   
        

IEX_SANDBOX = 'https://sandbox.iexapis.com'
TEST_TOKEN = 'Tpk_0e3629e14ac24927b78125d85a218b4a'
TEST_SECRET = 'Tsk_2f2335c5db654db7b8b0d21da312cbaf'

if __name__ == '__main__':
    
    val = ValueCalculator()
    val.main()
    

    
    
   

    





    


