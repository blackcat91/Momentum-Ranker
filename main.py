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
import csv
from secrets import IEX_CLOUD_API_TOKEN
from threading import Thread
import time



momentumData = pd.read_csv('momentum_strategy.csv')

momentumData.set_index('Ticker')
valueData = pd.read_csv('value_strategy.csv')
valueData.set_index('Ticker')


col = ['ticker', 'momentum', 'value', 'overall']

data = pd.DataFrame(columns=col)


from statistics import mean

#outer merge the two DataFrames, adding an indicator column called 'Exist'
diff_df = pd.merge(valueData['Ticker'], momentumData['Ticker'], how='outer', indicator='Exist')

#find which rows don't exist in both DataFrames
diff_df = diff_df.loc[diff_df['Exist'] != 'both']


for value in diff_df['Ticker']:
    try:
        
        momentumData.drop(momentumData[momentumData['Ticker'] == value].index, inplace=True)
        momentumData.dropna(axis = 0, inplace=True)
    except KeyError:
        print('Failed')
        pass        

same_df = pd.merge(valueData['Ticker'], momentumData['Ticker'], how='outer', indicator='Exist')

#find which rows don't exist in both DataFrames
same_df = same_df.loc[same_df['Exist'] != 'both']
if(same_df.empty):
    
    data['ticker'] = valueData['Ticker']
    data['value'] = valueData['RV Score']
    data['momentum'] = momentumData['HQM Score']
    data['momentum'].fillna(data['momentum'].mean(), inplace=True)

    for row in data.index:
        m = []
        m.append(data.loc[row, 'momentum'])
        m.append(data.loc[row, 'value'])
        data.loc[row, 'overall'] = mean(m)
    
    data.to_csv('overall.csv')


# data = data.loc[data['Ticker'] != diff_df['Ticker']]
# diff_df.to_csv('diff.csv')