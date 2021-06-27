from multiprocessing.spawn import freeze_support
import alpaca_trade_api as tradeapi
import numpy as np
from numpy.core.numeric import NaN
import pandas as pd
import requests #The requests library for HTTP requests in Python
import xlsxwriter #The XlsxWriter libarary for 
from scipy import stats #The SciPy stats module
import pandas_ta as ta
import csv
from secrets import MONGO_STRING
from threading import Thread
import time
import pymongo
import json
from momentum import MomentumCalculator
from value import ValueCalculator
import argparse



class StockRanker():
    def __init__(self):
        self.valueCalc = ValueCalculator()
        self.momentumCalc = MomentumCalculator()
    
    def get_value(self):
        return self.valueCalc.main()
    def get_momentum(self):
        return self.momentumCalc.main()

    def create_overall(self):
        try:
            momentumData = pd.read_csv('momentum_strategy.csv')
            momentumData.set_index('Ticker')
        except Exception:
            
            print('Please Run the momentum calulator first.')
            exit(1)

        
        try:
            valueData = pd.read_csv('value_strategy.csv')
            valueData.set_index('Ticker')
        except Exception:
            
            print('Please Run the value calulator first.')
            exit(1)


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
                print('Failed: '+ KeyError.message)
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
            data.to_json('overall.json')
            connect = pymongo.MongoClient(MONGO_STRING)
            mydb = connect['hotpick']
            collection = mydb['stocks']
            collection.drop()
            mydb.create_collection('stocks')

            dictionary = csv.DictReader(open('overall.csv'))

            for row in dictionary:
                stockData = dict()
                stockData['ticker'] = row['ticker']
                stockData['momentum'] = row['momentum']
                stockData['value'] = row['value']
                stockData['overall'] = row['overall']
                collection.insert_one(stockData)
                print('Inserted')

            print('Inserts Complete')
           

    def main(self):
        print('Calculating... \n Go for a walk. This can take a while!') 
        mThread = Thread(target=self.momentumCalc.main, args=())
        vThread = Thread(target=self.valueCalc.main, args=())
        mThread.start()
        vThread.start()
        mThread.join()
        vThread.join()
        self.create_overall()

if __name__ == '__main__':
    parser = argparse.ArgumentParser('StockRanker', usage='Stock Ranker: Grab the stocks from the S&P 500 and give them a score based on value and momentum. run with no options to run the whole thing otherwise use options [-m, -v, -o]')
    parser.add_argument("-m", help="Run Momentum Ranker", action="store_true")
    parser.add_argument("-v", help="Run Value Ranker", action="store_true")
    parser.add_argument("-o", help="Run Overall Comparison (Only run if momentum and value have already been calulated)", action="store_true")
    args = parser.parse_args()
    ranker = StockRanker()
    if args.m == True and args.v == False and args.o == False:   
        ranker.get_momentum() 
    elif args.v == True and args.m == False and args.o == False:
        ranker.get_value() 
    elif args.o == True and args.m == False and args.v == False:
        ranker.create_overall()
    elif args.v == True and args.m == True and args.o == False:
        print('Calculating... \n Go for a walk. This can take a while!') 
        mThread = Thread(target=ranker.momentumCalc.main, args=())
        vThread = Thread(target=ranker.valueCalc.main, args=())
        mThread.start()
        vThread.start()
        mThread.join()
        vThread.join()
        print('Done')
    elif args.o == True and args.m == True and args.v == False:
        print('Calculating Momentum \n This can take a while... \n\n')
        ranker.momentumCalc.main()
        print('Creating Overall \n This can take a while... \n\n')
        ranker.create_overall()
        print('Done')
    elif args.o == True and args.v == True and args.m == False:
        print('Calculating Value \n\n')
        ranker.valueCalc.main()
        print('Creating Overall \n\n')
        ranker.create_overall()
        print('Done')
    else:
        ranker.main()
              