# imports

import alpaca_trade_api as alpaca
import yfinance as yahoo

# defaults / stats

import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime
import math
import random
import json
import requests

# api keys

finn_api_key = 'cjhpdc9r01qonds7da50cjhpdc9r01qonds7da5g'
finn_base_url = 'https://finnhub.io/api/v1/'

# functions

def getQuarterlyFundamentalDataFinn(ticker):
  ret = getFundamentalDataFinnHub(ticker)
  columns = list(ret["series"]["quarterly"].keys())
  data = {}
  for column in columns:
    data[column] = [datum["v"] for datum in ret["series"]["quarterly"][column]]
  maxlist = [len(datum) for datum in data.values()]
  maximum = np.max(maxlist)
  maxcolumn = columns[np.argmax(maxlist)]
  for datum in data.values(): 
    if len(datum)<maximum:
      datum.extend([None]*(maximum-len(datum)))
  df = pd.DataFrame(data,columns=["Date"]+columns,index=[datetime.datetime.strptime(datum["period"], "%Y-%m-%d").date() for datum in ret["series"]["quarterly"][maxcolumn]])
  df.sort_values(by='Date',inplace=True)
  df=df.iloc[::-1]
  return df

def getDailyDataYahoo(ticker):
  # call stock from yahoo ticker
  stock = yahoo.Ticker(ticker)
  data = stock.history(period="max",interval="1d")
  indices = [datum.date() for datum in list(data.index)]
  data["Date"] = indices
  data.set_index("Date",inplace=True)
  return data

def getFundamentalDataFinnHub(ticker):
  endpoint = 'stock/metric'
  params = {'symbol': ticker,'metric': 'all','token': finn_api_key}
  response = requests.get(finn_base_url + endpoint, params=params)
  try: 
      response.status_code == 200
      data = response.json()
      return data
  except:
      return f"Error: {response.status_code}"

def getEarningsData(ticker,days_forward,write=False,filename="",returns=True):
  df=getQuarterlyFundamentalDataFinn(ticker)
  dates = df.index.to_list()
  data = getDailyDataYahoo(ticker)
  range = [1,3,5,10,30,90]
  for r in range: 
    data[str(r)+" day forward open %change"] = (-1+data["Open"].shift(-r)/data["Open"])*100
    data[str(r)+" day forward close %change"] = (-1+data["Close"].shift(-r)/data["Close"])*100
  data = data[data.index.isin(dates)]
  merged_df = pd.merge(df, data, left_index=True, right_index=True, how='inner')
  del df["Date"]
  df.to_csv("data_import/data/"+filename,index=True)
  print(df)
  if returns:
    return merged_df

range = [1,3,5,10]
for ticker in ["AAPL"]:
  getEarningsData(ticker,range,True,ticker+"earningslearningdata.csv",False)