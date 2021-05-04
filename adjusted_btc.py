#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#####  loading required packages ####
import pandas
import datetime
import mysql.connector
from sqlalchemy import create_engine
import time

#####################
## main table load ##
#####################

engine = create_engine('mysql+mysqlconnector://', echo=False)

df = pandas.read_sql( '''select distinct c.Rank
                              , c.Coin_Id
                              , mc.Icon
                              , c.Symbol
                              , mc.url_Adjusted_Price as 'Name'
                              , c.Price_USD_Noise
                              , c.Market_Cap_USD_Noise   
                              , c.Available_Supply                                                  
                        from c c
                        inner join mc mc on c.Symbol = mc.Symbol
                        order by c.Rank asc''',engine)
print (df.head())
print (df.tail())

#calculating circulating supply
df['Circulating_Supply'] = df['Available_Supply']

#calculating btc adjusted price (normalized price)
df['Btc_Adjusted_Price'] = round(df['Market_Cap_USD_Noise']/df['Circulating_Supply'][0],2)

#calculating maximum increase (supposing that btc market cap cannot be exceded)
df['Max_Btc_Multiplier'] = round(df['Btc_Adjusted_Price'][0]/df['Btc_Adjusted_Price'],2)

df['Price_BTC'] = df['Price_USD_Noise'].astype("float")/df['Price_USD_Noise'].astype("float")[0]

frames = [df['Rank']
         ,df['Coin_Id']
         ,df['Icon']          
         ,df['Symbol']
         ,df['Name']
         ,df['Price_USD_Noise']
         ,df['Btc_Adjusted_Price']
         ,df['Circulating_Supply']
         ,df['Market_Cap_USD_Noise']
         ,df['Max_Btc_Multiplier']
         ,df['Price_BTC']]

df_load = pandas.concat(frames, axis=1)

print(df_load.head(10))

ts = time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

df_load['Create_Date'] = timestamp

#loading data into database
engine = create_engine('mysql+mysqlconnector:', echo=False)

df_load.to_sql(name='', con=engine, if_exists = 'replace', index=False)
