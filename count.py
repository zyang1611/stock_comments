import os
import pandas as pd

stock = input('Enter stock: ')
data = pd.read_csv(os.path.join('data', str(stock), str(stock)+'.csv'), parse_dates=['Date'])
data['Date'] = data['Date'].dt.normalize()
data.drop(columns='PageNum', inplace=True)

pricedata = pd.read_csv(os.path.join('data', str(stock), str(stock)+'_pricedata_filled.csv'), parse_dates=['Date'])

counts = data.groupby(['Date']).count()
price = pd.Series([pricedata.loc[pricedata['Date'] == date, 'Price'].iloc[0] for date in counts.index])

counts = counts.assign(Price=price.values)
counts.to_csv(os.path.join('data', str(stock), str(stock)+'_commcounts.csv'))
