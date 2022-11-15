import os
import pandas as pd

def main():
    stock = input('Enter stock: ')
    split_dates = input('Enter split dates: (eg. 2020-02-20, 2019-02-20, ...)')
    by = input('Enter splits: (in order)')
    splits = [(date, n) for date, n in zip(split_dates.split(sep=','), by.split(sep=','))]

    data = load_data(stock)
    start = data['Date'].tail(1).iloc[0]
    end = data['Date'][0]
    dates = pd.DataFrame(pd.date_range(start, end), columns=['Date'])
    dates.keys = dates['Date']
    data = pd.merge_ordered(dates, data).fillna(method='pad')
    data.index = data['Date']
    data.drop(columns=['Date'], inplace=True)

    if split_dates:
        data = adjust_split(data, splits)
        data.to_csv(os.path.join('data', str(stock), str(stock)+'_pricedata_w_splits.csv'))
    else:
        data.to_csv(os.path.join('data', str(stock), str(stock)+'_pricedata_filled.csv'))

def load_data(stock):
    file = os.path.join('data', str(stock), str(stock)+'_pricedata.csv')
    data = pd.read_csv(file)
    data['Date'] = pd.to_datetime(data['Date'])
    data.keys = data['Date']
    return data

def adjust_split(data, splits):
    splits = [(pd.to_datetime(x[0]), x[1]) for x in splits]
    for date, n in splits:
        date += pd.Timedelta('1 day')
        cols = ['Price', 'Open', 'High', 'Low']
        data.loc[:date, cols] = data.loc[:date, cols].apply(lambda x: x*int(n))
    return data


if __name__ == '__main__':
    main()