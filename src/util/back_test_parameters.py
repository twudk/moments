import datetime
import strategy.moments as f
from backtesting import Backtest
import pandas as pd
import os

ticker = "XLK"
walk_back_in_days = 365 * 0
total_days_in_range = 365 * 4
max_stop_limit = 5
ROUNDING_DIGITS = 4
CASH = 1000000

end_date = datetime.date.today()

# Load prices
prices_df = f.download_stock_data(ticker, end_date - datetime.timedelta(days=total_days_in_range), end_date)

vol = prices_df['Close'].std() / prices_df['Close'].mean()

# Load parameters sheet
parameters_df = pd.read_csv('../../data/input/parameters_%s.csv' % ticker)
parameters_df['start_date'] = pd.to_datetime(parameters_df['start_date'])
parameters_df['end_date'] = pd.to_datetime(parameters_df['end_date']) + pd.Timedelta(days=1)
parameters_df['sleep_after_loss'] = parameters_df['sleep_after_loss'].round().astype(int)
parameters_df['max_days'] = parameters_df['max_days'].round().astype(int)
parameters_df = parameters_df[parameters_df['stop_limit'] <= max_stop_limit]

# Join prices with parameters
parameters_filtered_df = parameters_df[parameters_df['ticker'] == ticker]
prices_df.reset_index(inplace=True)
prices_df['Date'] = pd.to_datetime(prices_df['Date'])
set1 = set(prices_df['Date'])
set2 = set(parameters_filtered_df['end_date'])
set_x = set1.intersection(set2)
price_with_parameters_df = pd.merge(prices_df, parameters_filtered_df, left_on='Date', right_on='end_date', how='left')
price_with_parameters_df.sort_values(by='Date', inplace=True)
price_with_parameters_df.fillna(method='ffill', inplace=True)
price_with_parameters_df = price_with_parameters_df[price_with_parameters_df['index'].notna()]
price_with_parameters_df = price_with_parameters_df.set_index('Date')
price_with_parameters_df['target_profit'] = price_with_parameters_df['target_profit'].round().astype(int)
price_with_parameters_df['stop_limit'] = price_with_parameters_df['stop_limit'].round().astype(int)
price_with_parameters_df['sleep_after_loss'] = price_with_parameters_df['sleep_after_loss'].round().astype(int)
price_with_parameters_df['max_days'] = price_with_parameters_df['max_days'].round().astype(int)

# Prepare data to back test
start_date_x, end_date_x = f.get_start_end_date(walk_back_in_days, total_days_in_range)
stock_data = f.get_subrange_of_days(price_with_parameters_df, start_date_x, end_date_x)

# Back test


# with ctx.change_dir('../../report'):
os.chdir('../../report')
backtest = Backtest(stock_data, f.MyStrategy, cash=CASH, exclusive_orders=True, trade_on_close=True)
stats = backtest.run(n1=19, n2=39, vol=vol * 100, skip_trend=False)

# stats = backtest.run(n1=5, n2=35, vol=5, skip_trend=False)

backtest.plot()
print(stats)
print(stats._trades)
print("Vol=" + str(vol))
