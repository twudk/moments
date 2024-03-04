import logging
import math
import os
import socket
from datetime import datetime, timedelta

import pandas as pd
from backtesting import Backtest

import macd_optimization_dao as dao_macd
import util.util as util
from optimization.trading import trading_optimization_dao as dao_trading
from strategy import strategy_moments as f

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = os.getenv("MYSQL_ADDRESS")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default port if not set
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = 'tw'
MYSQL_PASSWORD = 'tw'

HOSTNAME = socket.gethostname()

# Constants
TICKER = "SPY"
MAX_STOP_LIMIT = 5
CASH = 1_000_000
START_DATE = "2015-01-01"
END_DATE = "2024-03-03"


def preprocess_parameters(parameters_df, max_stop_limit):
    """Preprocess parameters DataFrame."""
    parameters_df['start_date'] = pd.to_datetime(parameters_df['start_date'])
    parameters_df['end_date'] = pd.to_datetime(parameters_df['end_date']) - timedelta(days=1)
    for column in ['sleep_after_loss', 'max_days', 'profit_target', 'stop_limit']:
        parameters_df[column] = parameters_df[column].round().astype('Int64')
    return parameters_df[parameters_df['stop_limit'] <= max_stop_limit]


def merge_prices_with_parameters(prices_df, parameters_df, ticker):
    """Merge prices DataFrame with parameters DataFrame."""
    logging.info(f"Merging prices with parameters for {ticker}")
    prices_df.reset_index(inplace=True)
    prices_df['Date'] = pd.to_datetime(prices_df['Date'])
    parameters_filtered_df = parameters_df[parameters_df['symbol'] == ticker]
    parameters_filtered_df = (parameters_filtered_df.dropna(subset=['profit_target'])
                              .drop_duplicates(subset=['batch_id', 'request_id'], keep='first'))
    parameters_filtered_df['end_date'] = pd.to_datetime(parameters_filtered_df['end_date'])
    price_with_parameters_df = pd.merge(prices_df, parameters_filtered_df, left_on='Date', right_on='end_date', how='left')
    price_with_parameters_df.sort_values(by='Date', inplace=True)
    price_with_parameters_df.fillna(method='ffill', inplace=True)
    return price_with_parameters_df.set_index('Date').dropna(subset=['profit_target'])


def backtest_strategy(stock_data, strategy, cash=CASH):
    backtest = Backtest(stock_data, strategy, cash=cash, exclusive_orders=True, trade_on_close=True)

    optimize_on = 'Return [%]'
    opt_stats_x, heatmap = backtest.optimize(
        n1=[19],
        n2=[39],
        macd_threshold=range(0, 60, 10),
        skip_trend=[True, False],
        maximize=optimize_on,
        return_heatmap=True
    )

    return opt_stats_x


def main():
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d').date()

    batch_id = util.generate_uuid()
    tickers = [TICKER]
    sampling_frequency = 5
    walk_back_in_days = 30 * 6

    for ticker in tickers:
        stock_data = util.download_and_adjust_stock_data(ticker, start_date, end_date)

        for sampling_end_date in stock_data.iloc[::sampling_frequency].index.date:
            sampling_start_date = sampling_end_date - timedelta(days=walk_back_in_days)

            prices_df = util.download_and_adjust_stock_data(TICKER, sampling_start_date, sampling_end_date)
            parameters_df = dao_trading.get_trading_parameters_by_symbol_and_date_range(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, TICKER, sampling_start_date, sampling_end_date)
            parameters_df = preprocess_parameters(parameters_df, MAX_STOP_LIMIT)
            price_with_parameters_df = merge_prices_with_parameters(prices_df, parameters_df, TICKER)
            opt_stats = backtest_strategy(price_with_parameters_df, f.Moments)

            macd_threshold = opt_stats._strategy.macd_threshold
            skip_trend = opt_stats._strategy.skip_trend
            data = {
                'batch_id': batch_id,
                'request_id': util.generate_uuid(),
                'symbol': ticker,
                'start_date': sampling_start_date,  # Date in 'YYYY-MM-DD' format
                'end_date': sampling_end_date,  # Date in 'YYYY-MM-DD' format
                'macd_threshold': macd_threshold,
                'skip_trend': 1 if skip_trend else 0,
                'exposure_time': None if math.isnan(opt_stats['Exposure Time [%]']) else opt_stats['Exposure Time [%]'],
                'return_pct': None if math.isnan(opt_stats['Return [%]']) else opt_stats['Return [%]'],
                'buy_and_hold_return_pct': None if math.isnan(opt_stats['Buy & Hold Return [%]']) else opt_stats['Buy & Hold Return [%]'],
                'max_draw_down': None if math.isnan(opt_stats['Max. Drawdown [%]']) else opt_stats['Max. Drawdown [%]'],
                'sqn': None if math.isnan(opt_stats['SQN']) else opt_stats['SQN'],
                'handler_host': HOSTNAME,
            }
            logging.info(f"Optimized Threshold: {macd_threshold}, Skip Trend: {skip_trend}, Date: {sampling_end_date}")
            dao_macd.add_opt_trading_parameters(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, data)


if __name__ == "__main__":
    main()
