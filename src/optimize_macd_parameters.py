from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import moments as f
from backtesting import Backtest

logging.basicConfig(level=logging.INFO)

# Constants
TICKER = "SPY"
MAX_STOP_LIMIT = 5
CASH = 1_000_000
START_DATE = "2015-01-01"
END_DATE = "2024-02-29"
PARAMETERS_FILE_TEMPLATE = "../data/input/parameters_v2_{}.csv"
REPORT_DIRECTORY = "../report"


def load_prices(ticker, start_date, end_date):
    """Load stock prices from moments."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    logging.info(f"Loading stock data for {ticker} from {start} to {end}")
    return f.download_stock_data(ticker, start, end)


def preprocess_parameters(parameters_df, max_stop_limit):
    """Preprocess parameters DataFrame."""
    parameters_df['start_date'] = pd.to_datetime(parameters_df['start_date'])
    parameters_df['end_date'] = pd.to_datetime(parameters_df['end_date']) - timedelta(days=1)
    for column in ['sleep_after_loss', 'max_days', 'profit_target', 'stop_limit']:
        parameters_df[column] = parameters_df[column].round().astype('Int64')
    logging.info("Preprocessing parameters DataFrame")
    return parameters_df[parameters_df['stop_limit'] <= max_stop_limit]


def merge_prices_with_parameters(prices_df, parameters_df, ticker):
    """Merge prices DataFrame with parameters DataFrame."""
    logging.info(f"Merging prices with parameters for {ticker}")
    prices_df.reset_index(inplace=True)
    prices_df['Date'] = pd.to_datetime(prices_df['Date'])
    parameters_filtered_df = parameters_df[parameters_df['symbol'] == ticker]
    parameters_filtered_df = (parameters_filtered_df.dropna(subset=['profit_target'])
                              .drop_duplicates(subset=['batch_id', 'request_id'], keep='first'))
    price_with_parameters_df = pd.merge(prices_df, parameters_filtered_df, left_on='Date', right_on='end_date', how='left')
    price_with_parameters_df.sort_values(by='Date', inplace=True)
    price_with_parameters_df.fillna(method='ffill', inplace=True)
    return price_with_parameters_df.set_index('Date').dropna()


def backtest_strategy(stock_data, strategy, cash=CASH):
    """Run backtest on the strategy."""
    os.chdir(REPORT_DIRECTORY)
    backtest = Backtest(stock_data, strategy, cash=cash, exclusive_orders=True, trade_on_close=True)

    optimize_on = 'Return [%]'
    logging.info("Optimizing backtest strategy")
    opt_stats_x, heatmap = backtest.optimize(
        n1=[19],
        n2=[39],
        macd_threshold=range(0, 60, 10),
        skip_trend=[True, False],
        maximize=optimize_on,
        return_heatmap=True
    )
    macd_threshold = opt_stats_x._strategy.macd_threshold
    skip_trend = opt_stats_x._strategy.skip_trend

    # macd_threshold = 30
    # skip_trend = False

    stats = backtest.run(n1=19, n2=39, macd_threshold=macd_threshold, skip_trend=skip_trend)
    backtest.plot()

    logging.info(f"Backtesting complete with stats: {stats}")
    logging.info(f"Optimized Threshold: {macd_threshold}, Skip Trend: {skip_trend}")


def main():
    logging.info("Backtesting process started")
    start_date, end_date = START_DATE, END_DATE
    prices_df = load_prices(TICKER, start_date, end_date)
    parameters_df = pd.read_csv(PARAMETERS_FILE_TEMPLATE.format(TICKER))
    parameters_df = preprocess_parameters(parameters_df, MAX_STOP_LIMIT)
    price_with_parameters_df = merge_prices_with_parameters(prices_df, parameters_df, TICKER)
    stock_data = f.get_subrange_of_days(price_with_parameters_df, datetime.strptime(start_date, '%Y-%m-%d').date(), datetime.strptime(end_date, '%Y-%m-%d').date())
    backtest_strategy(stock_data, f.MyStrategy)


if __name__ == "__main__":
    main()
