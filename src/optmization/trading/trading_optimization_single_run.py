# Standard library imports
from datetime import datetime

# Third-party library imports
from backtesting import Backtest

import util.util as util
# Local application imports
from strategy import strategy_moments as f

# Constants are capitalized and underscore-separated
TICKER = "XLV"
CASH = 1_000_000
START_DATE = "2022-01-01"
END_DATE = "2024-03-03"
OPTIMIZE_ON = 'SQN'
SAMPLE_STEP = 1
ROUNDING_DIGITS = 4  # Note: This constant is defined but not used in the snippet provided

# Convert string dates to datetime objects
start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
end_date = datetime.strptime(END_DATE, "%Y-%m-%d").date()

# Load stock data using the moments library
data = util.download_and_adjust_stock_data(TICKER, start_date, end_date)

# Assuming get_start_end_date and get_subrange_of_days are correctly implemented in 'moments'
# Here, we directly use start_date and end_date without additional calculation
stock_data = util.filter_stock_data_in_date_range(data, start_date, end_date)

# Initialize Backtest object
backtest = Backtest(stock_data, f.Moments, cash=CASH, exclusive_orders=True, trade_on_close=True)

# Optimization with the backtesting library
opt_stats, heatmap = backtest.optimize(
    o_profit_target=range(2, 10, SAMPLE_STEP),
    o_stop_limit=range(2, 10, SAMPLE_STEP),
    o_max_days=range(16, 24, SAMPLE_STEP),
    o_sleep_after_loss=range(0, 10, SAMPLE_STEP),
    maximize=OPTIMIZE_ON,
    return_heatmap=True
)
