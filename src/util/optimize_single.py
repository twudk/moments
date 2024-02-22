import datetime
import src.strategy.moments as f
from backtesting import Backtest

optimize_on = 'SQN'
sample_step = 1
ticker = "SPY"
end_date = datetime.date.today()
duration = datetime.timedelta(days=365 * 4)

ROUNDING_DIGITS = 4
CASH = 1000000

# Load prices
data = f.download_stock_data(ticker, end_date - duration, end_date)

start_date_x, end_date_x = f.get_start_end_date(365 * 0, 365 * 2)
stock_data = f.get_subrange_of_days(data, start_date_x, end_date_x)

backtest_x = Backtest(stock_data, f.MyStrategy, cash=CASH, exclusive_orders=True, trade_on_close=True)

opt_stats_x, heatmap = backtest_x.optimize(
    o_profit_target=range(2, 10, sample_step),
    o_stop_limit=range(2, 10, sample_step),
    o_max_days=range(16, 24, sample_step),
    o_sleep_after_loss=range(0, 10, sample_step),
    maximize=optimize_on,
    return_heatmap=True
)
