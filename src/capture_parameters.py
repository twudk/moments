import datetime
import moments as f
from backtesting import Backtest

tickers = ["XLK", "SPY", "XLF", "XLE"]

total_days_in_range = 365 * 5  # 5 years
unit_in_days = 7  # Capture every 7 days
walk_back_in_days = 30 * 6  # Walk back 6 months

rounding_digits = 4
cash = 1000000
optimize_on = 'SQN'
sample_step = 1

output_dir = '../data/output'

for ticker in tickers:

    data = f.download_stock_data(ticker, datetime.date.today() - datetime.timedelta(days=total_days_in_range),
                                 datetime.date.today())
    output_file_parameter = '%s/parameters_%s.csv' % (output_dir, ticker)
    for offset in range(0, int((total_days_in_range - walk_back_in_days) / unit_in_days) - 1):
        try:
            start_date, end_date = f.get_start_end_date(unit_in_days * offset, walk_back_in_days)
            stock_data = f.get_subrange_of_days(data, start_date, end_date)
            vol = stock_data['Close'].std()
            backtest_x = Backtest(stock_data, f.MyStrategy, cash=cash, exclusive_orders=True, trade_on_close=True)

            opt_stats_x, heatmap = backtest_x.optimize(
                o_profit_target=range(2, 10, sample_step),
                o_stop_limit=range(2, 5, sample_step),
                o_max_days=range(16, 24, sample_step),
                o_sleep_after_loss=range(2, 10, sample_step),
                maximize=optimize_on,
                return_heatmap=True
            )

            exp_x = opt_stats_x['Exposure Time [%]']
            ret_x = opt_stats_x['Return [%]']
            bh_x = opt_stats_x['Buy & Hold Return [%]']

            f.save_to_file(output_file_parameter,
                           f"{ticker},{offset},{start_date},{end_date},{opt_stats_x._strategy.o_profit_target},{opt_stats_x._strategy.o_stop_limit},{opt_stats_x._strategy.o_sleep_after_loss},{opt_stats_x._strategy.o_max_days},{ret_x},{bh_x}")

        except Exception as e:
            print(e)
