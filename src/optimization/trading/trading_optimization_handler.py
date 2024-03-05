import logging
import math
import os
import socket
import time

from backtesting import Backtest

import trading_optimization_dao as dao
import util.util as util
from strategy import strategy_moments as f

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = os.getenv("MYSQL_ADDRESS")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default port if not set
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = 'tw'
MYSQL_PASSWORD = 'tw'

HOSTNAME = socket.gethostname()


def process_message(data):
    batch_id, request_id = data['batch_id'], data['request_id']
    symbol, optimize_on = data['symbol'], 'SQN'  # For now only optimize for SQN
    sampling_step = data['sampling_step']
    start_date, end_date = data['start_date'], data['end_date']

    if dao.check_request_id_exists(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, request_id):
        return

    stock_data = util.download_and_adjust_stock_data(symbol, start_date, end_date)
    backtest_x = Backtest(stock_data, f.Moments, cash=1000000, exclusive_orders=True, trade_on_close=True)

    opt_stats, heatmap = backtest_x.optimize(
        o_profit_target=range(2, 10, sampling_step),
        o_stop_limit=range(2, 5, sampling_step),
        o_max_days=range(16, 24, sampling_step),
        o_sleep_after_loss=range(2, 10, sampling_step),
        maximize=optimize_on,
        return_heatmap=True
    )

    result_df = heatmap.reset_index()
    max_val = result_df[optimize_on].max()
    top_result = result_df[result_df[optimize_on] == max_val].sort_values(by='o_stop_limit', ascending=True).iloc[0]

    data_to_insert = {
        "batch_id": batch_id,
        "request_id": request_id,
        "symbol": symbol,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "profit_target": int(top_result['o_profit_target']),
        "stop_limit": int(top_result['o_stop_limit']),
        "sleep_after_loss": int(top_result['o_sleep_after_loss']),
        "max_days": int(top_result['o_max_days']),
        'exposure_time': None if math.isnan(opt_stats['Exposure Time [%]']) else opt_stats['Exposure Time [%]'],
        'return_pct': None if math.isnan(opt_stats['Return [%]']) else opt_stats['Return [%]'],
        'buy_and_hold_return_pct': None if math.isnan(opt_stats['Buy & Hold Return [%]']) else opt_stats['Buy & Hold Return [%]'],
        'max_draw_down': None if math.isnan(opt_stats['Max. Drawdown [%]']) else opt_stats['Max. Drawdown [%]'],
        'sqn': None if math.isnan(opt_stats['SQN']) else opt_stats['SQN'],
        'handler_host': HOSTNAME,
    }

    dao.add_opt_trading_parameters(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, data_to_insert)


def main():
    while True:
        try:
            reserved = dao.get_reserved_opt_req_trading_parameter(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, HOSTNAME)

            if reserved is None:
                dao.reserve_opt_req_trading_parameter(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, HOSTNAME, 1)
                process = dao.get_reserved_opt_req_trading_parameter(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, HOSTNAME)
            else:
                process = reserved

            if process is not None:
                logging.info('Process ' + str(process))
                process_message(process)
                id = process.get('id')
                dao.upd_status_opt_req_trading_parameter(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, id, 'COMPLETED')

            time.sleep(2)
        except Exception as e:
            logging.info(f"Error:  {e}")


if __name__ == "__main__":
    main()
