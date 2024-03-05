import logging
import os
import socket
from datetime import datetime, timedelta

import trading_optimization_dao as dao
import util.util as util

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = os.getenv("MYSQL_ADDRESS")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))  # Default port if not set
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = 'tw'
MYSQL_PASSWORD = 'tw'
HOSTNAME = socket.gethostname()

if __name__ == '__main__':
    batch_id = util.generate_uuid()

    tickers = ["XLK"]  # Example ticker, extend as needed
    start_date = datetime.strptime('2014-01-01', '%Y-%m-%d').date()
    end_date = datetime.strptime('2024-03-05', '%Y-%m-%d').date()

    sampling_frequency = 1
    walk_back_in_days = 30 * 6

    data_list = []

    for ticker in tickers:
        stock_data = util.download_and_adjust_stock_data(ticker, start_date, end_date)

        for sampling_end_date in stock_data.iloc[::sampling_frequency].index.date:
            sampling_start_date = sampling_end_date - timedelta(days=walk_back_in_days)
            data = {
                'batch_id': batch_id,
                'request_id': util.generate_uuid(),
                'symbol': ticker,
                'optimize_on': 'SQN',
                'sampling_step': 1,  # This should be an integer
                'start_date': sampling_start_date,  # Date in 'YYYY-MM-DD' format
                'end_date': sampling_end_date,  # Date in 'YYYY-MM-DD' format
                'reserved_by': None,  # Optional, can be NULL if not reserved
                'reserved_until': None  # Optional, DATETIME format 'YYYY-MM-DD HH:MM:SS', can be NULL
            }
            data_list.append(data)

    dao.add_opt_req_trading_parameters_bulk(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, data_list)
