import pymysql


def add_opt_trading_parameters(host, port, user, password, db, data):
    """
    Inserts a single row of trading parameters data into the trading_data table.

    Parameters:
    - host: Database host address
    - user: Database user
    - password: Database password
    - db: Database name
    - data: A dictionary containing the data to insert
    """
    # Connect to the database
    connection = pymysql.connect(host=host, user=user, password=password, db=db, port=port)
    try:
        with connection.cursor() as cursor:
            # SQL for inserting data
            sql = """INSERT INTO opt_trading_parameters 
            (batch_id, request_id, symbol, start_date, end_date, profit_target, stop_limit, sleep_after_loss, max_days, exposure_time, return_pct, buy_and_hold_return_pct, max_draw_down, sqn, handler_host, request_partition, offset) 
            VALUES 
            (%(batch_id)s, %(request_id)s, %(symbol)s, %(start_date)s, %(end_date)s, %(profit_target)s, %(stop_limit)s, %(sleep_after_loss)s, %(max_days)s, %(exposure_time)s, %(return_pct)s, %(buy_and_hold_return_pct)s, %(max_draw_down)s, %(sqn)s, %(handler_host)s, %(request_partition)s, %(offset)s)"""

            # Execute SQL command
            cursor.execute(sql, data)
            connection.commit()
    finally:
        connection.close()


def check_request_id_exists(host, port, user, password, db, request_id):
    """
    Checks if a given request_id exists in the trading_data table.

    Parameters:
    - host: Database host address
    - user: Database user
    - password: Database password
    - db: Database name
    - request_id: The request_id to check

    Returns:
    - True if the request_id exists, False otherwise
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL for checking existence
            sql = "SELECT 1 FROM opt_trading_parameters WHERE request_id = %s"
            cursor.execute(sql, (request_id,))
            result = cursor.fetchone()
            return result is not None
    finally:
        connection.close()
