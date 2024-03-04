import pandas as pd
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
            sql = """INSERT INTO opt_macd_threashold 
            (batch_id, request_id, symbol, start_date, end_date, macd_threshold, skip_trend, exposure_time, return_pct, buy_and_hold_return_pct, max_draw_down, sqn, handler_host) 
            VALUES 
            (%(batch_id)s, %(request_id)s, %(symbol)s, %(start_date)s, %(end_date)s, %(macd_threshold)s, %(skip_trend)s, %(exposure_time)s, %(return_pct)s, %(buy_and_hold_return_pct)s, %(max_draw_down)s, %(sqn)s, %(handler_host)s)"""

            # Execute SQL command
            cursor.execute(sql, data)
            connection.commit()
    finally:
        connection.close()


def get_macd_parameters_by_symbol_and_date_range(host, port, user, password, db, symbol, start_date, end_date):
    """
    Retrieves trading parameters for a given symbol and date range, selecting one row per end_date
    with the largest id.

    Parameters:
    - host: Database host address
    - port: Database port
    - user: Database user
    - password: Database password
    - db: Database name
    - symbol: The trading symbol to filter by
    - start_date: The start of the date range (inclusive)
    - end_date: The end of the date range (inclusive)

    Returns:
    - A pandas DataFrame containing the filtered rows.
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL query to retrieve the data
            sql = """
            SELECT 
                *
            FROM
                opt_macd_threashold
            WHERE
                id IN ((SELECT 
                        MAX(id)
                    FROM
                        opt_macd_threashold
                    WHERE
                        symbol = %s AND end_date BETWEEN %s AND %s
                    GROUP BY end_date))
            """

            # Execute the SQL query
            cursor.execute(sql, (symbol, start_date, end_date))

            # Fetch all rows
            rows = cursor.fetchall()

            # Assuming you know the column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to pandas DataFrame
            df = pd.DataFrame(rows, columns=columns)

            return df
    finally:
        connection.close()
