import pymysql
import pandas as pd


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
            (batch_id, request_id, symbol, start_date, end_date, profit_target, stop_limit, sleep_after_loss, max_days, exposure_time, return_pct, buy_and_hold_return_pct, max_draw_down, sqn, handler_host) 
            VALUES 
            (%(batch_id)s, %(request_id)s, %(symbol)s, %(start_date)s, %(end_date)s, %(profit_target)s, %(stop_limit)s, %(sleep_after_loss)s, %(max_days)s, %(exposure_time)s, %(return_pct)s, %(buy_and_hold_return_pct)s, %(max_draw_down)s, %(sqn)s, %(handler_host)s)"""

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


def add_opt_req_trading_parameters_bulk(host, port, user, password, db, data_list):
    """
    Inserts multiple rows of trading parameters data into the opt_req_trading_parameters table,
    including reservation information.

    Parameters:
    - host: Database host address
    - port: Database port
    - user: Database user
    - password: Database password
    - db: Database name
    - data_list: A list of dictionaries, where each dictionary contains the data for one row
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL for inserting data
            sql = """INSERT INTO opt_req_trading_parameters 
            (batch_id, request_id, symbol, optimize_on, sampling_step, start_date, end_date, reserved_by, reserved_until) 
            VALUES 
            (%(batch_id)s, %(request_id)s, %(symbol)s, %(optimize_on)s, %(sampling_step)s, %(start_date)s, %(end_date)s, %(reserved_by)s, %(reserved_until)s)"""

            # Execute SQL command
            cursor.executemany(sql, data_list)
            connection.commit()
    finally:
        connection.close()


def reserve_opt_req_trading_parameter(host, port, user, password, db, processor_name, reservation_duration_hours):
    """
    Atomically reserves an unreserved or expired row in the opt_req_trading_parameters table
    using a single SQL UPDATE statement to prevent race conditions, using database server's UTC time for reservations.

    Parameters:
    - host: Database host address
    - port: Database port
    - user: Database user
    - password: Database password
    - db: Database name
    - processor_name: The name of the processor reserving the row
    - reservation_duration_hours: The duration in hours for which the row should be reserved
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL to atomically update an unreserved or expired row using database server's UTC time
            update_sql = """
            UPDATE opt_req_trading_parameters
            SET reserved_by = %s, reserved_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s HOUR)
            WHERE id = (
                SELECT id FROM (
                    SELECT id FROM opt_req_trading_parameters
                    WHERE STATUS IS NULL AND (reserved_by IS NULL OR reserved_until < UTC_TIMESTAMP())
                    ORDER BY id ASC
                    LIMIT 1
                ) AS subquery
            )
            """

            # Execute update SQL command
            affected_rows = cursor.execute(update_sql, (processor_name, reservation_duration_hours))
            connection.commit()

            return affected_rows > 0  # True if a row was reserved, False otherwise
    finally:
        connection.close()


def get_reserved_opt_req_trading_parameter(host, port, user, password, db, processor_name):
    """
    Selects the earliest reserved row for the provided processor from the
    opt_req_trading_parameters table.

    Parameters:
    - host: Database host address
    - port: Database port
    - user: Database user
    - password: Database password
    - db: Database name
    - processor_name: The name of the processor for which the row is reserved

    Returns:
    - A dictionary with the row data, or None if no row is found
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL to select the earliest reserved row for the provided processor
            sql = """
            SELECT * FROM opt_req_trading_parameters
            WHERE reserved_by = %s and status is null and reserved_until > UTC_TIMESTAMP()
            ORDER BY id ASC
            LIMIT 1
            """

            # Execute the SQL command
            cursor.execute(sql, (processor_name,))
            row = cursor.fetchone()

            if row:
                # Assuming you know the column order, you can construct a dict
                # Alternatively, use cursor.description to get column names dynamically
                columns = ['id', 'batch_id', 'request_id', 'symbol', 'optimize_on', 'sampling_step', 'start_date', 'end_date', 'reserved_by', 'reserved_until']
                row_data = dict(zip(columns, row))
                return row_data
            else:
                return None
    finally:
        connection.close()


def upd_status_opt_req_trading_parameter(host, port, user, password, db, row_id, new_status):
    """
    Updates the 'status' column for a specific row in the 'opt_req_trading_parameters' table.

    Parameters:
    - host: Database host address
    - port: Database port
    - user: Database user
    - password: Database password
    - db: Database name
    - row_id: The ID of the row to update
    - new_status: The new status value to set
    """
    # Connect to the database
    connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    try:
        with connection.cursor() as cursor:
            # SQL command to update the 'status' column
            sql = "UPDATE opt_req_trading_parameters SET status = %s WHERE id = %s"

            # Execute the SQL command
            cursor.execute(sql, (new_status, row_id))
            connection.commit()
            print(f"Status updated to '{new_status}' for ID {row_id}.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        connection.close()


def get_trading_parameters_by_symbol_and_date_range(host, port, user, password, db, symbol, start_date, end_date):
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
                opt_trading_parameters
            WHERE
                id IN ((SELECT 
                        MAX(id)
                    FROM
                        opt_trading_parameters
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
