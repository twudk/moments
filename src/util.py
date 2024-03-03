import datetime
import uuid
from typing import Optional
import pandas as pd
import yfinance as yf


def generate_uuid():
    return str(uuid.uuid4())


def download_and_adjust_stock_data(
        symbol: str, start_date: datetime.date, end_date: datetime.date
) -> Optional[pd.DataFrame]:
    """
    Download historical stock data for a given symbol.
    Returns a DataFrame or None if an error occurs.
    """
    try:
        price_df = yf.download(symbol, start=start_date, end=end_date)
        price_df['Open'] = price_df['Open'] + price_df['Adj Close'] - price_df['Close']
        price_df['High'] = price_df['High'] + price_df['Adj Close'] - price_df['Close']
        price_df['Low'] = price_df['Low'] + price_df['Adj Close'] - price_df['Close']
        price_df['Close'] = price_df['Adj Close']
        return price_df
    except Exception as e:
        print(f"Error downloading stock data: {e}")
        return None


def filter_stock_data_in_date_range(
        stock_data: pd.DataFrame, start: datetime.date, end: datetime.date
) -> pd.DataFrame:
    """
    Returns a subrange of the stock_data DataFrame between the specified start and end dates.
    """
    # Convert start and end dates to pandas Timestamp to match DataFrame index
    start_timestamp = pd.Timestamp(start)
    end_timestamp = pd.Timestamp(end)

    # Filter the DataFrame for the date range
    mask = (stock_data.index >= start_timestamp) & (stock_data.index <= end_timestamp)
    return stock_data.loc[mask]
