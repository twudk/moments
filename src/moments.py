import yfinance as yf
import datetime
from typing import Optional, Tuple
import pandas as pd
import pandas_ta as ta
from backtesting import Strategy
import uuid


def generate_random_uuid():
    # Generate a random UUID (UUID4)
    random_uuid = uuid.uuid4()
    return str(random_uuid)


def save_to_file(filename, data):
    with open(filename, 'a') as file:
        file.write(data + '\n')


def download_stock_data(
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


def calculate_daily_returns(stock_data: pd.DataFrame) -> pd.Series:
    """
    Calculate and return the daily returns from the stock data.
    """
    return stock_data["Close"].pct_change()


def days_to_target(
        stock_data: pd.DataFrame,
        start_index: int,
        o_target_profit: int,
        o_std_loss_limit: int,
        o_max_days: int,
) -> Tuple[int, float, float, datetime.date, bool]:
    """
    Calculate the number of days until the stock reaches the target profit or loss limit,
    up to a maximum of max_days. Also returns the stock price and the last day of investment on that day.
    """
    start_price = stock_data.iloc[start_index]["Close"]

    r_max_days = None
    r_final_return = None
    r_final_price = None
    r_final_date = None

    max_data_cnt = len(stock_data) - start_index
    for i in range(1, max_data_cnt):
        index_i = start_index + i
        current_price = stock_data.iloc[index_i]["Close"]
        current_date = stock_data.index[index_i]
        accumulated_return = (current_price - start_price) / start_price

        current_target_profit = o_target_profit / 100 if o_target_profit is not None \
            else stock_data.iloc[index_i]["profit_target"] / 100 if "profit_target" in stock_data.columns \
            else None
        current_stop_limit = -o_std_loss_limit / 100 if o_std_loss_limit is not None \
            else - stock_data.iloc[index_i]["stop_limit"] / 100 if "stop_limit" in stock_data.columns \
            else None
        current_max_days = o_max_days if o_max_days is not None \
            else stock_data.iloc[index_i]["max_days"] if "max_days" in stock_data.columns \
            else None

        if current_target_profit is None or current_stop_limit is None or current_max_days is None:
            return i, accumulated_return, current_price, current_date, False

        if accumulated_return >= current_target_profit:
            return i, accumulated_return, current_price, current_date, False

        if accumulated_return <= current_stop_limit:
            return i, accumulated_return, current_price, current_date, True

        if i >= current_max_days:
            return i, accumulated_return, current_price, current_date, False

        r_max_days = i
        r_final_return = accumulated_return
        r_final_price = current_price
        r_final_date = current_date

    return r_max_days, r_final_return, r_final_price, r_final_date, False


def calculate_target_days(
        stock_data: pd.DataFrame,
        o_target_profit: int,
        o_std_loss_limit: int,
        o_max_days: int,
) -> pd.DataFrame:
    """
    For each day in the data, calculate the number of days to reach the target profit or hit the loss limit.
    Returns a DataFrame with results including the stock price on the target/limit day, the last day of investment,
    and the "avoid investment day" which is the next day which is the next investment day of a loss investment.
    """
    results = {
        "date": [],
        "days_to_target": [],
        "final_return": [],
        "stock_price": [],
        "last_investment_day": [],
        "last_investment_day_next": [],
        "excessive_loss": [],
    }

    for start_index in range(len(stock_data) - 1):
        (
            days_needed,
            investment_return,
            stock_price,
            last_investment_day,
            excessive_loss
        ) = days_to_target(stock_data, start_index, o_target_profit, o_std_loss_limit, o_max_days)

        # Determine the next investment day (the day after last_investment_day)
        next_day_index = stock_data.index.get_loc(last_investment_day) + 1
        last_investment_day_next = (
            stock_data.index[next_day_index].date()
            if next_day_index < len(stock_data)
            else None
        )

        results["date"].append(str(stock_data.index[start_index].date()))
        results["days_to_target"].append(days_needed)
        results["final_return"].append(investment_return)
        results["stock_price"].append(stock_price)
        results["last_investment_day"].append(
            str(last_investment_day.date()) if investment_return < 0 else None
        )
        results["last_investment_day_next"].append(
            str(last_investment_day_next) if investment_return < 0 else None
        )
        results["excessive_loss"].append(
            str("Y") if excessive_loss else "N"
        )

    return pd.DataFrame(results)


def calculate_last_investment_day_extended(
        stock_data: pd.DataFrame,
        o_profit_target: int,
        o_stop_limit: int,
        o_max_days: int,
        o_sleep_after_loss: int,
) -> Tuple[set, set]:
    """
    Calculate the last investment day and continue loss dates based on the specified parameters.
    """
    results_df = calculate_target_days(stock_data, o_profit_target, o_stop_limit, o_max_days)

    # Extract the last investment days
    last_investment_days = set(results_df[results_df['excessive_loss'] == 'Y']["last_investment_day"].dropna())

    # Extract the date sequence from results_df
    date_sequence = results_df["date"]

    # Check for the conditions and filter the dates
    sleep_days = set()
    for date in last_investment_days:
        if date in date_sequence.values:
            date_index = date_sequence[date_sequence == date].index[0]
            sleep_after_loss = o_sleep_after_loss if o_sleep_after_loss is not None \
                else stock_data.iloc[date_index]["sleep_after_loss"] if "sleep_after_loss" in stock_data.columns \
                else None
            if sleep_after_loss is None:
                return set(), set()
            sleep_dates = date_sequence[date_index: date_index + sleep_after_loss] \
                if date_index + sleep_after_loss <= len(date_sequence) \
                else date_sequence[date_index: len(date_sequence)]
            for nd in sleep_dates:
                sleep_days.add(nd)

    return last_investment_days, sleep_days


def get_subrange_of_days(
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


def get_start_end_date(
        to_date: int, walk_back: int
) -> Tuple[datetime.date, datetime.date]:
    """
    Calculates and returns the start and end dates based on the specified number of days to walk back from today.
    """
    end_date = datetime.date.today() - datetime.timedelta(days=to_date)
    start_date = end_date - datetime.timedelta(days=walk_back)
    return start_date, end_date


class MyStrategy(Strategy):
    o_profit_target: int = None
    o_stop_limit: int = None
    o_max_days: int = None
    o_sleep_after_loss: int = None

    days_elapse = 0
    days_held = 0
    last_investment_day = None
    continue_loss = None
    entry_price = None

    skip_trend = True
    n1 = None
    n2 = None
    macd_threshold = None
    up_trend_macd = None

    trading_start_date = datetime.date.today()

    def init(self):
        super().init()

        if not self.skip_trend:
            self.macd = self.I(ta.macd, pd.Series(self.data.Close), self.n1, self.n2)

        self.days_elapse = 0
        self.days_held = 0
        self.last_investment_day = None
        self.continue_loss = None
        self.entry_price = None

        if self.data.df is None:
            return

        (
            self.last_investment_day,
            self.continue_loss,
        ) = calculate_last_investment_day_extended(
            self.data.df,
            self.o_profit_target,
            self.o_stop_limit,
            self.o_max_days,
            self.o_sleep_after_loss,
        )
        # last_inv_list = list(self.last_investment_day)
        # last_inv_list.sort(reverse=True)
        # continue_loss = list(self.continue_loss)
        # continue_loss.sort(reverse=True)
        # print("Exit days:" + str(last_inv_list[:10]))
        # print("Coll-down days:" + str(continue_loss[:10]))

    def next(self):
        super().next()

        close = self.data.Close[-1]

        if not self.skip_trend:
            if self.macd[1][-1] > self.macd_threshold / 100:
                self.up_trend_macd = True
            else:
                self.up_trend_macd = False

        self.days_elapse = self.days_elapse + 1
        current_price = close
        current_holding_pl = (
            (current_price - self.entry_price) / self.entry_price
            if current_price is not None and self.entry_price
            else None
        )
        current_date = self.data.index[-1].date()
        current_date_str = str(current_date)

        # print(current_date_str)

        target_profit = self.o_profit_target / 100 if self.o_profit_target is not None \
            else self.data.profit_target[-1] / 100 if 'profit_target' in self.data.df.columns \
            else None
        stop_limit = -self.o_stop_limit / 100 if self.o_stop_limit is not None \
            else -self.data.stop_limit[-1] / 100 if 'stop_limit' in self.data.df.columns \
            else None
        max_days = self.o_max_days if self.o_max_days is not None \
            else self.data.max_days[-1] if 'max_days' in self.data.df.columns \
            else None

        if target_profit is None or stop_limit is None or max_days is None:
            return

        if self.position:
            # print(f'''{current_date} {target_profit} {stop_limit} {max_days} {current_holding_pl}''')
            if current_holding_pl <= stop_limit:
                # print(f'''exit due to max loss at {current_date}: {self.entry_price}
                #     -> {current_price} = {current_holding_pl} pl_pct={self.position.pl_pct} stop_limit={stop_limit}''')
                self.position.close()
                self.entry_price = None
                self.days_held = 0
                return
            elif (
                    self.days_held > max_days
                    or self.position.pl_pct >= target_profit
            ) and (current_date_str in self.last_investment_day
                   or current_date_str in self.continue_loss):
                # print(f'''exit due to max days at {current_date}: {self.entry_price}
                #     -> {current_price} = {current_holding_pl} pl_pct={self.position.pl_pct} stop_limit={stop_limit}''')
                self.position.close()
                self.entry_price = None
                self.days_held = 0
                return
            else:
                self.days_held = self.days_held + 1
        else:
            if (
                    (self.continue_loss is None or (current_date_str not in self.continue_loss))
                    and (self.last_investment_day is None or (current_date_str not in self.last_investment_day))
                    and self.days_elapse > max_days + 5
                    and (self.up_trend_macd or self.skip_trend)
            ):
                # print(f'''buy at {current_date}''')
                self.buy()
                self.days_held = self.days_held + 1
                self.entry_price = current_price
