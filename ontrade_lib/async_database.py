import os
from asyncpg import connect
import pandas as pd

async def get_async_db_connection():
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_name = os.environ["DB_NAME"]
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    return await connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)

async def get_knowledge_dates(db_conn, ticker, year):
    rows = await db_conn.fetch("select ticker, year, quarter, earnings_call_date from earnings_call_dates where ticker = $1 and year >= $2 order by year, quarter", ticker, year)
    knowledge_dates_df = pd.DataFrame(rows, columns=['ticker', 'year', 'quarter', 'earningsCallDate'])
    knowledge_dates_df['earningsCallDate'] = pd.to_datetime(knowledge_dates_df['earningsCallDate'])
    # Following is for the edge case where multiple quarters have the same earnings call date
    knowledge_dates_df.drop_duplicates(subset=['earningsCallDate'], keep='last', inplace=True)
    return knowledge_dates_df

async def get_daily_index(db_conn, ticker, earliest_date):
    price_rows = await db_conn.fetch("select ticker, trading_date from daily_stock_prices where ticker = $1 and trading_date >= $2 order by trading_date", ticker, earliest_date)
    daily_index_df = pd.DataFrame(price_rows, columns=['ticker', 'tradingDate'])
    daily_index_df['tradingDate'] = pd.to_datetime(daily_index_df['tradingDate'])
    return daily_index_df

async def get_latest_knowledge_date(ticker, db_conn):
    values = await db_conn.fetchrow("select max(earnings_call_date) from earnings_call_dates where ticker = $1", ticker)
    return values[0]

async def get_latest_factor_data(ticker, db_conn, table_name):
    # get the latest knowledge date with corresponding year and quarter in the factor_book_value_per_share table
    values = await db_conn.fetchrow(f"select year, quarter, earnings_call_date, trading_date from {table_name} where ticker = $1 order by trading_date desc", ticker)
    if values is None:
        return None, None, None, None
    return values[0], values[1], values[2], values[3]

async def get_daily_adjusted_prices_index(db_conn, ticker, earliest_date):
    price_rows = await db_conn.fetch("select ticker, trading_date, close_price from daily_stock_prices where ticker = $1 and trading_date >= $2 order by trading_date", ticker, earliest_date)
    daily_index_df = pd.DataFrame(price_rows, columns=['ticker', 'tradingDate', 'closePrice'])
    daily_index_df['closePrice'] = daily_index_df['closePrice'].astype('float64')
    daily_index_df['tradingDate'] = pd.to_datetime(daily_index_df['tradingDate'])

    if daily_index_df.size == 0:
        return daily_index_df

    first_trading_date = daily_index_df['tradingDate'].iloc[0]

    splits_rows = await db_conn.fetch("select ticker, split_date, split_from, split_to from stock_splits where ticker = $1 and split_date > $2 order by split_date", ticker, first_trading_date)
        
    dividends_rows = await db_conn.fetch("select ticker, ex_dividend_date, dividend_amount from stock_dividends where ticker = $1 and ex_dividend_date > $2 order by ex_dividend_date", ticker, first_trading_date)
    dividends_df = pd.DataFrame(dividends_rows, columns=['ticker', 'exDividendDate', 'dividendAmount'])
    dividends_df['dividendAmount'] = dividends_df['dividendAmount'].astype('float64')
    dividends_df['exDividendDate'] = pd.to_datetime(dividends_df['exDividendDate'])

    # Adjust the close prices for splits and dividends
    for split in splits_rows:
        split_date = pd.to_datetime(split[1])
        split_from = split[2]
        split_to = split[3]
        if split_from == 0 or split_to == 0:
            continue
        split_ratio = float(split_from / split_to)
        daily_index_df.loc[daily_index_df['tradingDate'] < split_date, 'closePrice'] = daily_index_df['closePrice'] * split_ratio

        dividends_df.loc[dividends_df['exDividendDate'] < split_date, 'dividendAmount'] = dividends_df['dividendAmount'] * split_ratio

    for dividend in dividends_df.itertuples():
        dividend_date = dividend.exDividendDate
        dividend_amount = dividend.dividendAmount
        closing_price_before_div = daily_index_df.loc[daily_index_df['tradingDate'] < dividend_date, 'closePrice'].iloc[-1]
        adjustment_factor = 1 - (dividend_amount / closing_price_before_div)
        daily_index_df.loc[daily_index_df['tradingDate'] < dividend_date, 'closePrice'] *= adjustment_factor

    return daily_index_df