from database import get_db_connection

def sql_safe_string(text):
    return '"' + (text or '').replace("'", "''").replace('"', '\\"') + '"'

def get_stocks_of_interest():
    db_conn = get_db_connection()
    db_cur = db_conn.cursor()
    db_cur.execute("select distinct ticker from stock_tickers order by ticker")
    rows = db_cur.fetchall()
    db_conn.close()
    return [row[0] for row in rows]

def get_latest_year_in_db(table_name):
    db_conn = get_db_connection()
    db_cur = db_conn.cursor()
    db_cur.execute(f"select max(year)-1 from {table_name}")
    latest_year = db_cur.fetchone()[0]
    db_cur.close()
    db_conn.close()
    if latest_year is None:
        return 2001
    return latest_year