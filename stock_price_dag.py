from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta, datetime
import snowflake.connector
import requests



def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(symbol):
    api_vantage_key = Variable.get("api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_vantage_key}'

    r = requests.get(url)
    data = r.json()
    results = []


    for d in data["Time Series (Daily)"]:
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        results.append(stock_info)

    return results[0:90]


@task
def transform(data_list):
  clean_data = []

  for data in data_list:
    transformed_data = {
      "open": float(data["1. open"]),
      "high": float(data["2. high"]),
      "low": float(data["3. low"]),
      "close": float(data["4. close"]),
      "volume": int(data["5. volume"]),
      "date": data["date"]
    }
    clean_data.append(transformed_data)

  return clean_data

@task
def load(symbol, clean_data, target_table):
    con = return_snowflake_conn()

    try:
        con.execute("BEGIN")

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
            symbol varchar,
            date DATE,
            open NUMBER,
            close NUMBER,
            high NUMBER,
            low NUMBER,
            volume NUMBER,
            PRIMARY KEY (symbol, date)
            )
        """)

        con.execute(f""" DELETE FROM {target_table} """)

        insert_sql = f"""
            INSERT INTO {target_table} (symbol, date, open, close, high, low, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s) """

        records = [(symbol, data["date"], data["open"], data["close"], data["high"], data["low"], data["volume"]) for data in clean_data]
        con.executemany(insert_sql, records)

        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        raise e


with DAG(
    dag_id = 'stock_price_dag',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule_interval = '*/5 * * * *'
) as dag:
    target_table = "dev.raw.stock_price_table"
    symbol = Variable.get("symbol")
    
    data = extract(symbol)
    lines = transform(data)
    load(symbol, lines, target_table)