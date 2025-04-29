from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import yfinance as yf
import requests
from airflow.models import Variable

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(file_path, asset_pairs):
    API_KEY = Variable.get("api_key")
    headers = {
        'X-CoinAPI-Key': API_KEY
    }

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=180)

    all_data = []

    for pair in asset_pairs:
        url = f"https://rest.coinapi.io/v1/ohlcv/{pair}/history"
        params = {
            'period_id': '1DAY',
            'time_start': start_date.strftime('%Y-%m-%dT00:00:00'),
            'time_end': end_date.strftime('%Y-%m-%dT00:00:00'),
            'limit': 2000
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            temp_df = pd.DataFrame(data)
            temp_df['asset_pair'] = pair
            all_data.append(temp_df)
        else:
            print(f"Failed to fetch data for {pair}. Status code: {response.status_code}")
            print(response.text)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['time_period_start'] = pd.to_datetime(final_df['time_period_start']).dt.strftime('%Y-%m-%d')
        final_df.rename(columns={'time_period_start': 'Date'}, inplace=True)
        final_df.to_csv(file_path, index=False,  mode='w')
        print(f"Saved data to: {file_path}")
    else:
        print("No data fetched.")

@task
def create_table():
    conn = return_snowflake_conn()
    target_table = "USER_DB_JELLYFISH.raw.crypto_data"  

    try:
        conn.execute("BEGIN;")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                time_period_start TIMESTAMP,
                time_period_end TIMESTAMP,
                time_open TIMESTAMP,
                time_close TIMESTAMP,
                price_open FLOAT,
                price_high FLOAT,
                price_low FLOAT,
                price_close FLOAT,
                volume_traded FLOAT,
                trades_count INTEGER,
                asset_pair VARCHAR
            )
        """)
        conn.execute(f"DELETE FROM {target_table}") 
        conn.execute("COMMIT;")
        print(f"Table {target_table} created and cleared successfully.")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(f"Error creating table: {e}")
        raise e
    finally:
        conn.close()


@task
def populate_table_via_stage(table, file_path):
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path) 
    conn.execute(f"CREATE TEMPORARY STAGE {stage_name}")
    conn.execute(f"PUT file://{file_path} @{stage_name}")
    copy_query = f"""
        COPY INTO {schema}.{table} (time_period_start, time_period_end, 
        time_open, time_close, price_open, price_high, price_low, price_close, volume_traded, trades_count,asset_pair )
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """
    conn.execute(copy_query)


with DAG(
    dag_id = 'lab2_dag',
    start_date = datetime(2024,6,21),
    catchup=False,
    tags=['ETL'],
    schedule_interval = '0 0 * * *'
) as dag:
    
    schema = "USER_DB_JELLYFISH.raw"
    table = "crypto_data"
    file_path = "/opt/airflow/dags/crypto.csv"
    conn = return_snowflake_conn()
    extracted_data = extract(file_path, asset_pairs=["BINANCE_SPOT_ETH_BTC","BINANCE_SPOT_BTC_USDT"])
    create_tbl = create_table()
    inserted_data = populate_table_via_stage(table, file_path)
    
    extracted_data >> create_tbl >> inserted_data
