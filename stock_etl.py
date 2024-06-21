import requests
from datetime import datetime
import sqlite3
from prefect import flow, task, serve
from prefect.blocks.system import Secret

# Load api tokens
oer_appid_block = Secret.load("oer-appid")
stock_api_token_block = Secret.load("stockdata-api-token")

oer_appid = oer_appid_block.get()
stock_api_token = stock_api_token_block.get()


@task(retries=3, retry_delay_seconds=120)
def get_exchange_rate_data(app_id: str, currency: str, timestamp: str) -> tuple:
    '''Take in the app id of your openexchangerates.org account and the currency to be converted,
    then return a tuple including the current datetime, and the exchange rate.'''

    # create an api
    oer_api_endpoint = "latest.json"
    oer_base_api_path = "https://openexchangerates.org/api/"
    oer_api = f'{oer_base_api_path}{oer_api_endpoint}?app_id={app_id}'

    # attempting to get the exchange rate from the newly created api
    print('Requesting on openexchangerates.org')
    oer_response = requests.get(oer_api)
    oer_json = oer_response.json()
    exchange_rate = oer_json['rates'][currency]
    exchange_rate_data = (timestamp, exchange_rate)
    print("Exchange rate data retrieved successfully")
    return exchange_rate_data


@task(retries=3, retry_delay_seconds=120)
def get_stockdata(api_token: str, tickers_list: list, data_fields: list, timestamp: str) -> list:
    '''Take in the token id of your stockdata.org account, the list of tickers, and what 
    fields you want, return the list of the fields you input, plus the current datetime.'''

    # create a complete api for stockdata.org
    stockdata_base_api_path = "https://api.stockdata.org/v1/data/quote"
    ticker_str = ",".join(tickers_list)
    stockdata_api = f'{stockdata_base_api_path}?symbols={ticker_str}&api_token={api_token}'

    # attempt to get the json of stockdata
    print("Requesting on stockdata.org")
    stock_response = requests.get(stockdata_api)
    stock_json = stock_response.json()
    stock_data_dicts_list = stock_json["data"]

    # access the dictionary of each ticker then append data of each ticker to append to the final list
    stock_data = [
        tuple(stock_data_dict[field] for field in data_fields) + (timestamp,)
        for stock_data_dict in stock_data_dicts_list
    ]
    print("Stock data retrieved successfully")
    return stock_data


@task
def update_exchange_rate_table(exchange_rate_data: tuple):
    '''Create a table to contain openexchangerate.org data,then insert into the table 
    the data we just get from the function get_exchange_rate_data().'''

    print("Connecting to jason_etl_project.db")
    conn = sqlite3.connect("stockdata.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_rate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            usd_to_vnd_rate REAL
        )''')
    
    cursor.execute('''
        INSERT INTO exchange_rate (timestamp, usd_to_vnd_rate)
        VALUES (?, ?)''', exchange_rate_data)

    print("Inserting new values to exchange_rate table...")
    conn.commit()
    print("Changes to exchange_rate table committed")
    cursor.close()
    conn.close()


@task
def update_stockdata_table(stock_data: list):
    '''Create a table to contain stockdata.org data, then insert into the table 
    the data we just get from the function get_stockdata().'''

    print("Connecting to jason_etl_project.db")
    conn = sqlite3.connect("stockdata.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_name TEXT,
            price_usd REAL,
            day_high_usd REAL,
            day_low_usd REAL,
            timestamp TEXT
        )''')
    
    cursor.executemany('''
        INSERT INTO stock_data (stock_name, price_usd, day_high_usd, day_low_usd, timestamp)
        VALUES (?, ?, ?, ?, ?)''', stock_data)
    
    print("Inserting new values to stock_data table...")
    conn.commit()
    print("Changes to stock_data table committed")
    cursor.close()
    conn.close()


@flow(log_prints=True)
def exchange_rate_etl():
    """Extracting exchange rate data from openexchangerate.org and inserting the newly extracted data to the table 'exchange_rate'"""
    current_timestamp = datetime.now().isoformat()
    oer_data = get_exchange_rate_data(oer_appid, "VND", current_timestamp)
    update_exchange_rate_table(oer_data)
    

@flow(log_prints=True)
def stock_data_etl():
    """Extracting exchange rate data from stockdata.org and inserting the newly extracted data to the table 'stock_data'"""
    current_timestamp = datetime.now().isoformat()
    tickers = ["AAPL", "TSLA", "MSFT"]
    fields_for_stockdata = ["ticker", "price", "day_high", "day_low"]
    stock_data = get_stockdata(stock_api_token, tickers, fields_for_stockdata, current_timestamp)
    update_stockdata_table(stock_data)


if __name__ == '__main__':
    exchange_rate_etl_deploy = exchange_rate_etl.to_deployment(name="exchange_rate_etl", cron="0 5 * * *")
    stock_data_etl_deploy = stock_data_etl.to_deployment(name="stock_data_etl", cron="0 * * * *")
    serve(exchange_rate_etl_deploy, stock_data_etl_deploy, pause_on_shutdown=False)