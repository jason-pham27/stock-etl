# ETL Stock Price and Dashboard

## Project Overview
This project is an ETL pipeline that fetches stock data (_hourly_) and exchange rate data (_daily_), and stores them in a sqlite database. After that, data are processed and can be displayed on a dashboard using [Streamlit](https://streamlit.io/).

Data source:
- [stockdata.org](https://www.stockdata.org/documentation): contains stock data
- [openexchangerate.org](https://docs.openexchangerates.org/reference/api-introduction): contains exchange rate data

## Versions
- _Python: 3.12.1_
- _Prefect: 2.19.4_
- _Streamlit: 1.35.0_
- _Requests: 2.32.3_

## Installation
1. Clone the repository
```
git clone https://github.com/jason-pham27/stock-etl.git
```
2. Navigate to the project directory
3. Create a virtual environment
4. Activate the virtual environment
5. Install the required packages

## Setting Up API Keys
For this project, API keys for _[stockdata.org](https://www.stockdata.org/documentation)_ and _[openexchangerate.org](https://docs.openexchangerates.org/reference/api-introduction)_ are 
stored using _[Blocks](https://docs.prefect.io/latest/concepts/blocks/)_ of _[Prefect Cloud](https://www.prefect.io/cloud)_. Create your own API keys and stores them in Prefect with names as follow:
- `oer-appid`: your _openexchangrate.org_ app ID.
- `stockdata-api-token`: your _stockdata.org_ API token.

## Usage
- Run the ETL script to fetch and store data
```
python stock_etl.py
```
> [!NOTE]
> Whenever this ETL script runs, it will be in a long-running process. Therefore, for remotely triggered or scheduled runs to be executed, this script must be actively running.

- Start the Streamlit app to display the stock dashboard
```
streamlit run stock_dashboard.py
``` 
