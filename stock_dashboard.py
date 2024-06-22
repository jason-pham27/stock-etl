import streamlit as st
import pandas as pd
import sqlite3
import numpy as np

def merge_and_transform_table():
    """
    Merge the stock_data and exchange_rate table based on timestamps 
    to get the corresponding exchange rate for stock_data table.

    Returns:
        A dataframe contains 3 columns:
            - Timestamp: the time when the data was created.
            - Ticker: name of the ticker (e.g.AAPL, MSFT, etc.).
            - Price (VND): the price of the ticker at the corresponding timestamp.
    """
    conn = sqlite3.connect('stockdata.db')

    stock_data_df = pd.read_sql_query("SELECT * FROM stock_data", conn)
    exchange_rate_df = pd.read_sql_query("SELECT * FROM exchange_rate", conn)

    # Convert timestamp columns to datetime objects
    stock_data_df['timestamp'] = pd.to_datetime(stock_data_df['timestamp'])
    stock_data_df['epoch_time'] = stock_data_df['timestamp'].apply(lambda x: int(x.timestamp()))

    # Create a new column of epoch time for both dataframes, this column will be used to merge these 2 df together
    exchange_rate_df['timestamp'] = pd.to_datetime(exchange_rate_df['timestamp'])
    exchange_rate_df['epoch_time'] = exchange_rate_df['timestamp'].apply(lambda x: int(x.timestamp()))

    # Merge 2 tables by matching the nearest epoch times, avoid backward/forward fill which can lead to null values in the 'usd_to_vnd_rate' column
    merged_df = pd.merge_asof(stock_data_df, exchange_rate_df, on='epoch_time', direction="nearest", suffixes=('_stock', '_exchangerate'))

    # Create a column of price in VND using the exchange rate
    merged_df['price_vnd'] = merged_df['price_usd'] * merged_df['usd_to_vnd_rate']

    # Select and rename the relevant columns before returning the final dataframe
    final_df = merged_df[['timestamp_stock', 'stock_name', 'price_vnd']]
    final_df['price_vnd'] = final_df['price_vnd'].round()
    final_df.columns = ['Timestamp', 'Ticker', 'Price (VND)']

    conn.close()

    return final_df


def display_filtered_table(stock_df):
    """Filter the dataframe according to the ticker displaying in the select box on Streamlit."""
    if st.session_state.stock_selectbox == 'All':
        return stock_df
    else:
        filtered_stock_df = stock_df[stock_df['Ticker']==st.session_state.stock_selectbox].reset_index(drop=True)
        return filtered_stock_df
    
    
def avg_stock_price_by_day(stock_df, datetime_col, stock_col, price_col):
    """Return a dataframe that display daily average price of each ticker."""

    # Convert datetime column of the stock dataframe to date
    stock_df['Date'] = stock_df[datetime_col].dt.date

    # Group by the Date and Ticker name column, then calculate average price of each group 
    grouped_df = stock_df.groupby(['Date', stock_col])[price_col].mean().reset_index()
    grouped_df.columns = ['Date', 'Ticker', 'Average Price']
    return grouped_df


def filter_ticker(df, ticker_list: list):
    """Return a dataframe that filtered by chosen tickers."""
    filtered_df = df[df['Ticker'].isin(ticker_list)]
    return filtered_df


def max_price_ticker(stock_df, ticker: str):
    "Return the maximum value of a stock price."  
    # Filter dataframe by a ticker, then find the maximum value of the 'Price' column
    ticker_filtered_df = stock_df[stock_df['Ticker'] == ticker]
    max_price = round(ticker_filtered_df['Price (VND)'].max())
    formatted_max_price = format(max_price, ',')
    return formatted_max_price


def min_price_ticker(stock_df, ticker):
    """Return the minimum value of a stock price."""
    # Filter dataframe by a ticker, then find the minimum value of the 'Price' column
    ticker_filtered_df = stock_df[stock_df['Ticker'] == ticker]
    min_price = round(ticker_filtered_df['Price (VND)'].min())
    formatted_min_price = format(min_price, ',')
    return formatted_min_price


def current_price_ticker(stock_df, ticker):
    """Return the latest value of a stock price."""
    # Filter dataframe by a ticker, then find the latest timestamp value
    ticker_filtered_df = stock_df[stock_df['Ticker'] == ticker]
    latest_timestamp = ticker_filtered_df['Timestamp'].max()

    # Retrieve the 'Price' value of the row with the latest timestamp
    current_price_series = ticker_filtered_df[ticker_filtered_df['Timestamp'] == latest_timestamp]['Price (VND)']

    # Using `.values[0]` to access the value inside the pandas Series
    current_price = round(current_price_series.values[0])
    formatted_current_price = format(current_price, ',')
    return formatted_current_price


# Create a full dataframe of stock data, and a dataframe for daily average stock price
stock_df = merge_and_transform_table()
avg_price_by_day_df = avg_stock_price_by_day(stock_df, 'Timestamp', 'Ticker', 'Price (VND)')

st.title("Hello, Jason! Keep pushing forward :muscle::star2:")
st.subheader('This is a dashboard for :green[stock prices] visualization :chart_with_upwards_trend::chart_with_downwards_trend:')
st.divider()
st.sidebar.title("Dashboard Settings :wrench:")

# Display the full stock price dataframe if user clicks the checkbox
if st.sidebar.checkbox('Display full stock price table'):
    st.sidebar.selectbox(
        label='Choose the ticker(s) you want to display',
        # The options for select box will be every unique values in the column ticker, with an 'All' option
        options=np.append(stock_df['Ticker'].unique(), "All"),
        index=0,
        key='stock_selectbox',
        placeholder='Select a ticker...')
        
    # Create a time slider with the range from the earliest to the latest timestamp in the stock dataframe
    min_date = stock_df['Timestamp'].min().to_pydatetime()
    max_date = stock_df['Timestamp'].max().to_pydatetime()
    period = st.sidebar.slider(
        "Select date range:",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="DD/MM/YYYY",
        key='all_data_timeslider')
    
    # Display the dataframe based on the options chosen in the select box
    # The displayed dataframe is also filtered based on the time slider
    stockname_filtered_df = display_filtered_table(stock_df)
    stockname_and_time_filtered_df = stockname_filtered_df[(stockname_filtered_df['Timestamp'] >= period[0]) & (stockname_filtered_df['Timestamp'] <= period[1])]

    # Write the table name and dataframe to Streamlit frontend
    st.subheader('Stock price data')
    st.dataframe(stockname_and_time_filtered_df, width=500, height=280)
    st.divider()

st.sidebar.divider()

st.header('Dashboard :bar_chart:')

# Create a radio button widget consiting of every tickers in the stock dataframe
st.sidebar.radio(
    label = 'Choose a ticker for overview',
    options = stock_df['Ticker'].unique(),
    key = 'ticker_radio')

# Display ticker name with min, max, and latest value of stock price
st.subheader(st.session_state.ticker_radio)

# Create a time slider for displaying overall data (min, max, current price)
min_date = stock_df['Timestamp'].min().to_pydatetime()
max_date = stock_df['Timestamp'].max().to_pydatetime()
overall_data_period = st.sidebar.slider(
    "Select date range:",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="DD/MM/YYYY",
    key='overall_data_timeslider')

# Create a dataframe filtered based on the overall data time slider
time_filtered_stock_df = stock_df[(stock_df['Timestamp'] >= overall_data_period[0]) & (stock_df['Timestamp'] <= overall_data_period[1])]

# Display min, max, and latest value of stock price in 3 columns
col1, col2, col3 = st.columns(3, gap='medium')
col1.container(height=100).metric(
    label = ':chart_with_downwards_trend: Min Price (VND)',
    value = min_price_ticker(time_filtered_stock_df, st.session_state.ticker_radio))
col2.container(height=100).metric(
    label = ':money_with_wings: Current Price (VND)',
    value = current_price_ticker(time_filtered_stock_df, st.session_state.ticker_radio))
col3.container(height=100).metric(
    label = ':chart_with_upwards_trend: Max Price (VND)',
    value = max_price_ticker(time_filtered_stock_df, st.session_state.ticker_radio))

st.divider()

st.sidebar.divider()

# Create a multiselect bar of tickers for customizing the line chart of 'Average Daily Stock Price'
st.sidebar.multiselect(
    label = 'Choosing ticker(s) for line chart',
    options = stock_df['Ticker'].unique(),
    key = 'ticker_multiselect')

# Create 2 columns with different ratio, with this the Chart Name can be displayed in the middle
col4, col5 = st.columns([0.3,0.7])
col5.subheader('Average Daily Stock Price')
ticker_filtered_df = filter_ticker(avg_price_by_day_df, st.session_state.ticker_multiselect)
st.line_chart(
    data = ticker_filtered_df,
    x = 'Date',
    y = 'Average Price',
    color = 'Ticker')
