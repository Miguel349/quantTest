import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import time

# Base URL for Binance API
BASE_URL = 'https://api.binance.com'

def fetch_agg_trades_from_binance(symbol, start_time, end_time, output_file_name):
    """Fetch aggregated trades for a given symbol and time range in chunks."""
    endpoint = f"{BASE_URL}/api/v3/aggTrades"
    params = {
        'symbol': symbol,
        'startTime': start_time,
        'endTime': end_time,
        'limit': 1000  # Max limit per request
    }

    all_trades = []
    rows_written = 0

    while True:
        response = requests.get(endpoint, params=params)
        if response.status_code != 200:
            raise Exception(f"Error fetching trades: {response.json()}")

        data = response.json()
        if not data:
            break  # Exit if no more data

        all_trades.extend(data)

        # Write to CSV in chunks of 100,000 rows
        if len(all_trades) >= 100000:
            append_to_csv(all_trades, output_file_name)
            rows_written += len(all_trades)
            print(f"{rows_written} rows written to {output_file_name}")
            all_trades = []  # Clear buffer

        # Use `fromId` for the next batch to avoid conflict
        last_trade_id = data[-1]['a']
        params = {
            'symbol': symbol,
            'fromId': last_trade_id + 1,
            'limit': 1000  # Continue with pagination
        }

        # Stop if the last trade exceeds the end time
        if int(data[-1]['T']) >= end_time:
            break

        # Respect Binance rate limits
        time.sleep(0.1)

    # Write remaining data (if any) to CSV
    if all_trades:
        append_to_csv(all_trades, output_file_name)
        rows_written += len(all_trades)
        print(f"{rows_written} rows written to {output_file_name}")


def append_to_csv(trades, file_name):
    """Append trades to the CSV file."""
    trades_df = pd.DataFrame(trades, columns=['a', 'p', 'q', 'f', 'l', 'T', 'm'])
    trades_df.rename(columns={
        'a': 'trade_id',
        'p': 'price',
        'q': 'quantity',
        'f': 'first_trade_id',
        'l': 'last_trade_id',
        'T': 'timestamp',
        'm': 'is_buyer_maker'
    }, inplace=True)

    # Convert timestamp to datetime
    trades_df['datetime'] = pd.to_datetime(trades_df['timestamp'], unit='ms')

    # Filter out consecutive rows with the same price
    if os.path.exists(file_name):
        existing_df = pd.read_csv(file_name)
        last_price = existing_df['price'].iloc[-1]
        trades_df = trades_df[trades_df['price'] != last_price]

    trades_df['prev_price'] = trades_df['price'].shift(1)
    trades_df = trades_df[trades_df['price'] != trades_df['prev_price']].copy()
    trades_df.drop(columns=['prev_price'], inplace=True)

    # Append to CSV
    write_header = not os.path.exists(file_name)  # Write header only if file doesn't exist
    trades_df.to_csv(file_name, mode='a', index=False, header=write_header)


def fetch_binance_agg_trades(file_name):
    """Fetch aggregated trades for a given market and save them to CSV."""
    # Print the current working directory
    print(f"Current working directory: {os.getcwd()}")

    # Extract the market symbol from the file name
    market = file_name.replace('market1inchIntentFlow', '').replace('.csv', '')

    # Construct the full file path
    file_path = f'../csvs/marketData/{file_name}'

    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Convert block_time to datetime
    df['block_time'] = pd.to_datetime(df['block_time'], utc=True)

    # Determine the start and end dates
    start_date = df['block_time'].min() - timedelta(days=1)
    end_date = df['block_time'].max() + timedelta(days=1)

    # Convert datetime to milliseconds for Binance API
    start_time = int(start_date.timestamp() * 1000)
    end_time = int(end_date.timestamp() * 1000)

    # Output file name
    output_dir = '../csvs/raw'
    os.makedirs(output_dir, exist_ok=True)
    output_file_name = os.path.join(output_dir, f'priceActionBinance_{market}.csv')

    # Fetch aggregated trades and save them to CSV in chunks
    fetch_agg_trades_from_binance(
        symbol=market,
        start_time=start_time,
        end_time=end_time,
        output_file_name=output_file_name
    )
    print(f"Saved aggregated trades to {output_file_name}")


# Example usage
if __name__ == '__main__':
    try:
        fetch_binance_agg_trades('market1inchIntentFlowBNBUSDT.csv')
    except Exception as e:
        print(f"An error occurred: {e}")