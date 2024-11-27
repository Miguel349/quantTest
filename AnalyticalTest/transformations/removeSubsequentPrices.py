import pandas as pd

# Load the data from the CSV file
file_path = '../csvs/raw/aggTradesBinance_BNBUSDT.csv'
df = pd.read_csv(file_path)

# Ensure the 'datetime' column is in datetime format
df['datetime'] = pd.to_datetime(df['datetime'])

# Use the shift method to compare each row with the previous one
df['prev_price'] = df['price'].shift(1)
cleaned_df = df[df['price'] != df['prev_price']].copy()

# Drop the temporary 'prev_price' column
cleaned_df.drop(columns=['prev_price'], inplace=True)

# Write the cleaned DataFrame to a new CSV file
cleaned_df.to_csv('../csvs/marketData/aggTradesBinance_BNBUSDT_clean.csv', index=False)