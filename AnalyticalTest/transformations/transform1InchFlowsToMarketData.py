import pandas as pd
import os


def process_csv(file_name):
    print(f"Processing file: {file_name}")

    # Load the CSV file into a DataFrame
    df = pd.read_csv(f'csvs/{file_name}')
    print(f"Loaded {file_name} with {len(df)} rows")

    # Convert the relevant columns to numeric types with high precision
    df['src_token_amount'] = pd.to_numeric(df['src_token_amount'], errors='coerce', downcast='float')
    df['dst_token_amount'] = pd.to_numeric(df['dst_token_amount'], errors='coerce', downcast='float')
    print("Converted src_token_amount and dst_token_amount to numeric types")

    # Order by block_time in ascending order
    df = df.sort_values(by='block_time')
    print("Sorted DataFrame by block_time")

    # Calculate the price
    df['price'] = df['src_token_amount'] / df['dst_token_amount']
    print("Calculated price column")

    # Create the output directory if it doesn't exist
    output_dir = 'csvs/marketData'
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists: {output_dir}")

    # Save the processed DataFrame to a new CSV file
    output_file_name = f'market{file_name}'
    df.to_csv(os.path.join(output_dir, output_file_name), index=False)
    print(f"Saved processed DataFrame to {output_file_name}")


# List of CSV files to process
csv_files = ['1inchFlowBNBUSDT.csv', '1inchIntentFlowBNBUSDT.csv']

# Process each CSV file
for csv_file in csv_files:
    process_csv(csv_file)