import pandas as pd

# Load the CSV files into DataFrames
df1 = pd.read_csv('../csvs/raw/1inchFlowBNBUSDT.csv')
df2 = pd.read_csv('../csvs/raw/01JDFAWT4XT8KPAE02P93J72RP.csv')

# Print the lengths of the DataFrames
print(f"Length of df1: {len(df1)}")
print(f"Length of df2: {len(df2)}")