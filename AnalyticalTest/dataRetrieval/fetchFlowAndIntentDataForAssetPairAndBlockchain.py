from AnalyticalTest.utils.analyticUtils import *
asset1="BNB"
asset2="USDT"
blockchain="bnb"


result_df= getDataFrameFor1InchFlow(blockchain, asset1, asset2)
csv_file_path = os.path.join('../csvs', '1inchFlow' + asset1 + asset2 + '.csv')
result_df.to_csv(csv_file_path, index=False)


result_df= getDataFrameFor1InchIntentFlow(blockchain, asset1, asset2)
csv_file_path = os.path.join('../csvs', '1inchIntentFlow' + asset1 + asset2 + '.csv')
result_df.to_csv(csv_file_path, index=False)