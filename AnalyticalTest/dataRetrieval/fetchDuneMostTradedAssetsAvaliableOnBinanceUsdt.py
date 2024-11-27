import os

from AnalyticalTest.utils.analyticUtils import getAssetsWithMostTradesOn1InchFusion

result_df= getAssetsWithMostTradesOn1InchFusion()
csv_file_path = os.path.join('../csvs', 'mostTradedAssets.csv')
result_df.to_csv(csv_file_path, index=False)
