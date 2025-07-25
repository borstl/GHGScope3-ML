import pandas as pd

appleDf = pd.read_csv("../data/datasets/apple_esg_time_series.csv")

cleared = appleDf.dropna(subset=appleDf.columns.difference(['Date']), how='all')

cleared.to_csv("apple_esg_time_series_cleared.csv", index=False)
