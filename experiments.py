import pandas as pd
import numpy as np
import os

dataframe_complete = None
PATH = "datasets/gdp"
dir_list = os.listdir(PATH)
for filename in dir_list:
    if filename.endswith("zip"):
        continue
    temp_df = pd.read_csv(PATH + "/" + filename)
    col_name = temp_df["Country"].iloc[0] + "GDP"
    date = temp_df["Ticker"].iloc[3:].to_list()
    GDP_value = temp_df["Series Type"].iloc[3:].to_list()
    dataframe = pd.DataFrame({"datetime": date, col_name: GDP_value})
    dataframe["datetime"] = pd.to_datetime(dataframe["datetime"])
    dataframe = dataframe.set_index("datetime")

    if dataframe_complete is None:
        dataframe_complete = dataframe
    else:
        tol = pd.Timedelta("25 days")
        dataframe_complete = pd.merge_asof(
            left=dataframe,
            right=dataframe_complete,
            left_index=True,
            right_index=True,
            direction="nearest",
            tolerance=tol,
        )
        print(dataframe_complete)
print(dataframe_complete)
