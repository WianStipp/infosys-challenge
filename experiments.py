import pandas as pd
import numpy as np
import os

dataframe_complete = None
PATH = "datasets/interest_rates"
dir_list = os.listdir(PATH)
for filename in dir_list:
    if filename.endswith("zip"):
        continue
    print(filename)
    temp_df = pd.read_excel(PATH + "/" + filename, header=None)
    temp_df = temp_df.iloc[:, 1:]
    dti = pd.date_range("2008-01-01", periods=24, freq="M")
    if "no data" not in temp_df.loc[:, 1].to_list() and (
        "Government Bonds" in temp_df.loc[:, 1].to_list()
        or "Treasury Bill Rate" in temp_df.loc[:, 1].to_list()
    ):
        country_name = temp_df.iloc[2, 0]
        print
        try:
            data_for_df = (
                temp_df[temp_df.loc[:, 1] == "Government Bonds"].iloc[:, 4:].values[0]
            )
        except:
            print(
                "Warning: We are using Treasury Bills instead of Government Bonds for",
                country_name,
            )
            data_for_df = (
                temp_df[temp_df.loc[:, 1] == "Treasury Bill Rate"].iloc[:, 4:].values[0]
            )
        dataframe = pd.DataFrame({country_name + "TBillRate": data_for_df}, index=dti,)

    else:
        country_name = temp_df.iloc[2, 0]
        print(
            country_name,
            "has no data. It will be included in the dataframe with nan (not a number) as the entries.",
        )
        dataframe = pd.DataFrame(
            {country_name + "TBillRate": np.empty((24,))}, index=dti,
        )

    if dataframe_complete is None:
        dataframe_complete = dataframe
    else:
        dataframe_complete = dataframe_complete.merge(
            dataframe, left_index=True, right_index=True
        )
print(dataframe_complete)
