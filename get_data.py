import boto3
import shutil
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class ImportData:
    def __init__(
        self,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        gdp_data=False,
        fx_reserves_data=False,
        fx_price_data=False,
        interest_rate_data=False,
    ):

        """
            gdp_data: set this as True if you want to download the GDP data
            fx_reserves_data: set this as True if you want to download the FX Reserves data
            fx_price_data: set this as True if you want to download the FX Price data
        
        Calling the class object will connect to an Amazon AWS S3 bucket, from where it can automatically
        download the data into the relevant folders. It downloads a zip file containing all of the data, 
        and then extracts it into each folder.
        """

        BUCKET_NAME = "infosyschallenge"
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        if "datasets" not in os.listdir():
            PATH_DIR = "datasets"
            os.mkdir(PATH_DIR)

        if gdp_data is True:
            PATH = "datasets/gdp/gdp.zip"
            if "gdp" not in os.listdir("datasets"):
                PATH_DIR = "datasets/gdp"
                os.mkdir(PATH_DIR)
            print("*** DOWNLOADING GDP DATA ***")
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            print("*** UNPACKING GDP DATA ***")
            shutil.unpack_archive(PATH, extract_dir="datasets/gdp/")
            print("")

        if fx_reserves_data is True:
            PATH = "datasets/fxreserves/fxreserves.zip"
            if "fxreserves" not in os.listdir("datasets"):
                PATH_DIR = "datasets/fxreserves"
                os.mkdir(PATH_DIR)
            print("*** DOWNLOADING FX RESERVES DATA ***")
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            print("*** UNPACKING FX RESERVES DATA ***")
            shutil.unpack_archive(PATH, extract_dir="datasets/fxreserves/")
            print("")

        if fx_price_data is True:
            PATH = "datasets/primary/Foreign_Exchange_Rates.csv"
            if "primary" not in os.listdir("datasets"):
                PATH_DIR = "datasets/primary"
                os.mkdir(PATH_DIR)
            print("*** DOWNLOADING FX PRICE DATA ***")
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            print("")

        if interest_rate_data is True:
            PATH = "datasets/interest_rates/interest_rates.zip"
            if "interest_rates" not in os.listdir("datasets"):
                PATH_DIR = "datasets/interest_rates"
                os.mkdir(PATH_DIR)
            print("*** DOWNLOADING RATES DATA ***")
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            print("*** UNPACKING RATES DATA ***")
            shutil.unpack_archive(PATH, extract_dir="datasets/interest_rates/")
            print("")

    def create_price_dataframe(self):
        PATH = "datasets/primary/Foreign_Exchange_Rates.csv"
        dataframe = pd.read_csv(PATH, index_col="Unnamed: 0")
        # Set the index as the Time Series column
        dataframe.index = dataframe[dataframe.columns[0]]
        # Drop that column as it is now the index
        dataframe = dataframe.drop(dataframe.columns[0], axis=1)
        # Drop rows that have no data
        columns = dataframe.columns
        filter_ND = dataframe[columns[0]] != "ND"
        dataframe = dataframe[filter_ND]

        print("There are", len(columns), "different currency pairs in this dataframe.")
        return dataframe

    def create_gdp_dataframe(self):
        """
        This function loops through all of the dataset folders and creates a dataframe combining all of the information.
        """
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
                dataframe_complete = dataframe_complete.merge(
                    dataframe, left_index=True, right_index=True
                )
        return dataframe_complete

    def create_rates_dataframe(self, preferred_rate="Government Bonds"):
        """
        This function loops through all of the dataset folders and creates a dataframe combining all of the information.
        """
        if preferred_rate == "Government Bonds":
            alt_rate = "Treasury Bill Rate"
        elif preferred_rate == "Treasury Bill Rate":
            alt_rate = "Government Bonds"
        else:
            print(
                "Please select either: Government Bonds ... or ... Treasury Bill Rate"
            )
        dataframe_complete = None
        PATH = "datasets/interest_rates"
        dir_list = os.listdir(PATH)
        for filename in dir_list:
            if filename.endswith("zip"):
                continue
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
                        temp_df[temp_df.loc[:, 1] == preferred_rate]
                        .iloc[:, 4:]
                        .values[0]
                    )
                except:
                    print(
                        "Warning: We are using",
                        alt_rate,
                        "instead of",
                        preferred_rate,
                        "for",
                        country_name,
                    )
                    data_for_df = (
                        temp_df[temp_df.loc[:, 1] == alt_rate].iloc[:, 4:].values[0]
                    )
                dataframe = pd.DataFrame({country_name: data_for_df}, index=dti,)

            else:
                country_name = temp_df.iloc[2, 0]
                print(
                    country_name,
                    "has no data. It will be included in the dataframe with nan (not a number) as the entries.",
                )
                dataframe = pd.DataFrame({country_name: np.empty((24,))}, index=dti,)

            if dataframe_complete is None:
                dataframe_complete = dataframe
            else:
                dataframe_complete = dataframe_complete.merge(
                    dataframe, left_index=True, right_index=True
                )
        return dataframe_complete

    def create_fxreserves_dataframe(self):
        dataframe_complete = None
        PATH = "datasets/fxreserves"
        dir_list = os.listdir(PATH)
        for filename in dir_list:
            if filename.endswith("zip"):
                continue
            country = pd.read_csv(PATH + "/" + filename)["Country"].iloc[0]
            temp_df = pd.DataFrame(
                pd.read_csv(PATH + "/" + filename, skiprows=2, index_col="Date")[
                    "Close"
                ]
            )
            temp_df.index = pd.to_datetime(temp_df.index)
            temp_df = temp_df.rename(columns={"Close": country})
            if dataframe_complete is None:
                dataframe_complete = temp_df
            else:
                dataframe_complete = dataframe_complete.merge(
                    temp_df, left_index=True, right_index=True
                )
        return dataframe_complete

    def create_reservesByGDP_dataframe(self):
        df_gdp = self.create_gdp_dataframe()
        df_reserves = self.create_fxreserves_dataframe()
        # combine the reserves and gdp dataframes
        composite_df = pd.merge(
            left=df_reserves, right=df_gdp, left_index=True, right_index=True
        )
        # fix an incorrect name
        composite_df = composite_df.rename(columns={"World": "Euro Area"})
        # make a list of the 22 countries we are comparing with
        countries = composite_df.columns[:22].to_list()
        # create a blank dataframe
        resGDP_df = pd.DataFrame(columns=countries)
        # populate this dataframe with the FX Reserve-to-GDP ratio for each country
        for index in range(22):
            for column in composite_df.columns.to_list():
                if column.startswith(countries[index][:4]):
                    if column.endswith("GDP"):
                        gdp_data = composite_df[column].values
                    else:
                        fx_res_data = composite_df[column].values
            resGDP_df[countries[index]] = fx_res_data / gdp_data.astype("float32")
        # add the datetime to this dataframe
        resGDP_df.index = composite_df.index
        print("Dataframe Successfully Created. The shape is", resGDP_df.shape)
        return resGDP_df
