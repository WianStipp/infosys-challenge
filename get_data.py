import boto3
import shutil
import os
import pandas as pd


class ImportData:
    def __init__(
        self,
        AWS_ACCESS_KEY_ID="AKIA3M3UMVFNW3OVFPFS",
        AWS_SECRET_ACCESS_KEY="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",
        gdp_data=False,
        fx_reserves_data=False,
        fx_price_data=False,
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
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            shutil.unpack_archive(PATH, extract_dir="datasets/gdp/")

        if fx_reserves_data is True:
            PATH = "datasets/fxreserves/fxreserves.zip"
            if "fxreserves" not in os.listdir("datasets"):
                PATH_DIR = "datasets/fxreserves"
                os.mkdir(PATH_DIR)
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)
            shutil.unpack_archive(PATH, extract_dir="datasets/fxreserves/")

        if fx_price_data is True:
            PATH = "datasets/primary/Foreign_Exchange_Rates.csv"
            if "primary" not in os.listdir("datasets"):
                PATH_DIR = "datasets/primary"
                os.mkdir(PATH_DIR)
            self.s3.download_file(Bucket=BUCKET_NAME, Key=PATH, Filename=PATH)

    def create_dataframe(self):
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
