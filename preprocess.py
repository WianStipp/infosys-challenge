import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import wget
import shutil
import boto3


class PreprocessData:
    def __init__(self):
        # wget.download(
        #    "http://infosyschallenge.s3.amazonaws.com/datasets/primary/Foreign_Exchange_Rates.csv",
        #    out="/datasets/primary/Foreign_Exchange_Rates.csv",
        # )

        wget.download(
            "http://infosyschallenge.s3.amazonaws.com/datasets/gdp/gdp.zip",
            out="/datasets/gdp/gdp.zip",
            bar=None,
        )
        shutil.unpack_archive("/datasets/gdp/gdp.zip", extract_dir="/datasets/gdp")

        # wget.download(
        #    "http://infosyschallenge.s3.amazonaws.com/datasets/fxreserves/fxreserves.rar",
        #    out="/datasets/fxreserves/fxreserves.rar",
        # )
        # shutil.unpack_archive(
        #    "/datasets/fxreserves/fxreserves.zip", extract_dir="/datasets/fxreserves"
        # )


j = PreprocessData()
