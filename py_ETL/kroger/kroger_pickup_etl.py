import pandas as pd
import os


class LoadKrogerPickup:
    """ this class is strictly focused on loading KROGER PICKUP data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        df = pd.read_excel(self.F_PATH, sheet_name='Business Review Integrated (13)', dtype=str)
        return df

    def pre_process_data(self, df):
        return df

    def process_file(self, output_file_name):
        df = self.read_file()
        df = self.pre_process_data(df)
        df.to_csv(path_or_buf=output_file_name, index=0)
        df.columns = df.columns.str.lower()
        col = 'CLICKLIST_SCANNED_RETAIL_DOLLARS_CUR'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.COL_TOT = df[col].sum()

