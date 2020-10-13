import pandas as pd
import os
import re


class LoadSams:
    """ this class is strictly focused on loading SAMS (DotCom) and SAMS PICKUP data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self, sheet_name):
        df = pd.read_excel(self.F_PATH, sheet_name=sheet_name, skiprows=1, dtype=str)
        return df

    def pre_process_data(self, df):
        return df

    def process_file(self, output_file_name, sheet_name, customer_name, src_dir):
        df = self.read_file(sheet_name)

        df = self.pre_process_data(df)
        # df.to_csv(path_or_buf=output_file_name, index=False)
        # df.columns = df.columns.str.lower()
        if (customer_name == 'sams'):
            df.columns = df.columns.str.lower()
            col = 'Curr. Per.\nEcomm\nSales $'.lower()
            if col not in df.columns:
                col = 'Curr. Per.\nEcomm Shipped\nSales $'.lower()
                if col not in df.columns:
                    col = 'Curr. Per.\nEcomm Completed\nSales $'.lower()
            df.to_csv(path_or_buf=output_file_name, index=False)
        else:
            col = 'Curr. Per.\nClub Pickup\nMember Picked\nSales $'.lower()
            print(df.head())
            sample = pd.read_csv(src_dir + os.path.sep + 'sample.csv', encoding='iso-8859-1')
            final = df[sample.columns]
            print(final.head())
            final.to_csv(path_or_buf=output_file_name, index=False)
            df.columns = df.columns.str.lower()
            
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()
        # Get week no
        self.DATE_VALUE = df['WM Year Week'.lower()].max()
