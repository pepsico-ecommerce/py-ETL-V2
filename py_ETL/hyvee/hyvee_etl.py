import pandas as pd
import os

import datetime


class LoadHyVee:
    """ this class is strictly focused on loading HYVEE data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        df = pd.read_excel(self.F_PATH, dtype=str)
        return df

    def pre_process_data(self, df):
        # Delete extraneous cols
        del df['Manuf Code']
        return df

    def convert_datenum_to_date(self, df, col):
        # 44003 = 2020-06-21
        print(df.head())
        week_as_num = df[col].min()
        print(week_as_num)
        serial = float(week_as_num + '.0')
        seconds = (serial - 25569) * 86400.0
        week_as_date = datetime.datetime.utcfromtimestamp(seconds).date()
        print(week_as_date)
        df[col] = df[col].replace(week_as_num, week_as_date)
        print(df.head())
        return df

    def process_file(self, output_file_name):
        df = self.read_file()
        df = self.pre_process_data(df)
        convert_datenum_to_date_ind = True # 7/13=F, 7/20=T
        if (convert_datenum_to_date_ind):
            df = self.convert_datenum_to_date(df, 'Week')
        df.to_csv(path_or_buf=output_file_name, index=0)
        df.columns = df.columns.str.lower()
        col = 'Dollars'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()
        col = 'Week'.lower()
        df[col] = pd.to_datetime(df[col])
        # Add Wk Ending date to date val/log str
        date_val = df[col].min()
        we_date_val = date_val - datetime.timedelta(days=1)
        date_str = date_val.strftime("%Y-%m-%d")
        we_date_str = we_date_val.strftime("%Y-%m-%d")
        self.DATE_VALUE = f'{date_str} (WkEnd {we_date_str})'
