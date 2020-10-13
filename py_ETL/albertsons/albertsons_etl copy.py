import pandas as pd
import os


class LoadAlbertsons:
    """ this class is strictly focused on loading ALBERTSONS data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        df = pd.read_excel(self.F_PATH, sheet_name='Sheet1', skiprows=2, dtype=str)
        return df

    def pre_process_data(self, df):
        # Delete extraneous cols
        del df['Delivery Type']
        del df[r"'Weekly Sales'[DIVISION]"]
        try:
            del df['WEEK']
        #except AttributeError:
        except:
            pass

        # Drop time component off date
        col = 'Activity Date'
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.date

        return df

    def process_file(self, output_file_name):
        df = self.read_file()
        df = self.pre_process_data(df)
        # df.to_csv(path_or_buf=output_file_name, index=0) #date_format='%Y-%m-%d'
        df.to_csv(path_or_buf=output_file_name, index=0)
        df.columns = df.columns.str.lower()
        col = 'Net Amount'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()
        col = 'Activity Date'.lower()
        self.DATE_VALUE = df[col].min().strftime("%Y-%m-%d")
