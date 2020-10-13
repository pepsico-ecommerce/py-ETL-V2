import pandas as pd
import os
from datetime import datetime
from py_ETL.kroger import kroger_sql as ksql


class LoadKrogerShip:
    """ this class is strictly focused on loading KROGER SHIP data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        df = pd.read_excel(self.F_PATH, sheet_name='WTD-', skiprows=3, skipfooter=1, dtype=str)
        return df

    def get_wkend_date(self):
        df = pd.read_excel(self.F_PATH, sheet_name='WTD-', dtype=str, nrows=1)
        # print(df)
        # print(df.columns[0])
        first_row = df.columns[0]
        wkend_date = first_row[-10:]
        # print(wkend_date)
        date_time_obj = datetime.strptime(wkend_date, '%m/%d/%Y')
        # print(date_time_obj)
        date_str = date_time_obj.strftime("%Y-%m-%d")
        # print(date_str)
        return date_str

    def pre_process_data(self, df, kroger_desc):
        # Delete extraneous cols
        del df['Unnamed: 13']
        del df['Unnamed: 14']
        # Insert initial col w/ date
        df.insert(loc=0, column='KROGER DESC', value=kroger_desc)
        return df

    def process_file(self, output_file_name):
        df = self.read_file()
        week_end_date = self.get_wkend_date()
        print('week_end_date:', week_end_date)

        kroger_desc, PeriodNumber, PeriodWeekNumber, row_count = ksql.get_kroger_desc_from_WkEndDate(debug=True, week_end_date=week_end_date)
        if (row_count == 0):
            raise Exception('Call to get_kroger_desc_from_WkEndDate() produced no rows')
        # exit(1)
        df = self.pre_process_data(df, kroger_desc=kroger_desc)
        df.to_csv(path_or_buf=output_file_name, index=0)
        df.columns = df.columns.str.lower()
        col = 'Net Sales'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()
        self.DATE_VALUE = week_end_date
