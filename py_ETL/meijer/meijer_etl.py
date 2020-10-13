import pandas as pd
import os
import re


class LoadMeijer:

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    # def read_file(self, sheet_name):
    #     df = pd.read_excel(self.F_PATH, sheet_name=sheet_name, skiprows=5, dtype=str)
    #     return df

    def read_file(self):
        df = pd.read_excel(self.F_PATH, skiprows=5, dtype=str)
        return df

    def pre_process_data(self, df):
        df.columns = df.columns.str.rstrip()
        repl_col = 'Sales $'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: 'Sales'})
        # Set Product_Name col
        # repl_col_newval = 'Product_Name'
        # repl_col = 'Product'
        # if repl_col in df.columns:
        #     df = df.rename(columns={repl_col: repl_col_newval})
        repl_col = 'Unnamed: 3'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: repl_col_newval})
        repl_col = 'Unnamed: 2'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: repl_col_newval})

        # Drop time component off date
        # col_names = list(df)
        # print(col_names)
        # exit(1)
        col = 'Week End Date'
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.date

        # Process suspect cols
        col_names = list(df)
        print(col_names)

        suspect_cols = {
            'L5-Business Segment': 'L5.+Business.+Segment',
            'L4-Category': 'L4.+Category',
            'L3-Sub Category': 'L3.+Sub.+Category',
            'Product_Name': '^Product.*'
        }
        print(suspect_cols)
        keys = suspect_cols.keys()
        # print(keys)
        for key in keys:
            # print(key)
            patt = suspect_cols[key]
            print(key, ':', patt)
            match_found = False
            for col in col_names:
                if (re.search(patt, col)):
                    match_found = True
                    break
            if (match_found):
                print('Match found: ', col)
                if (col != key):
                    df.rename(columns={col: key}, inplace=True)
            else:
                print('Missing col: ', key)
                df[key] = ''

        col_names = list(df)
        print(col_names)

        # exit(1)

        # print(df.columns)
        return df

    def process_file(self, output_file_name, src_dir):
        # df = self.read_file(sheet_name='SHEET99') # Todo : Automate sheet name detection/change
        df = self.read_file()
        df = self.pre_process_data(df)

        sample = pd.read_csv(src_dir + os.path.sep + 'sample.csv', encoding='iso-8859-1')
        final = df[sample.columns]
        #exit(1)
        final.to_csv(path_or_buf=output_file_name, index=False)

        #df.to_csv(path_or_buf=output_file_name, index=0)
        df.columns = df.columns.str.lower()
        col = 'Sales'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()
        col = 'Week End Date'.lower()
        self.DATE_VALUE = df[col].max().strftime("%Y-%m-%d")
