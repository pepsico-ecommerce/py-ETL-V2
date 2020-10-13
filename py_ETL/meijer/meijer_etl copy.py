import pandas as pd
import os


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
        #col_names = list(df)
        #print(col_names)
        df.columns = df.columns.str.rstrip()
        #col_names = list(df)
        #print(col_names)
        #exit(1)
        repl_col = 'Sales $'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: 'Sales'})
        # Set Product_Name col
        repl_col_newval = 'Product_Name'
        repl_col = 'Product'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: repl_col_newval})
        repl_col = 'Unnamed: 3'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: repl_col_newval})
        repl_col = 'Unnamed: 2'
        if repl_col in df.columns:
            df = df.rename(columns={repl_col: repl_col_newval})

        # Drop time component off date
        col = 'Week End Date'
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.date

        # Add missing cols
        col_names = list(df)
        print(col_names)
        suspect_cols = ['L5-Business Segment', 'L4-Category', 'L3-Sub Category']
        for col in suspect_cols:
            exists = col in col_names
            if exists is False:
                print('Missing col: ', col)
                df[col] = ''
        col_names = list(df)
        print(col_names)

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
