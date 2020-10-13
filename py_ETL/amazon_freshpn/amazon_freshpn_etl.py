import pandas as pd
import os


class LoadAmazonFreshPN:
    """ this class is strictly focused on loading AMAZON FRESH PN data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        df = pd.read_excel(self.F_PATH, dtype=str, sheet_name='Sheet1')
        return df

    def pre_process_data(self, df):
        # Categorize dependent on program
        df['program'] = df['program'].str.replace('PRIME_NOW', 'PrimeNow', regex=True)
        df['program'] = df['program'].str.replace('ULTRA_FAST', 'Fresh', regex=True)
        df['program'] = df['program'].str.replace('FRESH', 'Fresh', regex=True)
        df['program'] = df['program'].str.replace('PHYSICAL_STORE', 'Fresh', regex=True)
        df['upc'] = None
        df['temp_zone'] = None
        # Work w/ date_shipped col
        col = 'date_shipped'
        df[col] = pd.to_datetime(df[col])
        df['calender_year'] = df[col].apply(lambda x: x.year)
        df['calender_month'] = df[col].apply(lambda x: x.month)
        df['calender_week'] = df[col].apply(lambda x: int(x.strftime("%U"))+1)
        return df

    def process_file(self, output_file_name, src_dir, basename):
        df = self.read_file()
        df = self.pre_process_data(df)
        # df.to_csv(path_or_buf=output_file_name, index=0)

        sample = pd.read_csv(src_dir + os.path.sep + 'sample.csv', encoding='iso-8859-1')
        final = df[sample.columns]

        # print(sample.columns)
        # print(df.columns)

        final.to_csv(path_or_buf=output_file_name, index=False)
        df.columns = df.columns.str.lower()
        col = 'shipped_ops'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()

        # Get week no from file name
        #print('basename: ' + basename)
        nameelems = basename.split()
        weekno = ''.join(nameelems[:2])
        self.DATE_VALUE = weekno
