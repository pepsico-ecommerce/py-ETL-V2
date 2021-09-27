import pandas as pd
import os


class LoadAmazonBM:
    """ this class is strictly focused on loading AMAZON BRICK AND MORTAR data
    """

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self):
        # df = pd.read_excel(self.F_PATH, dtype=str, sheet_name='Sheet1')
        df = pd.read_excel(self.F_PATH, dtype=str)
        return df

    def pre_process_data(self, df):
        # Add/delete cols
        df['program'] = 'B&M'
        df['fc'] = None
        df['ASIN.1'] = df['asin']
        del df['asp']

        # Rename cols
        df = df.rename(columns={'asin': 'ASIN'})
        #df = df.rename(columns={'ASIN.1': 'ASIN'})
        df = df.rename(columns={'date': 'date_shipped'})
        df = df.rename(columns={'units': 'shipped_units'})
        df = df.rename(columns={'ops': 'shipped_ops'})
        df = df.rename(columns={'pcogs': 'shipped_cogs'})
        df = df.rename(columns={'brand_name': 'merchant_brand_name'})

        print(df.head())
        
        return df

    def process_file(self, output_file_name, src_dir, basename):
        df = self.read_file()
        df = self.pre_process_data(df)

        #df.to_csv(path_or_buf=output_file_name, index=0)
        #exit(1)

        sample = pd.read_csv(src_dir + os.path.sep + 'sample.csv', encoding='iso-8859-1')
        final = df[sample.columns]

        # print(sample.columns)
        # print(df.columns)

        #final.to_csv(path_or_buf=output_file_name, index=False)
        #df.to_excel(output_file_name)
        final.to_excel(output_file_name, index=False)
        exit(1)
        df.columns = df.columns.str.lower()
        col = 'shipped_ops'.lower()
        df[col] = pd.to_numeric(df[col], downcast="float")
        self.SALES_TOT = df[col].sum()

        # Get week no from file name
        print('basename: ' + basename)
        nameelems = basename.split()
        weekno = ''.join(nameelems[:2])
        self.DATE_VALUE = weekno


if __name__ == "__main__":
    input_file_path = r'C:\CompaniesSourceFilesReceived\AmazonBM\AmazonBM.xlsx'
    output_file_name = r'C:\CompaniesSourceFilesReceived\AmazonBM\AmazonBM_OUT.xlsx'
    input_dir = r'C:\CompaniesSourceFilesReceived\AmazonBM'
    e = LoadAmazonBM(input_file_path)
    e.process_file(output_file_name=output_file_name, src_dir=input_dir, basename=input_file_path)

    #e.process_file('test.csv')
