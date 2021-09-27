import pandas as pd
import os


class LoadAmazonBM:
    """ this class is strictly focused on convertting AMAZON BRICK AND MORTAR data to AMAZON FRESHPN format
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

        #print(df.head())
        
        return df

    def process_file(self, output_file_name, src_dir, basename):
        df = self.read_file()
        df = self.pre_process_data(df)

        sample = pd.read_csv(src_dir + os.path.sep + 'sample.csv', encoding='iso-8859-1')
        final = df[sample.columns]

        final.to_excel(output_file_name, index=False)

if __name__ == "__main__":
    input_dir = r'C:\CompaniesSourceFilesReceived\AmazonBM'
    output_dir = input_dir
    input_file_path = os.path.join(input_dir, 'AmazonBM.xlsx')
    output_file_path = os.path.join(output_dir, 'AmazonBM_OUT.xlsx')
    print('input_file_path', input_file_path)
    print('output_file_path', output_file_path)
    e = LoadAmazonBM(input_file_path)
    e.process_file(output_file_name=output_file_path, src_dir=input_dir, basename=input_file_path)
