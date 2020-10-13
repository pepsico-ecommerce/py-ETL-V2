import pandas as pd
import sqlalchemy


class LoadMeijerCurb:

    def __init__(self, F_PATH):
        self.F_PATH = F_PATH

    def read_file(self, F_PATH):
        df = pd.read_excel(F_PATH, header=1, dtype=str)
        return df

    def push_to_sql(self, df, TDWH_prod_engine):
        TDWH_prod_engine = sqlalchemy.create_engine(TDWH_prod_engine)
        table = 'STG_MeijerSales'
        schema = 'DataStage'
        print('appending to', schema, ',', table)
        df.to_sql(name=table,
                  con=TDWH_prod_engine,
                  schema=schema,
                  index=False,
                  if_exists='append')
        return True

    def process_meijer_curb(self):
        print('please ensure that STG_MeijerSales has been truncated')
        df = self.read_file(self.F_PATH)
        # rename cols to fit existing table
        renamed_cols = ['UPC',
                        'ProductDescription',
                        'WeekEndDate',
                        'CurbsideSales',
                        'CurbsideQty']
        df.columns = renamed_cols
        df['CurbsideSales'] = (df['CurbsideSales']
                               .astype(float).round(2).astype(str))
        # test sales numbers
        df = self.assert_low_sales(df)
        return df

    def assert_low_sales(self, df):
        if df['CurbsideSales'].astype(float).sum() > 12000:
            print('WARNING, low sales')
        return df
