import os
from py_ETL.meijer.meijer_curbside_etl import LoadMeijerCurb
from config import conn_string_prod, meijer_fp

DIR = meijer_fp


def main(directory=DIR):
    cont = 'n'
    while not cont == 'y':
        available = os.listdir(directory)
        load = [f for f in available if f.endswith('.xlsx')]
        print('available files: ', load)
        cont = input('continue loading? y/n ')
        break
    if cont == 'y':
        flat_file = load[0]
        file_path = os.path.join(directory, flat_file)
        m = LoadMeijerCurb(file_path)
        sales_df = m.process_meijer_curb()
        print('Total dollars: ',
              sales_df['CurbsideSales'].astype(float).sum().round(2))
    sql = 'n'
    while not sql == 'y' and cont == 'y':
        sql = input('would you like to push to SQL? ')
        break
    if sql == 'y':
        m.push_to_sql(sales_df, conn_string_prod)
        os.rename(file_path, os.path.join(directory, 'Historical', flat_file))
        print('Done!')
