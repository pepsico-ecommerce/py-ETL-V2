from io import BytesIO
import pandas as pd
import os
import sys
import pytest
from py_ETL.meijer import LoadMeijerCurb

# this is a dummy conn_string
CONN_STRING = r'mssql+pyodbc://user:pw@PEPWDS01340\SQLITGEC1T:60109/TDWH?\
    driver=ODBC+Driver+13+for+SQL+Server'
F_PATH = os.path.join('py_ETL', 'meijer',
                      'data_samples', 'meijer_curb_sample.xlsx')


def test_read_file():
    meijer = LoadMeijerCurb(F_PATH)
    df = meijer.read_file(F_PATH)
    assert df.equals(pd.read_excel(F_PATH, header=1, dtype=str))


def test_process_meijer_curb():
    meijer = LoadMeijerCurb(F_PATH)
    df = meijer.process_meijer_curb()
    test_cols = ['UPC',
                 'ProductDescription',
                 'WeekEndDate',
                 'CurbsideSales',
                 'CurbsideQty']
    assert df.columns.to_list() == test_cols


def test_assert_low_sales_assert():
    df = pd.DataFrame({'UPC': [10000000],
                      'ProductDescription': 'PEP',
                       'WeekEndDate': '7/20/2019',
                       'CurbsideSales': [1000],
                       'CurbsideQty': [50]})
    meijer = LoadMeijerCurb(F_PATH)
    with pytest.raises(AssertionError) as aerror:
        meijer.assert_low_sales(df)
    assert AssertionError == aerror.type
