import pandas as pd
import pytest
import os
from py_ETL.kroger import LoadKroger


F_PATH = os.path.join('py_ETL', 'kroger',
                      'data_samples', 'P9W4.csv')


def test_read_file():
    kroger = LoadKroger(F_PATH)
    df = kroger.read_file()
    assert df.equals(pd.read_csv(F_PATH, dtype=str))


def test_kroger_to_pep_pd():
    kroger = LoadKroger(F_PATH)
    pep_pd, pep_wk = kroger.kroger_to_pep_pd()
    assert pep_pd == 9
    assert pep_wk == 4


def test_pre_process_data():
    kroger = LoadKroger(F_PATH)
    df = kroger.read_file()
    test_df = kroger.pre_process_data(df)
    assert test_df.columns.to_list() == ['UPC', 'Dollars',
                                         'Units', 'W_Date',
                                         'PickupOrDelivery',
                                         'Retail_Dollars',
                                         'Retail_Units',
                                         'LineageID',
                                         'IsProcessed',
                                         'IsDuplicate']
    assert test_df.loc[0, 'PickupOrDelivery'] == 'Pickup'


def test_join_activity_day():
    activity_days = pd.DataFrame({'WeekDateValue':
                                 ['2019-08-17',
                                  '2019-08-24',
                                  '2019-08-31',
                                  '2019-09-07'],
                                  'PeriodNumber': [9, 9, 9, 9],
                                  'PeriodWeekNumber': [1, 2, 3, 4]},
                                 dtype=str)
    kroger = LoadKroger(F_PATH)
    df = kroger.read_file()
    test_df = kroger.pre_process_data(df)
    period, week = kroger.kroger_to_pep_pd()
    test_df['period'] = period
    test_df['week'] = week
    activity_df = kroger.join_activity_day(test_df, activity_days)
    assert activity_df['Activity_Day'].unique().shape[0] == 1


def test_join_brand():
    kroger = LoadKroger(F_PATH)
    df = kroger.read_file()
    test_df = kroger.pre_process_data(df)
    dated_df = kroger.append_pd_numbers(test_df)
    brand_df = kroger.join_brand(dated_df)
    assert brand_df.isna().sum()['BU'] < brand_df.shape[0]  # expected upper limit
    assert brand_df.isna().sum()['PepsiBrand'] < brand_df.shape[0]
    assert brand_df.isna().sum()['SubBU'] < brand_df.shape[0]


def test_assert_correct_sales():
    sales_df = pd.DataFrame({'Dollars': [100, 10000, 2000]})
    kroger = LoadKroger(F_PATH)
    with pytest.raises(AssertionError) as a_error:
        kroger.assert_correct_sales(sales_df)
    assert AssertionError == a_error.type


def test_process_market_six_file():
    test_cols = ['Activity_Day', 'W_Date', 'Dollars', 'Units',
                 'Retail_Dollars', 'Retail_Units', 'BU',
                 'PepsiBrand', 'SubBU', 'LineageID',
                 'IsProcessed', 'IsDuplicate',
                 'PickupOrDelivery']
    kroger = LoadKroger(F_PATH)
    processed_df = kroger.process_marketsix_file()
    assert processed_df.columns.to_list() == test_cols
    assert processed_df.isna().sum()['Activity_Day'] == 0
