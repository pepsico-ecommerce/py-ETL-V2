from config import CONN_STRING_TDWH
from config_ECommSalesDW import CONN_STRING_ECommSalesDW
import sqlalchemy
from sqlalchemy.types import String
import pandas as pd
import numpy as np
import pyodbc
import sys    
import os
import win32com.client
outlook = win32com.client.Dispatch("Outlook.Application")

def get_stg_data(debug=True):
    query = """
        declare @src varchar(99) = '[DataStage].[STG_AlbertsonsMasterSales]'

        SELECT 
        @src [src],
        cast (dte.[WeekDateValue] as date) [calendarWeekStartDate]
        ,dte.FiscalYearNumber [FiscalYear]
        ,dte.[WeekNumber] [fiscalYearWeekNumber]
        ,dte.PeriodNumber [fiscalPeriod]
        ,dte.PeriodWeekNumber [fiscalPeriodWeekNumber]
        ,sum(cast(sales.[NETAmount] as money)) [Dollars]
        --,count (*) [Rows]
        FROM [TDWH].[DataStage].STG_Albertsons_MasterSales sales
        join tdwh.common.datedimensionview dte on dte.[DateValue] = sales.ActivityDay
        where dte.[WeekDateValue] >= dateadd(dd, -60, getdate())
        group by
        dte.FiscalYearNumber
        ,dte.[WeekDateValue]
        ,dte.[WeekNumber]
        ,dte.PeriodNumber
        ,dte.PeriodWeekNumber
        order by dte.[WeekDateValue]        
        """

    connection_string = CONN_STRING_TDWH
    #connection_string = CONN_STRING_ECommSalesDW
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print ('row_count: ', row_count)
    if (row_count == 0):
        # print('\nFollowing query produced no rows:\n', query)
        whatever = False
        # exit(1)
    else:
        #kroger_desc = df.KrogerDesc[0]
        whatever = True
    # exit(1)
    return df


def get_ecommsldw_data(debug=True):
    query = """
        declare @src varchar(99) = '[landing].[Albertsons]'

        select
        @src [src],
        cast (dte.calendarWeekStartDate as DATE) [calendarWeekStartDate]
        ,dte.fiscalYear [FiscalYear]
        ,dte.fiscalYearWeekNumber
        ,dte.fiscalPeriod
        ,dte.fiscalPeriodWeekNumber
        ,sum (sale.NetAmount) [Dollars]
        --,count (*) [Rows]
        FROM [ECommSalesDW].[landing].[AlbertsonsDaily] sale
        join ECommSalesDW.dim.date dte on dte.dateId =  sale.ActivityDay
        where dte.calendarWeekStartDate >= dateadd(dd, -60, getdate())
        group by
        dte.calendarWeekStartDate
        ,dte.fiscalYear
        ,dte.fiscalYearWeekNumber
        ,dte.fiscalPeriod
        ,dte.fiscalPeriodWeekNumber
        order by
        dte.calendarWeekStartDate
        """

    #connection_string = CONN_STRING_TDWH
    connection_string = CONN_STRING_ECommSalesDW
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print ('row_count: ', row_count)
    if (row_count == 0):
        print('\nFollowing query produced no rows:\n', query)
    return df


def send_email(cmd, msg_body, debug=False):
    Msg = outlook.CreateItem(0) # Email
    Msg.To = "David.McIntyre.Contractor@pepsico.com" # you can add multiple emails with the ; as delimiter. E.g. test@test.com; test2@test.com;
    Msg.Subject = "[" + cmd + "]"
    Msg.BodyFormat = 2
    Msg.HTMLBody = msg_body
    Msg.display()
    Msg.Send()


if __name__ == "__main__":
    debug = False
    cmd_name = os.path.basename(sys.argv[0])
    cmd_name = cmd_name[:-3] # drop .py off name
    df_stg = get_stg_data(debug=debug)
    if (debug):
        print('df_stg', df_stg)
    df_esdw = get_ecommsldw_data(debug=debug)
    if (debug):
        print('df_esdw', df_esdw)

    # Compare DFs
    df_final = df_stg
    df_final.rename(columns={'src': 'Stg_Src'}, inplace=True)
    df_final.rename(columns={'Dollars': 'Stg_Dollars'}, inplace=True)
    df_final.insert(1, 'Landing_Src', df_esdw['src'])
    df_final['Landing_Dollars'] = df_esdw['Dollars']
    df_final['Diff'] = np.where(df_final['Stg_Dollars'] == df_final['Landing_Dollars'], 0, df_final['Landing_Dollars'] - df_final['Stg_Dollars'])

    # Send mail
    html = df_final.to_html(border=1,index=False)
    if (debug):
        print('html', html)
    send_email(cmd=cmd_name, msg_body=html)
