# from config import CONN_STRING_TDWH
from config import CONN_STRING_TDWH
import sqlalchemy
from sqlalchemy.types import String
import pandas as pd
import pyodbc
import os

DATA_TYPE_F3 = 'f3'
DATA_TYPE_BM = 'bm'


def get_dates(debug):
    query = """
        select
        distinct
        d.[FiscalYearNumber]
        ,d.[PeriodNumber]
        ,d.[PeriodWeekNumber]
        ,d.WeekNumber
		,CONVERT(VARCHAR(22), DATEADD(dd, 6, d.WeekDateValue), 112) [DATE_STR]
        from TDWH.Common.DateDimensionView d 
        where d.FiscalYearNumber >= 2020
        and d.FiscalYearNumber <= 2021
        and d.DateValue <= '2021-08-22'
        and d.DateValue in ( -- tmp!!
           '2020-08-30'
        )
        order by
        d.[FiscalYearNumber]
        ,d.WeekNumber
        """

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print('row_count: ', row_count)
    if (row_count == 0):
        print('\nFollowing query produced no rows:\n', query)
        exit(1)
    else:
        return df


def get_f3_data(debug, year, week):
    query = """
-- Parms (set as approp.)
declare @year int 
,@week int

set @year = {year}
set @week = {week}

--set @year = 2020
--set @week = 32
;

WITH results as (
    select
    --top 100
    [ASIN]
    ,case isprime
    WHEN 0 then 'Fresh'
    WHEN 1 then 'PrimeNow'
    WHEN 2 then 'B&M'
    END [program]
    ,[calendar_day][date_shipped]
    ,[warehouse_ID][fc]
    ,[Product_Description] [item_name]
    ,[Brand_Name] [merchant_brand_name]
    ,sum([Paid_Shipped_Units]) [shipped_units]
    ,sum([Product_COGS]) [shipped_cogs]
    ,sum([Product_GMS]) [shipped_ops]
    FROM [TDWH].[DataStage].[STG_AmazonMasterFresh] sales
    JOIN TDWH.Common.DateDimension d on d.[DateValue] = sales.Delivery_Day
    where d.FiscalYearNumber = @year
    and d.WeekNumber = @week
    and sales.isprime in (0, 1)
    GROUP BY
    [ASIN]
    ,isprime
    ,[calendar_day]
    ,[warehouse_ID]
    ,[Product_Description]
    ,[Brand_Name]
)
,prod_info as (
	select
	distinct
	ASIN
	,[item_name]
	from results
)
,prod_info2 as (
	select
	ASIN
	,[item_name]
	,ROW_NUMBER() OVER(PARTITION BY [ASIN] ORDER BY [item_name]) AS RowNum
	from prod_info
)
,brand_info as (
	select
	distinct
	ASIN
	,[merchant_brand_name]
	from results
)
,brand_info2 as (
	select
	ASIN
	,[merchant_brand_name]
	,ROW_NUMBER() OVER(PARTITION BY [ASIN] ORDER BY [merchant_brand_name]) AS RowNum
	from brand_info
)

SELECT 
res.[ASIN]
,res.[program]
,res.[date_shipped]
,res.[fc]
,prod.item_name
,brand.[merchant_brand_name]
,SUM(res.[shipped_units])[shipped_units]
,SUM(res.[shipped_cogs])[shipped_cogs]
,SUM(res.[shipped_ops])[shipped_ops]
FROM results res
JOIN prod_info2 prod on prod.ASIN = res.ASIN
AND prod.RowNum = 1
JOIN brand_info2 brand on brand.ASIN = res.ASIN
AND brand.RowNum = 1
GROUP BY
res.[ASIN]
,res.[program]
,res.[date_shipped]
,res.[fc]
,prod.item_name
,brand.[merchant_brand_name]
    """.format(year=year, week=week)

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print('row_count: ', row_count)
    if (row_count == 0):
        print('\nFollowing query produced no rows:\n', query)
    else:
        return df


def get_bm_data(debug, year, week):
    query = """
        -- Parms (set as approp.)
        declare @year int 
        ,@week int

        set @year = {year}
        set @week = {week}

        --set @year = 2021
        --set @week = 32

        select
        --top 100
        [asin]
        ,[Product_Description] [item_name]
        ,[Brand_Name] [brand_name]
        ,[calendar_day][date]
        ,SUM([Product_GMS]) [ops]
        ,SUM([Paid_Shipped_Units]) [units]
        ,cast(0 as float) [asp]
        ,SUM([Product_COGS]) [pcogs]

        FROM [TDWH].[DataStage].[STG_AmazonMasterFresh] sales
        join TDWH.Common.DateDimension d on d.[DateValue] = sales.Delivery_Day
        where d.FiscalYearNumber = @year
        and d.WeekNumber = @week
        and sales.isprime in (2)
        GROUP BY
        [ASIN]
        ,[Product_Description]
        ,[Brand_Name]
        ,[calendar_day]
        """.format(year=year, week=week)

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print('row_count: ', row_count)
    if (row_count == 0):
        print('\nFollowing query produced no rows:\n', query)
    else:
        return df


if __name__ == "__main__":
    data_types = [DATA_TYPE_F3, DATA_TYPE_BM]
    data_types = [DATA_TYPE_BM, DATA_TYPE_F3]
    data_types = [DATA_TYPE_F3] #tmp!!!
    df_dates = get_dates(debug=True)
    print('dates:', df_dates)
    print(df_dates.iloc[0])
    print('date_str', df_dates.iloc[0][4])
    index = df_dates.index
    num_rows = len(index)
    print('num_rows', num_rows)
    row_num = 0
    while (row_num < num_rows):
        year = df_dates.iloc[row_num][0]
        week = df_dates.iloc[row_num][3]
        date_str = df_dates.iloc[row_num][4]
        print('row_num/date info:', row_num, year, week, date_str)

        for data_type in data_types:
            print(data_type)
            if (data_type == DATA_TYPE_F3):
                file_prefix = 'AmazonF3_DailySales_weekending_'
                folder_name = 'F3'
                df = get_f3_data(debug=True, year=year, week=week)
            if (data_type == DATA_TYPE_BM):
                file_prefix = 'AmazonBM_DailySales_weekending_'
                folder_name = 'BM'
                df = get_bm_data(debug=True, year=year, week=week)
    
            if df is None:
                print('DataFrame is empty!')
                #exit(1)
            else:
                print('data:', df)
                output_folder = r'C:\Users\09276425\Documents\SQL Server Management Studio\JIRA\DEP-2779 Provide 2020 to date (through 20210731) historical data in S3 for DE\Amazon F3 (Grocery)\Data'
                output_folder += os.path.sep + folder_name
                output_file = output_folder + os.path.sep + file_prefix + date_str + '.xlsx'
                print(output_file)
                df.to_excel(output_file, index=False)

        #exit(1)

        row_num+= 1
        #if row_num > 1:
            #exit(1)
        #exit(1)
