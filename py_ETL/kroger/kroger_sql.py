# from config import CONN_STRING_TDWH
from config import CONN_STRING_TDWH
import sqlalchemy
from sqlalchemy.types import String
import pandas as pd
import pyodbc


def get_kroger_desc(debug=True, year=-1, periodnumber=-1, periodweeknumber=-1):
    query = """
        -- Parms (set as approp.)
        DECLARE 
        @PeriodNumber INT		= {periodnumber}
        ,@PeriodWeekNumber INT	= {periodweeknumber}
        ,@Year INT				= {year}

        --set @Year = 1934

        SELECT 
        @Year [Year]
        ,cal.PeriodNumber
        ,cal.PeriodWeekNumber
        ,cal.DateValue
        ,DATEADD(dd, 6, cal.DateValue) [WeekEndDate]
        ,kcal.KrogerDesc
        FROM [TDWH].[Common].[WeekCalView] cal
        JOIN [TDWH].[dbo].[KrogerCalendar] kcal ON kcal.[WeekEndingDate] = dateadd(dd, 6, cal.DateValue)
        WHERE YEAR(cal.DateValue) = @Year
        AND cal.PeriodNumber = @PeriodNumber
        AND cal.PeriodWeekNumber = @PeriodWeekNumber
        """.format(periodnumber=periodnumber, periodweeknumber=periodweeknumber, year=year)

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print ('row_count: ', row_count)
    if (row_count == 0):
        # print('\nFollowing query produced no rows:\n', query)
        kroger_desc = None
        # exit(1)
    else:
        kroger_desc = df.KrogerDesc[0]
        # print ('kroger_desc: ', kroger_desc, ', len: ', len(kroger_desc), sep='')
    # exit(1)
    return kroger_desc, row_count

def get_kroger_desc_from_WkEndDate(debug, week_end_date):
    query = """
        -- Parms (set as approp.)
        DECLARE @week_end_date date = '{week_end_date}'
        DECLARE @week_start_date date = dateadd(dd, -6, @week_end_date)
        SELECT 
        cal.fiscalyearnumber [FiscalYearNumber]
        ,cal.PeriodNumber
        ,cal.PeriodWeekNumber
        ,@week_start_date [WeekStartDate]
        ,@week_end_date [WeekEndDate]
        ,kcal.KrogerDesc
        FROM [TDWH].[Common].[WeekCalView] cal
        JOIN [TDWH].[dbo].[KrogerCalendar] kcal ON kcal.[WeekEndingDate] = @week_end_date
        WHERE cal.DateValue = @week_start_date
        """.format(week_end_date=week_end_date)

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print('row_count: ', row_count)
    if (row_count == 0):
        # print('\nFollowing query produced no rows:\n', query)
        kroger_desc = None
        PeriodNumber = None
        PeriodWeekNumber = None
        # exit(1)
    else:
        kroger_desc = df.KrogerDesc[0]
        PeriodNumber = df.PeriodNumber[0]
        PeriodWeekNumber = df.PeriodWeekNumber[0]
        if (debug):
            print('kroger_desc: ', kroger_desc, ', PeriodNumber: ', PeriodNumber, ', PeriodWeekNumber: ', PeriodWeekNumber, sep='')
        # exit(1)
    return kroger_desc, PeriodNumber, PeriodWeekNumber, row_count


if __name__ == "__main__":
    # kroger_desc, row_count = get_kroger_desc(debug=True, year=2020, periodnumber=2, periodweeknumber=4)
    kroger_desc, PeriodNumber, PeriodWeekNumber, row_count = get_kroger_desc_from_WkEndDate(debug=True, week_end_date='2020-07-04')
    print('kroger_desc:', kroger_desc)
