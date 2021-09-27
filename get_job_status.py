from config import CONN_STRING_TDWH
import sqlalchemy
from sqlalchemy.types import String
import pandas as pd
import pyodbc
import win32com.client
outlook = win32com.client.Dispatch("Outlook.Application")

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_string("cust",
                    None,
                    "customer name" 
                    )
flags.DEFINE_string("job",
                    None,
                    "job name" 
                    )
flags.DEFINE_bool("debug",
                    False,
                    "Run in debug mode")

# Required flag
#flags.mark_flag_as_required("cust")
flags.mark_flag_as_required("job")

# Amazon FreshPN command line args: --cust amazon_freshpn --job  GECM_RefreshAmazonFreshPN
# Sams pickup command line args: --cust sams_pickup --job GECM_RefreshSamsPickup
# WMT OG command line args: --cust walmart_og --job GECM_RefreshWalmartOG


def main(argv):
    del argv
    cust = FLAGS.cust
    job = FLAGS.job
    debug = FLAGS.debug

    html = get_job_status_html(job=job, debug=debug)
    html_load = get_load_status_html(cust=cust, debug=debug)
    if (html_load is not None):
        if (html is None):
            html = html_load
        else:
            html += '\n\n' + html_load
    #print('html:\n', html)
    if (html is None):
        html = '<html><body>Nothing to report.</html></body>'
    send_email(cust=cust, job=job, msg_body=html, debug=debug)


def get_job_status_html(job, debug=False):
    query = """
        SELECT
        --TOP 1
        [JobName]
        ,[Output]
        ,[created]
        ,[createdby]
        FROM [TDWH].[dbo].[ManualFileLoad_JobStatus]
        WHERE [JobName] = '{job}'
        ORDER BY [created] DESC
        """.format(job=job)

    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    # if (debug):
    #     print('sp:\n', sp)
    # db_engine.execute(sp)

    if (debug):
        print('Query:\n', query)
    df = pd.read_sql(query, db_engine)
    row_count, col_count = df.shape
    if (debug):
        print('row_count: ', row_count)
    if (row_count == 0):
        print('\nFollowing query produced no rows:\n', query)
        output = None
    else:
        output = df.Output[0]
        if (debug):
            print('Output: ', output)
    return output


def get_load_status_html(cust, debug=False):
    query = """
        SELECT
        TOP 1  
        [Cust]
        ,[Output]
        ,[created]
        ,[createdby]
        FROM [TDWH].[dbo].[ManualFileLoad_LoadStatus]
        WHERE [Cust] = '{cust}'
        ORDER BY [created] DESC
        """.format(cust=cust)

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
        output = None
    else:
        output = df.Output[0]
        if (debug):
            print('Output: ', output)
    return output


def send_email(cust, job, msg_body, debug=False):
    Msg = outlook.CreateItem(0) # Email
    Msg.To = "David.McIntyre.Contractor@pepsico.com" # you can add multiple emails with the ; as delimiter. E.g. test@test.com; test2@test.com;
    #Msg.CC = "test@test.com"
    Msg.Subject = "[MAN_FILE_LOAD] Load Status for '" + cust.upper() + "'" # + ' (Job: ' + job + ')'
    Msg.BodyFormat = 2
    Msg.HTMLBody = msg_body
    #Msg.display()
    Msg.Send()


if __name__ == "__main__":
    app.run(main)
