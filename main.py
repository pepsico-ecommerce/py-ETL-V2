import os
import glob
from datetime import date

from absl import app
from absl import flags

from py_ETL.albertsons import albertsons_etl
from py_ETL.amazon_freshpn import amazon_freshpn_etl
from py_ETL.kroger import kroger_ship_etl
from py_ETL.kroger import kroger_pickup_etl
from py_ETL.sams import sams_etl
from py_ETL.meijer import meijer_etl
from py_ETL.meijer import upload_blob as meijer_upload
from py_ETL.hyvee import hyvee_etl

from config import CONN_STRING_TDWH
import sqlalchemy

import winsound

""" absl CLI interface for the ET packages stored in this repo """

MODE_INTERACTIVE = 1
MODE_AUTOMATIC = 2

# change DIR to your needs
# DIR = kroger_fp
ROOT_SRC_DIR = r'C:\CompaniesSourceFilesReceived'
ROOT_INTERIM_CSV_DIR = r'C:\CompaniesSourceFilesConvertedToCSV'
ROOT_OUTPUT_DIR = r'\\pepwwb00348\FMS_Ecom\Source'

CUSTOMER_KROGER_SHIP = 'kroger_ship'
CUSTOMER_KROGER_PICKUP = 'kroger_pickup'
CUSTOMER_SAMS = 'sams'
CUSTOMER_SAMS_PICKUP = 'sams_pickup'
CUSTOMER_ALBERTSONS = 'albertsons'
CUSTOMER_AMAZON_FRESHPN = 'amazon_freshpn'
CUSTOMER_MEIJER = 'meijer'
CUSTOMER_HYVEE = 'hyvee'

FLAGS = flags.FLAGS
flags.DEFINE_string("customer",
                    None,
                    "Customer ETL to run, currently supports " 
                    + CUSTOMER_KROGER_SHIP
                    + ", " + CUSTOMER_KROGER_PICKUP
                    + ", " + CUSTOMER_SAMS
                    + ", " + CUSTOMER_SAMS_PICKUP
                    + ", " + CUSTOMER_ALBERTSONS
                    + ", " + CUSTOMER_AMAZON_FRESHPN
                    + ", " + CUSTOMER_MEIJER
                    + ", " + CUSTOMER_HYVEE
                    )
flags.DEFINE_string("year",
                    None,
                    "Year to use for KrogerDesc query (customer: kroger_ship)")
flags.DEFINE_bool("debug",
                    False,
                    "Run in debug mode")

# Required flag
flags.mark_flag_as_required("customer")


def main(argv):
    del argv
    customer = FLAGS.customer
    year = FLAGS.year
    debug = FLAGS.debug
    exec_mode = MODE_INTERACTIVE
    # exec_mode = MODE_AUTOMATIC

    # Validate parms
    validate_parm_customer(customer)
    # validate_parm_kroger_desc(customer)

    # Init, show parms
    print_keyvalpair('customer', customer)
    if customer in [CUSTOMER_KROGER_SHIP]:
        customer_input_folder = 'KrogerShip'
        customer_output_folder = 'KrogerShip'
    if customer in [CUSTOMER_KROGER_PICKUP]:
        customer_input_folder = 'KrogerPickup'
        customer_output_folder = 'Kroger'
    if customer in [CUSTOMER_SAMS]:
        customer_input_folder = 'SamsDotCom'
        customer_output_folder = 'SamsDotCom'
        file_prefix = 'D2H'
    if customer in [CUSTOMER_SAMS_PICKUP]:
        customer_input_folder = 'SamsPickup'
        customer_output_folder = 'SamsPickup'
    if customer in [CUSTOMER_ALBERTSONS]:
        customer_input_folder = 'Albertsons'
        customer_output_folder = 'Albertsons'
    if customer in [CUSTOMER_AMAZON_FRESHPN]:
        customer_input_folder = 'AmazonFreshPN'
        customer_output_folder = 'AmazonFreshPN'
    if customer in [CUSTOMER_MEIJER]:
        customer_input_folder = 'Meijer'
        customer_output_folder = 'Meijer'
    if customer in [CUSTOMER_HYVEE]:
        customer_input_folder = 'HyVee'
        customer_output_folder = 'HyVeeSales'

    # Make settings
    # input_dir = ROOT_SRC_DIR + os.path.sep + customer_input_folder
    input_dir = os.path.join(ROOT_SRC_DIR, customer_input_folder)
    # output_dir = ROOT_OUTPUT_DIR + os.path.sep + customer_output_folder
    output_dir = os.path.join(ROOT_OUTPUT_DIR, customer_output_folder)
    if customer in [CUSTOMER_MEIJER]:
        output_dir = os.path.join(ROOT_INTERIM_CSV_DIR, customer_output_folder)

    # Show settings
    print_keyvalpair('input directory', input_dir)
    print_keyvalpair('output directory', output_dir)
    cont = 'n'
    while not cont == 'y':
        available = os.listdir(input_dir)
        load = [f for f in available if f.endswith('.xlsx')]
        num_files = len(load)
        if (num_files == 0):
            print('No files to load in input directory: ', input_dir)
            exit()
        print('# available files: ', num_files)
        print('available files: ', load)
        cont = 'y'
        if (exec_mode == MODE_INTERACTIVE):
            cont = input('continue loading? y/n ')
        break
    if cont == 'y':
        # Make more settings
        basename = load[0]
        # input_file_path = input_dir + os.path.sep + basename
        input_file_path = os.path.join(input_dir, basename)
        output_file_name = get_output_file_name(basename=basename, output_dir=output_dir)
        print_keyvalpair('src file', input_file_path)   # todo: check if func like this already exists
        print_keyvalpair('output file', output_file_name)

        # #######################################################
        # Process Kroger Ship
        if customer in [CUSTOMER_KROGER_SHIP]:
            e = kroger_ship_etl.LoadKrogerShip(input_file_path)
            e.process_file(output_file_name=output_file_name)

        # #######################################################
        # Process Kroger Pickup
        if customer in [CUSTOMER_KROGER_PICKUP]:
            e = kroger_pickup_etl.LoadKrogerPickup(input_file_path)
            e.process_file(output_file_name=output_file_name)

        # #######################################################
        # Process Sams (DotCom)
        if customer in [CUSTOMER_SAMS]:
            e = sams_etl.LoadSams(input_file_path)
            e.process_file(output_file_name=output_file_name, sheet_name='Sales Trend Report', customer_name=customer, src_dir=input_dir)

        # #######################################################
        # Process Sams Pickup
        if customer in [CUSTOMER_SAMS_PICKUP]:
            e = sams_etl.LoadSams(input_file_path)
            e.process_file(output_file_name=output_file_name, sheet_name='Inventory Trend Report', customer_name=customer, src_dir=input_dir)

        # #######################################################
        # Process Albertsons
        if customer in [CUSTOMER_ALBERTSONS]:
            e = albertsons_etl.LoadAlbertsons(input_file_path)
            e.process_file(output_file_name=output_file_name)

        # #######################################################
        # Process Amazon FreshPN
        if customer in [CUSTOMER_AMAZON_FRESHPN]:
            e = amazon_freshpn_etl.LoadAmazonFreshPN(input_file_path)
            e.process_file(output_file_name=output_file_name, src_dir=input_dir, basename=basename)

        # #######################################################
        # Process Meijer
        if customer in [CUSTOMER_MEIJER]:
            remove_folder_content(output_dir)
            e = meijer_etl.LoadMeijer(input_file_path)
            e.process_file(output_file_name=output_file_name, src_dir=input_dir)

            # Upload file to blob
            # upld = meijer_upload.MeijerUploadBlob(output_file_name)
            # upld.perf_upload(debug=debug)

        # #######################################################
        # Process HyVee
        if customer in [CUSTOMER_HYVEE]:
            e = hyvee_etl.LoadHyVee(input_file_path)
            e.process_file(output_file_name=output_file_name)

        # #######################################################
        # Sales tot, date col
        try:
            sales_tot = e.SALES_TOT
        except AttributeError:
            sales_tot = None
        try:
            date_value = e.DATE_VALUE
        except AttributeError:
            date_value = 'NA'
        process_sales_tot(customer, sales_tot, date_value, debug)


def print_keyvalpair(key, val):
    print(f'{key:24}: {val:40}')


def get_output_file_name(basename, output_dir):
    loc = basename.rfind(".")
    basename_excl_suffix = basename[0:loc]
    # output_file_name = output_dir + os.path.sep + basename_excl_suffix + '.csv'
    output_file_name = os.path.join(output_dir, basename_excl_suffix + '.csv')
    return output_file_name


def validate_parm_customer(customer):
    if customer not in [CUSTOMER_KROGER_SHIP, CUSTOMER_KROGER_PICKUP, CUSTOMER_SAMS
                        , CUSTOMER_SAMS_PICKUP, CUSTOMER_ALBERTSONS
                        , CUSTOMER_AMAZON_FRESHPN, CUSTOMER_MEIJER, CUSTOMER_HYVEE]:
        # raise Exception("Invalid customer: '" + customer + "'")
        print("Invalid customer '", customer, "'", sep='')
        exit(1)


# Get periodno, weekno from string
def get_period_weekno(s):
    s = s.upper()
    loc = s.find("P")
    period_num = s[1:loc+3]
    if loc >= 0:
        loc = s.find("W")
        week_num = s[loc+1:loc+3]
    if loc < 0:
        raise Exception('Period or Week # missing in following string: {}'.format(s))
    return period_num.strip('W'), week_num.strip(' ')


def process_sales_tot(customer, sales_tot, date_value, debug):
    if sales_tot is None:
        print('WARNING: SALES_TOT class var does not exist')
        return
    print_keyvalpair('sales tot ', '{:.2f}'.format(sales_tot))
    if date_value is not None:
        print_keyvalpair('date value ', date_value)
    # Write to DB
    connection_string = CONN_STRING_TDWH
    db_engine = sqlalchemy.create_engine(connection_string)
    query = """
    insert [dbo].[ManualFileLoadLog] (CustomerName, Cust_WeekNo, Total)
    values ('{customer}', '{date_value}', {sales_tot})
    """.format(customer=customer, date_value=date_value, sales_tot=sales_tot)
    if (debug):
        print('Query:\n', query)
    # Create connection
    conn = db_engine.connect()
    # Exec cmd
    conn.execute(query)


def remove_folder_content(folder):
    files = glob.glob(os.path.join(folder, '*'))
    for f in files:
        # input('rm fname: ' + f)
        os.remove(f)


if __name__ == "__main__":
    app.run(main)
