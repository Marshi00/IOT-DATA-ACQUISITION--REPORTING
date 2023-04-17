from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from pylogix import PLC
import logging

# Set up logging
### TODO: Dynamic daily log text + in spec folder
logging.basicConfig(filename='log.txt', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

PLC_IP = '192.168.0.99'
TAG_CATEGORY = "report_data"
START_HOURLY_RECORD = 0
STOP_HOURLY_RECORD = 125
# TODO: Come from Timing Sys
DATETIME_FROM_TIMING_SYS = datetime(2023, 1, 18, 5, 59, 39)
# TODO: Come from Timing Sys
NUMBER_FROM_TIMING_SYS = 0
TABLE_NAME = 'records_table'
# TODO: CLOUD Azure MYSQL Setup

# TODO: Check Conn Setup & update
# Define the connection string to the MySQL database
#connection_string = 'mysql+pymysql://username:password@hostname/database_name'

# Create the SQLAlchemy engine
#engine = create_engine(connection_string)


# define the function to read PLC Record values
def read_tag_record(plc_connection, tag_category: str, read_num: int, record_num: int) -> object:
    """
        Reads the records of a desired tag from the PLC.
        Returns a dictionary or None if the tag is not found, valid or Connection Problem.
    """
    if not isinstance(tag_category, str):
        raise ValueError("Tag category must be a string")
    if not isinstance(read_num, int):
        raise ValueError("Read number must be an integer")

    # Construct tag path
    tag_path = f'{tag_category}[{read_num}].log_data[{record_num}].'

    # Read all values for tag
    test_conn = plc_connection.Read(tag_path + 'min')
    response = plc_connection.Read

    if test_conn.Status != "Success":
        # TODO: LOG IT !
        print(f"Read failed for tag {tag_category} at number {read_num} at record {record_num}")
        logging.error(f"Read failed for tag {tag_category} at number {read_num} at record {record_num}")
        return None
        #raise Exception(f"Read failed for tag {tag_category} at number {read_num} at record {record_num}")

    # Extract record values
    record_min = response(tag_path + 'MIN').Value
    record_max = response(tag_path + 'MAX') .Value
    record_total = response(tag_path + 'TOTAL').Value
    record_time = response(tag_path + 'TIME').Value
    record_min_minute = response(tag_path + 'MINMINUTE').Value
    record_max_minute = response(tag_path + 'MAXMINUTE').Value
    record_valid = response(tag_path + 'VALID').Value

    # Store the extracted values in a dictionary
    record_dict = {
        'Tag_name': f'{tag_category}[{read_num}].log_data[{record_num}]',
        'Min': record_min,
        'Max': record_max,
        'Total': record_total,
        'Time': record_time,
        'MinMinute': record_min_minute,
        'MaxMinute': record_max_minute,
        'Valid': record_valid,
    }
    # TODO: REMOVE AFTER DEBUG
    print(record_dict)
    return record_dict


# set up the connection to the PLC
with PLC() as plc:
    plc.IPAddress = PLC_IP
    connected = plc.IPAddress
    if connected:
        record_list = []
        logging.info("Connected to the PLC")
        print("Connected to the PLC")
        # read the tag 'MyTag' from the PLC
        for i in range(START_HOURLY_RECORD, STOP_HOURLY_RECORD):
            current_read_record = read_tag_record(plc_connection=plc, tag_category=TAG_CATEGORY,
                                                  read_num=NUMBER_FROM_TIMING_SYS, record_num=i)
            if current_read_record is not None:
                # Append the dictionary to a list of dictionaries
                record_list.append(current_read_record)
            # Turn it into a data frame
            df = pd.DataFrame(record_list)
        print(df.head())
        print(df.shape)
        # Saving dataframe into a CSV
        # TODO: Make dyno CSV with Datetime
        # TODO: Folder Setup and if not exist Create ( Sample In Auto _ manu file )
        # TODO: ADD ERROR handling
        # Overwriting mode
        df.to_csv('my_data.csv', mode='w', index=False)

        # If append needed for later
        #new_df.to_csv('my_data.csv', mode='a', index=False, header=False)

        # TODO: DB setup
        #df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    else:
        logging.info("Could not connect to PLC")
        print("Could not connect to PLC")
