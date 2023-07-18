import pandas as pd
import numpy as np
from pylogix import PLC
import os
import utilities.connection as uc
from custom_helper_functions.custom_functions import *

# TODO: Come from DB
LAST_READ_DATETIME_FROM_DB = datetime(2021, 5, 2, 23, 59, 59)
# TODO: Come from DB
LAST_READ_NUMBER_FROM_DB = 150

END_POINT = False

# TODO: CLOUD Azure MYSQL Setup


# DB Local MySQL setup & Run
engine = uc.mysql_connection()

if engine is not False:
    try:
        # set up the connection to the PLC
        with PLC() as plc:
            plc.IPAddress = PLC_IP
            connected = plc.IPAddress
            if connected:
                logging.info("Connected to the PLC")
                print("Connected to the PLC")
                # matched with db or not ( it is used in search_match function and last value check function ) we want
                # to add 1 for a match since we have the record in db but want to read the closest record if no
                # match found
                is_matched = 0
                data_stream_list = []
                last_value = check_last_read(plc_connection=plc,
                                             tag_category=TAG_CATEGORY,
                                             last_read_number_from_db=LAST_READ_NUMBER_FROM_DB,
                                             last_read_datetime_from_db=LAST_READ_DATETIME_FROM_DB,
                                             start_hourly_archive=START_HOURLY_ARCHIVE,
                                             stop_hourly_archive=STOP_HOURLY_ARCHIVE)

                while not END_POINT:
                    start_data_point = last_value["read_num"] + is_matched
                    stop_data_point = find_stop_datetime(plc_connection=plc, tag_category=TAG_CATEGORY,
                                                         start_point=start_data_point)
                    for j in range(start_data_point, start_data_point + STOP_HOURLY_ARCHIVE + 1):
                        target_read = j % (STOP_HOURLY_ARCHIVE + 1)
                        # Check if we reached endpoint
                        print(
                            f" current main loop, target read is {target_read} ===== stop data point{stop_data_point}")
                        if target_read == stop_data_point:
                            END_POINT = True
                            break

                        record_list = []

                        for i in range(START_HOURLY_RECORD, STOP_HOURLY_RECORD + 1):
                            current_read_record = read_tag_record(plc_connection=plc, tag_category=TAG_CATEGORY,
                                                                  read_num=target_read, record_num=i)
                            if current_read_record is not None:
                                # Append the dictionary to a list of dictionaries
                                record_list.append(current_read_record)
                            # Turn it into a data frame
                        df = pd.DataFrame(record_list)

                        print(df.head())
                        print(df.shape)
                        # Saving dataframe into a CSV
                        # TODO: ADD ERROR handling

                        # Get the current directory
                        current_directory = os.getcwd()

                        # Convert the 'datetime_obj' column to datetime
                        df['datetime_obj'] = pd.to_datetime(df['datetime_obj'])
                        # TODO: Test it
                        # Change the seconds to 00 Cause our SQL table is up to Min precision
                        df['datetime_obj'] = df['datetime_obj'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:00'))

                        # Get the first datetime object
                        date_time_obj = df['datetime_obj'].iloc[0]

                        # Extract year, month, and day
                        year = date_time_obj.year
                        month = date_time_obj.month
                        day = date_time_obj.day

                        # Get the name of the month
                        month_name = date_time_obj.strftime('%B')

                        # Create the target folder path
                        target_folder = os.path.join(current_directory,
                                                     f'csv_archive/{year}/{month_name} ({month})/{day}')

                        # Create the target folder if it doesn't exist
                        os.makedirs(target_folder, exist_ok=True)

                        # Define the target file path
                        target_file = os.path.join(target_folder, f'{target_read}.csv')

                        # Check if the file already exists
                        if os.path.exists(target_file):
                            counter = 1
                            while True:
                                # Modify the target filename by adding a counter
                                modified_target_file = os.path.join(target_folder, f'{target_read}_{counter}.csv')

                                # Check if the modified filename already exists
                                if not os.path.exists(modified_target_file):
                                    target_file = modified_target_file
                                    break
                                counter += 1

                        # Overwriting mode
                        # Save the DataFrame to the target CSV file
                        df.to_csv(target_file, mode='w', index=False)
                        # TODO: Clean Later
                        # If append needed for later
                        # new_df.to_csv('my_data.csv', mode='a', index=False, header=False)

                        # Add NaN (NULL) as the default value to the 'status' column in SQL table
                        df['status'] = np.nan

                        # dropping unneeded data fro DB
                        columns_to_drop = ['raw_attributes', 'end_point']
                        df_dropped = df.drop(columns=columns_to_drop)
                        # TODO: test setup and working on inserts + update table
                        # change db tables or change the tag name
                        # Extract the tag category and original data
                        df['tag_category'] = df['tag_name'].str.extract(r'^(.*?)\[')
                        df['original_data'] = df['tag_name']

                        # Extract the tag name
                        df['tag_name'] = df['tag_name'].str.extract(r'\[(\d+)\]$')


                        # TODO: DB setup
                        # Insert the DataFrame into the existing SQL table
                        df_dropped.to_sql(name='raw_stream', con=engine, if_exists='append', index=False)
                        #TODO : test it

                        # Extracting the last datetime value from 'datetime_obj' column
                        last_datetime = pd.to_datetime(df['datetime_obj']).iloc[-1]

                        # Extracting the last value from 'tag_name' column
                        last_tag_name = df['tag_name'].str.extract(r'\[(\d+)\]').astype(int).iloc[-1][0]
            # TODO : see the break
            # Recheck Num + End Point REQ
            else:
                logging.info("Could not connect to PLC")
                print("Could not connect to PLC")
    except Exception as e:
        print(e)
        print("failed to do the data data extraction , Main loop")
else:
    print("Couldn't make connection to MYSQL DB, try again in 5 min")
