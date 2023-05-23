from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from pylogix import PLC
import logging

# Set up logging
# TODO: Dynamic daily log text + in spec folder
logging.basicConfig(filename='log.txt', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

PLC_IP = '192.168.0.31'
TAG_CATEGORY = "report_data"
START_HOURLY_ARCHIVE = 0
STOP_HOURLY_ARCHIVE = 200
START_HOURLY_RECORD = 0
STOP_HOURLY_RECORD = 26

# TODO: Come from DB
LAST_READ_DATETIME_FROM_DB = datetime(2023, 1, 18, 5, 59, 39)
# TODO: Come from DB
LAST_READ_NUMBER_FROM_DB = 123

# TODO: Come from Timing Sys
DATETIME_FROM_TIMING_SYS = datetime(2023, 1, 18, 5, 59, 39)
END_POINT = True
# TODO: Come from Timing Sys
NUMBER_FROM_TIMING_SYS = 0
TABLE_NAME = 'records_table'
# TODO: CLOUD Azure MYSQL Setup

# TODO: Check Conn Setup & update
# Define the connection string to the MySQL database
#connection_string = 'mysql+pymysql://username:password@hostname/database_name'

# Create the SQLAlchemy engine
#engine = create_engine(connection_string)


# function 0
# define the function to find the refrence point
def find_stop_datetime(plc_connection, tag_category: str, start_point: int) -> int:
    """
        Reads the DateTime of a tag from the PLC.
        finds the refrence stop point, for optimized speed and performance it picks up where it left.
    """
    if not isinstance(tag_category, str):
        raise ValueError("Tag category must be a string")
    # optimized to read from where it left on DB and stop once it find the target rather loop from start
    for i in range(start_point, start_point + STOP_HOURLY_ARCHIVE + 1):
        # Construct tag path
        target = i % (STOP_HOURLY_ARCHIVE + 1)
        tag_path = f'{tag_category}[{target}].time.'

        # Read all values for tag
        test_conn = plc_connection.Read(tag_path + 'year')
        response = plc_connection.Read

        if test_conn.Status != "Success":
            # TODO: LOG IT !
            print(f"Read failed for tag {tag_category} at number {i}")
            logging.error(f"Read failed for tag {tag_category} at number {i}")
            return None
            # raise Exception(f"Read failed for tag {tag_category} at number {read_num}")

        # Extract date-time values
        year = response(tag_path + 'Year').Value
        month = response(tag_path + 'Month').Value
        day = response(tag_path + 'Day').Value
        hour = response(tag_path + 'Hour').Value
        minute = response(tag_path + 'Minute').Value
        second = response(tag_path + 'Second').Value

        # Check for invalid date-time values
        if year == 0 and month == 0 and day == 0 and hour == 0 and minute == 0 and second == 0:
            return target

# function 1
# define the function to read PLC datetime values
def read_tag_datetime(plc_connection, tag_category: str, read_num: int) -> object:
    """
        Reads the DateTime of a tag from the PLC.
        Returns a datetime object or None if the tag is not found, valid or Connection Problem.
    """
    if not isinstance(tag_category, str):
        raise ValueError("Tag category must be a string")
    if not isinstance(read_num, int):
        raise ValueError("Read number must be an integer")

    # Construct tag path
    tag_path = f'{tag_category}[{read_num}].time.'

    # Read all values for tag
    test_conn = plc_connection.Read(tag_path + 'year')
    response = plc_connection.Read

    if test_conn.Status != "Success":
        # TODO: LOG IT !
        print(f"Read failed for tag {tag_category} at number {read_num}")
        logging.error(f"Read failed for tag {tag_category} at number {read_num}")
        return None
        # raise Exception(f"Read failed for tag {tag_category} at number {read_num}")

    # Extract date-time values
    year = response(tag_path + 'Year').Value
    month = response(tag_path + 'Month').Value
    day = response(tag_path + 'Day').Value
    hour = response(tag_path + 'Hour').Value
    minute = response(tag_path + 'Minute').Value
    second = response(tag_path + 'Second').Value
    microsecond = response(tag_path + 'Microsecond').Value
    day_of_week = response(tag_path + 'DayOfWeek').Value
    day_time_saving = response(tag_path + 'DST_ON').Value

    # Construct and return datetime object
    datetime_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"

    # Check for invalid date-time values
    if year == 0 and month == 0 and day == 0:
        print(f"End-Point date-time value for {read_num} : {datetime_str}")
        logging.info(f"End-Point date-time value for {read_num} : {datetime_str}")
        return {"end_point": True,
                "read_num": read_num,
                "datetime_obj": None,
                "datetime": datetime_str
                }
        # raise ValueError(f"Invalid date-time value for {read_num} : {year}-{month}-{day} {hour}:{minute}:{second}")

    try:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        print(f"The value of {read_num} is: {datetime_obj}")
        logging.info(f"The value of {read_num} is: {datetime_obj}")
        # TODO: READ NUMBER return
        return {"end_point": False,
                "read_num": read_num,
                "datetime_obj": datetime_obj,
                "datetime_str": datetime_str,
                "raw_attributes": [year, month, day, hour, minute, second, microsecond, day_of_week, day_time_saving]
                }
    except ValueError:
        print(f"Invalid date-time value for {read_num} : {datetime_str}")
        logging.error(f"Invalid date-time value for {read_num} : {datetime_str}")
        return None
        # raise ValueError(f"Invalid date-time value for {read_num} : {datetime_str}")


# function 2
def search_for_match(plc_connection, start_tag_num: int, stop_tag_num: int,
                     last_read_datetime_from_db: datetime) -> object:
    """
    Searches for a tag that matches the last read datetime from the database.
    Returns the datetime object of the closest matching tag or None if no matches are found.
    """
    # initialize closest_datetime to None and smallest_timedelta to a large value
    closest_datetime = None
    smallest_timedelta = [timedelta(days=365)]  # set to 1 year initially

    for i in range(start_tag_num, stop_tag_num):
        current_read_values = read_tag_datetime(plc_connection=plc_connection, tag_category=TAG_CATEGORY, read_num=i)
        current_read_datetime = current_read_values["datetime_obj"]
        if LAST_READ_DATETIME_FROM_DB == current_read_datetime:
            print(
                f"Found a match on search all ,The value of current number  {i} is: {current_read_datetime} & it matches  = {LAST_READ_DATETIME_FROM_DB}")
            logging.info(
                f"Found a match on search all ,The value of current number  {i} is: {current_read_datetime} & it matches  = {LAST_READ_DATETIME_FROM_DB}")
            # TODO: READ NUMBER return
            return current_read_values
        if current_read_datetime is not None:
            # TODO unify [0] to dictionary time_obj
            timedelta_to_desired = [abs(current_read_datetime - last_read_datetime_from_db), current_read_values]
            if timedelta_to_desired[0] < smallest_timedelta[0]:
                closest_read_values = current_read_values
                smallest_timedelta = timedelta_to_desired

    print("couldn't get a match on search all , stage 3 Checking the closest date now")
    logging.info("couldn't get a match on search all , stage 3 Checking the closest date now")
    # print the closest date-time value
    if not closest_read_values == None:
        print(f"The closest date-time value is: {closest_read_values['datetime_obj']}")
        logging.info(f"The closest date-time value is: {closest_read_values['datetime_obj']}")
    else:
        # TODO: LOG IT !
        print("No valid date-time values were found.")
        logging.error("No valid date-time values were found.")
    # TODO: READ NUMBER return
    return closest_read_values


# function 3
def check_last_read(plc_connection, tag_category: str, last_read_number_from_db: int, last_read_datetime_from_db,
                    start_hourly_archive: int, stop_hourly_archive: int) -> object:
    """

    :param plc_connection:
    :param tag_category:
    :param last_read_number_from_db:
    :param last_read_datetime_from_db:
    :param start_hourly_archive:
    :param stop_hourly_archive:
    :return: The atributes of last read if matching with DB, if not look for a match and find either a match or closest match
    """

    logging.info("tage 1 checking if db is having same read")
    print("stage 1 checking if db is having same read")

    last_values_from_PLC = read_tag_datetime(plc_connection=plc_connection, tag_category=tag_category,
                                             read_num=last_read_number_from_db)
    last_value_datetime_from_PLC = last_values_from_PLC["datetime_obj"]

    if last_read_datetime_from_db == last_value_datetime_from_PLC:
        print(
            f"It matches the last read, the value of the last read is {last_read_datetime_from_db} & last read number is {last_read_number_from_db}")
        logging.info(
            f"It matches the last read, the value of the last read is {last_read_datetime_from_db} & last read number is {last_read_number_from_db}")
        return last_values_from_PLC
    else:
        print("Couldn't match, stage 2 searching all")
        logging.info("Couldn't match, stage 2 searching all")
        match_search = search_for_match(plc_connection=plc_connection, start_tag_num=start_hourly_archive,
                                        stop_tag_num=stop_hourly_archive,
                                        last_read_datetime_from_db=last_read_datetime_from_db)
        return match_search

# function 4
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
        print(f"Read failed for min value test, tag {tag_category} at number {read_num} at record {record_num}")
        logging.error(f"Read failed for min value test, tag {tag_category} at number {read_num} at record {record_num}")
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
        logging.info("Connected to the PLC")
        print("Connected to the PLC")
        data_stream_list = []
        last_value = check_last_read(plc_connection=plc,
                                     tag_category=TAG_CATEGORY,
                                     last_read_number_from_db=LAST_READ_NUMBER_FROM_DB,
                                     last_read_datetime_from_db=LAST_READ_DATETIME_FROM_DB,
                                     start_hourly_archive=START_HOURLY_ARCHIVE,
                                     stop_hourly_archive=STOP_HOURLY_ARCHIVE)

        while END_POINT != True:
            start_data_point = last_value["read_num"] + 1
            stop_data_point = find_stop_datetime(plc_connection=plc, tag_category=TAG_CATEGORY,
                                                 start_point=start_data_point)
            for j in range(start_data_point, start_data_point + STOP_HOURLY_ARCHIVE + 1):
                target_read = j % (stop_data_point + 1)
                # Check if we reached endpoint
                if target_read == stop_data_point:
                    END_POINT = True

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
                # TODO: Make dyno CSV with Datetime
                # TODO: Folder Setup and if not exist Create ( Sample In Auto _ manu file )
                # TODO: ADD ERROR handling
                # Overwriting mode
                df.to_csv('my_data.csv', mode='w', index=False)

                # If append needed for later
                # new_df.to_csv('my_data.csv', mode='a', index=False, header=False)

                # TODO: DB setup
                # df.to_sql(table_name, con=engine, if_exists='replace', index=False)

                # Recheck Num + End Point REQ

    else:
        logging.info("Could not connect to PLC")
        print("Could not connect to PLC")
