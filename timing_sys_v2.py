from datetime import datetime, timedelta
from pylogix import PLC
import logging
import pandas as pd

# Set up logging
logging.basicConfig(filename='log.txt', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


PLC_IP = '192.168.0.99'
TAG_CATEGORY = "report_data"
START_HOURLY_TAG = 0
STOP_HOURLY_TAG = 200
### TODO: Come from DB
LAST_READ_DATETIME_FROM_DB = datetime(2023, 1, 18, 5, 59, 39)
### TODO: Come from DB
LAST_READ_NUMBER_FROM_DB = 123




# define the function to read PLC tag values
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
        ### TODO: LOG IT !
        print(f"Read failed for tag {tag_category} at number {read_num}")
        logging.error(f"Read failed for tag {tag_category} at number {read_num}")
        return None
        #raise Exception(f"Read failed for tag {tag_category} at number {read_num}")

    # Extract date-time values
    year = response(tag_path + 'Year').Value
    month = response(tag_path + 'Month') .Value
    day = response(tag_path + 'Day').Value
    hour = response(tag_path + 'Hour').Value
    minute = response(tag_path + 'Minute').Value
    second = response(tag_path + 'Second').Value

    # Construct and return datetime object
    datetime_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"

    # Check for invalid date-time values
    if year == 0 or month == 0 or day == 0:
        print(f"Invalid date-time value for {read_num} : {datetime_str}")
        logging.error(f"Invalid date-time value for {read_num} : {datetime_str}")
        return None
        #raise ValueError(f"Invalid date-time value for {read_num} : {year}-{month}-{day} {hour}:{minute}:{second}")


    try:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        print(f"The value of {read_num} is: {datetime_obj}")
        logging.info(f"The value of {read_num} is: {datetime_obj}")
        ### TODO: READ NUMBER return
        return datetime_obj
    except ValueError:
        print(f"Invalid date-time value for {read_num} : {datetime_str}")
        logging.error(f"Invalid date-time value for {read_num} : {datetime_str}")
        return None
        #raise ValueError(f"Invalid date-time value for {read_num} : {datetime_str}")


def search_for_match(plc_connection, start_tag_num: int, stop_tag_num: int, last_read_datetime_from_db: datetime) -> object:
    """
    Searches for a tag that matches the last read datetime from the database.
    Returns the datetime object of the closest matching tag or None if no matches are found.
    """
    # initialize closest_datetime to None and smallest_timedelta to a large value
    closest_datetime = None
    smallest_timedelta = timedelta(days=365)  # set to 1 year initially

    for i in range(start_tag_num, stop_tag_num):
        current_read_datetime = read_tag_datetime(plc_connection=plc_connection, tag_category=TAG_CATEGORY, read_num=i)
        if LAST_READ_DATETIME_FROM_DB == current_read_datetime:
            print(f"Found a match on search all ,The value of current number  {i} is: {current_read_datetime} & it matches  = {LAST_READ_DATETIME_FROM_DB}")
            logging.info(f"Found a match on search all ,The value of current number  {i} is: {current_read_datetime} & it matches  = {LAST_READ_DATETIME_FROM_DB}")
            ### TODO: READ NUMBER return
            return current_read_datetime
        if current_read_datetime is not None:
            timedelta_to_desired = abs(current_read_datetime - last_read_datetime_from_db)
            if timedelta_to_desired < smallest_timedelta:
                closest_datetime = current_read_datetime
                smallest_timedelta = timedelta_to_desired

    print("couldn't get a match on search all , stage 3 Checking the closest date now")
    logging.info("couldn't get a match on search all , stage 3 Checking the closest date now")
    # print the closest date-time value
    if closest_datetime:
        print(f"The closest date-time value is: {closest_datetime}")
        logging.info(f"The closest date-time value is: {closest_datetime}")
    else:
        ### TODO: LOG IT !
        print("No valid date-time values were found.")
        logging.error("No valid date-time values were found.")
    ### TODO: READ NUMBER return
    return closest_datetime


# set up the connection to the PLC
with PLC() as plc:
    plc.IPAddress = PLC_IP
    connected = plc.IPAddress
    if connected:
        logging.info("Connected to the PLC")
        print("Connected to the PLC")
        logging.info("tage 1 checking if db is having same read")
        print("stage 1 checking if db is having same read")
        # Read the DateTime of TAG from the PLC
        last_value_datetime_from_PLC = read_tag_datetime(plc_connection=plc, tag_category=TAG_CATEGORY, read_num=LAST_READ_NUMBER_FROM_DB)
        if LAST_READ_DATETIME_FROM_DB == last_value_datetime_from_PLC:
            print(f"It matches the last read, the value of the last read  is {LAST_READ_DATETIME_FROM_DB} & last read number is {LAST_READ_NUMBER_FROM_DB}")
            logging.info(f"It matches the last read, the value of the last read  is {LAST_READ_DATETIME_FROM_DB} & last read number is {LAST_READ_NUMBER_FROM_DB}")
        else:
            print("Couldn't match, stage 2 searching all")
            logging.info("Couldn't match, stage 2 searching all")
            match_search = search_for_match(plc_connection=plc, start_tag_num=START_HOURLY_TAG, stop_tag_num=STOP_HOURLY_TAG, last_read_datetime_from_db=LAST_READ_DATETIME_FROM_DB)

