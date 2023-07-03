from datetime import datetime, timedelta
import logging
from utilities.configs import *

# Set up logging
# TODO: Dynamic daily log text + in spec folder
logging.basicConfig(filename='log.txt', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


# function 0
# define the function to find the reference point
def find_stop_datetime(plc_connection, tag_category: str, start_point: int) -> int:
    """
        Reads the DateTime of a tag from the PLC.
        finds the refrence stop point, for optimized speed and performance it picks up where it left.
    """
    if not isinstance(tag_category, str):
        raise ValueError("Tag category must be a string")
    # optimized to read from where it left on DB and stop once it finds the target rather loop from start
    for point in range(start_point, start_point + STOP_HOURLY_ARCHIVE + 1):
        # Construct tag path
        target = point % (STOP_HOURLY_ARCHIVE + 1)
        print(f"target at search is {target}, the current point is {point}")
        tag_path = f'{tag_category}[{target}].time.'

        # Read all values for tag
        test_conn = plc_connection.Read(tag_path + 'year')
        response = plc_connection.Read

        if test_conn.Status != "Success":
            # TODO: LOG IT !
            print(f"Read failed for tag {tag_category} at number {point}")
            logging.error(f"Read failed for tag {tag_category} at number {point}")
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
            print(f"Fount stop point here , at target {target}, at point{point}")
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
    global is_matched
    """
    Searches for a tag that matches the last read datetime from the database.
    Returns the datetime object of the closest matching tag or None if no matches are found.
    """
    # initialize closest_datetime to None and smallest_timedelta to a large value
    closest_datetime = None
    smallest_timedelta = [timedelta(days=365)]  # set to 1 year initially

    for num in range(start_tag_num, stop_tag_num):
        current_read_values = read_tag_datetime(plc_connection=plc_connection, tag_category=TAG_CATEGORY, read_num=num)
        current_read_datetime = current_read_values["datetime_obj"]
        if last_read_datetime_from_db == current_read_datetime:
            is_matched = 1
            print(
                f"Found a match on search all ,The value of current number  {num} is: {current_read_datetime} "
                f"& it matches  = {last_read_datetime_from_db}")
            logging.info(
                f"Found a match on search all ,The value of current number  {num} is: {current_read_datetime} "
                f"& it matches  = {last_read_datetime_from_db}")
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
    is_matched = 0
    return closest_read_values


# function 3
def check_last_read(plc_connection, tag_category: str, last_read_number_from_db: int, last_read_datetime_from_db,
                    start_hourly_archive: int, stop_hourly_archive: int) -> object:
    """
    :param plc_connection: connection of PLC
    :param tag_category: the category of tag
    :param last_read_number_from_db: last record coming from db
    :param last_read_datetime_from_db: time of last record coming from db
    :param start_hourly_archive: lower bound of  archive records ( mostly 0 )
    :param stop_hourly_archive: upper bound of archive records
    :return: The attributes of last read if matching with DB, if not search  and find either a match or closest match,
    it will also affect the global variable is_match if it finds a match
    """
    global is_matched
    logging.info("tage 1 checking if db is having same read")
    print("stage 1 checking if db is having same read")

    last_values_from_plc = read_tag_datetime(plc_connection=plc_connection, tag_category=tag_category,
                                             read_num=last_read_number_from_db)
    last_value_datetime_from_plc = last_values_from_plc["datetime_obj"]

    if last_read_datetime_from_db == last_value_datetime_from_plc:
        is_matched = 1
        print(
            f"It matches the last read, the value of the last read is {last_read_datetime_from_db} "
            f"& last read number is {last_read_number_from_db}")
        logging.info(
            f"It matches the last read, the value of the last read is {last_read_datetime_from_db} "
            f"& last read number is {last_read_number_from_db}")
        return last_values_from_plc
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
        # raise Exception(f"Read failed for tag {tag_category} at number {read_num} at record {record_num}")

    # Extract record values
    record_min = response(tag_path + 'MIN').Value
    record_max = response(tag_path + 'MAX').Value
    record_total = response(tag_path + 'TOTAL').Value
    record_time = response(tag_path + 'TIME').Value
    record_min_minute = response(tag_path + 'MINMINUTE').Value
    record_max_minute = response(tag_path + 'MAXMINUTE').Value
    record_valid = response(tag_path + 'VALID').Value

    # Store the extracted values in a dictionary
    record_dict = {
        'tag_name': f'{tag_category}[{read_num}].log_data[{record_num}]',
        'min': record_min,
        'max': record_max,
        'total': record_total,
        'time': record_time,
        'min_minute': record_min_minute,
        'max_minute': record_max_minute,
        'valid': record_valid,
    }

    dt_values = read_tag_datetime(plc_connection=plc_connection, tag_category=tag_category,
                                  read_num=read_num)
    if dt_values is None:
        print(f"Read failed on DT_values = {dt_values}for min value test, tag {tag_category} at number {read_num}"
              f" at record {record_num}")
        logging.error(f"Read failed DT_values = {dt_values} for min value test, tag {tag_category} at number {read_num}"
                      f" at record {record_num}")
        return None
    record_dict.update(dt_values)
    # TODO: REMOVE AFTER DEBUG
    print(record_dict)
    return record_dict
