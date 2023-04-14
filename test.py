from datetime import datetime, timedelta
from pylogix import PLC
import pandas as pd
TAG_CATEGORY = "report_data"
START_HOURLY_TAG = 0
STOP_HOURLY_TAG = 200
### TODO: Come from DB
LAST_READ_DATETIME = datetime(2023, 1, 18, 5, 59, 39)
### TODO: Come from DB
LAST_READ_NUMBER = 123

# initialize closest_datetime to None and smallest_timedelta to a large value
closest_datetime = None
smallest_timedelta = timedelta(days=365) # set to 1 year initially


# define the function to read PLC tag values
def read_tag(plc, tag, read_num):
    response = plc.Read(f'{tag}[{read_num}].time.Year')
    year = plc.Read(f'{tag}[{read_num}].time.Year')
    month = plc.Read(f'{tag}[{read_num}].time.Month')
    day = plc.Read(f'{tag}[{read_num}].time.Day')
    hour = plc.Read(f'{tag}[{read_num}].time.Hour')
    minute = plc.Read(f'{tag}[{read_num}].time.Minute')
    second = plc.Read(f'{tag}[{read_num}].time.Second')
    if response.Status == "Success":
        datetime_str = f"{year.Value}-{month.Value}-{day.Value} {hour.Value}:{minute.Value}:{second.Value}"
        if year.Value != 0 and month.Value != 0 and day.Value != 0:
            try:
                datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                return datetime_obj
            except:
                print(f"Invalid date-time value for {read_num} : {datetime_str}")
                return None
    else:
        print(f"Read failed for tag {tag} at number {read_num}")
        return None


# set up the connection to the PLC
with PLC() as plc:
    plc.IPAddress = '192.168.0.99'
    connected = plc.IPAddress
    if connected:
        print("stage 1 checking if db is having same read")
        # read the tag 'MyTag' from the PLC
        response = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Year')
        year = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Year')
        month = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Month')
        day = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Day')
        hour = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Hour')
        minute = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Minute')
        second = plc.Read(f'{TAG_CATEGORY}[{LAST_READ_NUMBER}].time.Second')
        last_value_datetime_str = f"{year.Value}-{month.Value}-{day.Value} {hour.Value}:{minute.Value}:{second.Value}"
        if response.Status == "Success":
            if year.Value != 0 and month.Value != 0 and day.Value != 0:
                last_value_datetime = datetime.strptime(last_value_datetime_str, '%Y-%m-%d %H:%M:%S')
                if LAST_READ_DATETIME == last_value_datetime:
                    print(f"it matches the last read, the value of the last read  is {LAST_READ_DATETIME} & last read number is {LAST_READ_NUMBER}")
                else:
                    print("Couldn't match, stage 2 searching all")
                    for i in range(START_HOURLY_TAG, STOP_HOURLY_TAG):
                        # read the tag 'MyTag' from the PLC
                        response = plc.Read(f'{TAG_CATEGORY}[{i}].time.Year')
                        year = plc.Read(f'{TAG_CATEGORY}[{i}].time.Year')
                        month = plc.Read(f'{TAG_CATEGORY}[{i}].time.Month')
                        day = plc.Read(f'{TAG_CATEGORY}[{i}].time.Day')
                        hour = plc.Read(f'{TAG_CATEGORY}[{i}].time.Hour')
                        minute = plc.Read(f'{TAG_CATEGORY}[{i}].time.Minute')
                        second = plc.Read(f'{TAG_CATEGORY}[{i}].time.Second')

                        # check if the read was successful
                        if response.Status == "Success":
                            # handle zero values
                            current_read_datetime_str = f"{year.Value}-{month.Value}-{day.Value} {hour.Value}:{minute.Value}:{second.Value}"
                            if year.Value == 0 or month.Value == 0 or day.Value == 0:
                                print(f"Invalid date-time value for {i} : {current_read_datetime_str}")
                            else:
                                # convert the read values into a datetime object
                                current_read_datetime = datetime.strptime(current_read_datetime_str, '%Y-%m-%d %H:%M:%S')
                                # print the value of the tag to the console
                                print(f"The value of {i} is: {current_read_datetime}")
                                # store the current_read_datetime value in a database
                                if LAST_READ_DATETIME == current_read_datetime:
                                    print(f" we found a match on search all ,The value of current number  {i} is: {current_read_datetime} & it matches  = {LAST_READ_DATETIME}")
                                    break
                                ### TODO: db.insert(current_read_datetime)
                                # db.insert(current_read_datetime)
                                timedelta_to_desired = abs(current_read_datetime - LAST_READ_DATETIME)
                                # update closest_datetime and smallest_timedelta if this tag is closer to the desired_datetime
                                ### TODO: this is checking vs magnitude of the difference and not whether it is positive or negative. may cause errors
                                if timedelta_to_desired < smallest_timedelta:
                                    closest_datetime = current_read_datetime
                                    smallest_timedelta = timedelta_to_desired
                        else:
                            ### TODO: log it
                            print(f"Read failed {i}")
                    print("couldn't get a match on search all , stage 3 Checking the closest date now")
                    # print the closest date-time value
                    if closest_datetime:
                        print(f"The closest date-time value is: {closest_datetime}")
                    else:
                        print("No valid date-time values were found.")
        else:
            ### TODO log it
            print(f"Read failed at {LAST_READ_NUMBER}")
    else:
        print("Could not connect to PLC")
