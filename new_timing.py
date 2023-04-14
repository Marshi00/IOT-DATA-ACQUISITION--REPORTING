from datetime import datetime, timedelta
from pylogix import PLC
import pandas as pd

def get_datetime_from_tag(plc, tag):
    year = plc.Read(f'{tag}.time.Year')
    month = plc.Read(f'{tag}.time.Month')
    day = plc.Read(f'{tag}.time.Day')
    hour = plc.Read(f'{tag}.time.Hour')
    minute = plc.Read(f'{tag}.time.Minute')
    second = plc.Read(f'{tag}.time.Second')
    if year.Value == 0 or month.Value == 0 or day.Value == 0:
        return None
    else:
        datetime_str = f"{year.Value}-{month.Value}-{day.Value} {hour.Value}:{minute.Value}:{second.Value}"
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

def read_tag_and_compare(plc, tag, last_read_datetime):
    response = plc.Read(f'{tag}.time.Year')
    if response.Status != "Success":
        return False
    tag_datetime = get_datetime_from_tag(plc, tag)
    if tag_datetime is None:
        return False
    if tag_datetime == last_read_datetime:
        return True
    timedelta_to_desired = abs(tag_datetime - last_read_datetime)
    return timedelta_to_desired < smallest_timedelta

def find_closest_datetime(plc, start_tag, stop_tag, last_read_datetime):
    closest_datetime = None
    smallest_timedelta = timedelta(days=365) # set to 1 year initially
    for tag in range(start_tag, stop_tag):
        if read_tag_and_compare(plc, f'report_data[{tag}]', last_read_datetime):
            return last_read_datetime
        tag_datetime = get_datetime_from_tag(plc, f'report_data[{tag}]')
        if tag_datetime is None:
            continue
        timedelta_to_desired = abs(tag_datetime - last_read_datetime)
        if timedelta_to_desired < smallest_timedelta:
            closest_datetime = tag_datetime
            smallest_timedelta = timedelta_to_desired
    return closest_datetime

def main():
    START_HOURLY_TAG = 0
    STOP_HOURLY_TAG = 200
    ### TODO: Come from DB
    LAST_READ_DATETIME = datetime(2023, 1, 18, 5, 59, 39)
    ### TODO: Come from DB
    LAST_READ_NUMBER = 123

    # set up the connection to the PLC
    with PLC() as plc:
        plc.IPAddress = '192.168.0.99'
        connected = plc.IPAddress
        if connected:
            if read_tag_and_compare(plc, f'report_data[{LAST_READ_NUMBER}]', LAST_READ_DATETIME):
                print("it matches the last read")
            else:
                closest_datetime = find_closest_datetime(plc, START_HOURLY_TAG, STOP_HOURLY_TAG, LAST_READ_DATETIME)
                if closest_datetime:
                    print(f"The closest date-time value is: {closest_datetime}")
                else:
                    print("No valid date-time values were found.")
        else:
            print("Could not connect to PLC")

if __name__ == '__main__':
    main()
