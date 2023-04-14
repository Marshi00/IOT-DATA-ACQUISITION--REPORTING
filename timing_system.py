from datetime import datetime, timedelta
from pylogix import PLC

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

# define the function to search for a match in PLC tag values
def search_tag(plc, tag, start_num, stop_num, last_datetime):
    closest_datetime = None
    smallest_timedelta = timedelta(days=365)
    for i in range(start_num, stop_num):
        current_datetime = read_tag(plc, tag, i)
        if current_datetime is not None:
            if last_datetime == current_datetime:
                print(f"We found a match at number {i}")
                return current_datetime
            timedelta_to_desired = abs(current_datetime - last_datetime)
            if timedelta_to_desired < smallest_timedelta:
                closest_datetime = current_datetime
                smallest_timedelta = timedelta_to_desired
    if closest_datetime is not None:
        print(f"Closest datetime value is {closest_datetime}")
        return closest_datetime
    else:
        print("No valid datetime values found.")
        return None

# define the main function to run the script
def main(ip_address, tag_category, start_num, stop_num, last_read_datetime, last_read_num):
    with PLC() as plc:
        plc.IPAddress = ip_address
        connected = plc.IPAddress
        if connected:
            print("Stage 1: Checking if DB has same read")
            last_value_datetime = read_tag(plc, tag_category, last_read_num)
            if last_value_datetime is not None:
                if last_read_datetime == last_value_datetime:
                    print(f"Match found for last read: {last_read_datetime}, number: {last_read_num}")
                else:
                    print("Stage 2: Searching all")
                    closest_datetime = search_tag(plc, tag_category, start_num, stop_num, last_read_datetime)
                    if closest_datetime is not None:
                        ### TODO: db.insert(closest_datetime)
                        print(f"Inserted closest datetime value {closest_datetime} into DB")
            else:
                print(f"Failed to read tag {tag_category} at number {last_read_num}")
        else:
            print("Could not connect to PLC")

# set the input values
ip_address = '192.168.0.99'
tag_category = "report_data"
start_num = 0
stop_num = 200
last_read_datetime = datetime(2023, 1, 18, 5, 59, 39)
last_read_num = 123

# run the main function
main(ip_address, tag_category, start_num, stop_num, last_read_datetime, last_read_num)
