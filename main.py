"""
from pycomm3 import LogixDriver

# set up the driver
with LogixDriver('192.168.1.1') as plc:
    # read the tag 'MyTag' from the PLC
    tag_value = plc.read('MyTag')

    # print the value of the tag to the console
    print(f"The value of 'MyTag' is: {tag_value}")
"""
from pylogix import PLC
import pandas as pd

# set up the connection to the PLC
with PLC() as plc:
    plc.IPAddress = '192.168.0.99'
    connected = plc.IPAddress

    if connected:
        # read the tag 'MyTag' from the PLC
        response = plc.Read('report_data[0].log_data[0].min')

        # check if the read was successful
        if response.Status == "Success":
            # print the value of the tag to the console
            print(f"The value of 'MyTag' is: {response.Value}")
        else:
            print("Read failed")
    else:
        print("Could not connect to PLC")
