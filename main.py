from fastapi import FastAPI
import utilities.connection as uc
import sql_get_queries.get_tag_mapping as get_tag_mapping


app = FastAPI()
engine = uc.mysql_connection()


@app.get("/api/get/tag_mapping")
def get_tag_mapping():
    response = get_tag_mapping.get_tag_mapping(engine)
    return response


@app.get("/api/data")
def get_data(group: str = None, limit: int = None):
    if group is None or limit is None:
        return {"error": "Both 'group' and 'limit' parameters are required."}

    # Use the `group` and `limit` inputs in your logic
    # For example, retrieve data for the specified group and limit the number of results

    # Return the response based on the inputs
    return {"group": group, "limit": limit}


"""
from pycomm3 import LogixDriver

# set up the driver
with LogixDriver('192.168.1.1') as plc:
    # read the tag 'MyTag' from the PLC
    tag_value = plc.read('MyTag')

    # print the value of the tag to the console
    print(f"The value of 'MyTag' is: {tag_value}")
"""
"""
from pylogix import PLC
import pandas as pd

# set up the connection to the PLC
with PLC() as plc:
    plc.IPAddress = '192.168.0.31'
    connected = plc.IPAddress

    if connected:
        # read the tag 'MyTag' from the PLC
        response = plc.Read('report_data[0].log_data[0].total')

        # check if the read was successful
        if response.Status == "Success":
            # print the value of the tag to the console
            print(f"The value of 'MyTag' is: {response.Value}")
        else:
            print("Read failed")
    else:
        print("Could not connect to PLC")
"""
