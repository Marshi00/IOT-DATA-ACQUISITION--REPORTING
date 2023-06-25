import pandas as pd
import utilities.connection as uc
from utilities.configs import *
import psutil


# Function to estimate the chunk size based on available memory
def estimate_chunk_size(dataframe, available_memory):
    total_memory = dataframe.memory_usage(deep=True).sum()
    if total_memory == 0:
        return len(dataframe)
    max_chunks = available_memory // total_memory
    if max_chunks == 0:
        return len(dataframe)
    chunk_size = int(len(dataframe) / max_chunks)
    return chunk_size


# function to Produce the Dimension Calendar Table
def dimension_datetime_frame(start='2020-01-01', end='2050-12-31', freq="S"):
    """ Return a ready  Dimension Calendar Table frame with precision of seconds"""
    df = pd.DataFrame({"DateTime": pd.date_range(start=start, end=end, freq=freq)})
    df["second"] = df.DateTime.dt.second
    df["minute"] = df.DateTime.dt.minute
    df["hour"] = df.DateTime.dt.hour
    df["day"] = df.DateTime.dt.day
    df["dayofweek"] = df.DateTime.dt.dayofweek
    df["is_weekend"] = df.DateTime.dt.dayofweek > 4
    df["month"] = df.DateTime.dt.month
    df["Quarter"] = df.DateTime.dt.quarter
    df["Year"] = df.DateTime.dt.year
    return df


engine = uc.mysql_connection()
if engine is not False:
    try:
        calendar_df = dimension_datetime_frame(start=PY_CALENDAR_TABLE_START, end=PY_CALENDAR_TABLE_STOP,
                                               freq=PY_CALENDAR_TABLE_FREQUENCY)
        # Estimate chunk size based on available memory
        available_memory = psutil.virtual_memory().available
        chunk_size = estimate_chunk_size(calendar_df, available_memory)
        print("starting")
        calendar_df.to_sql('dimDate', engine, chunksize=chunk_size, if_exists='replace', index=False)
        print(f"successfully created the calendar Table from {PY_CALENDAR_TABLE_START} to {PY_CALENDAR_TABLE_STOP}")
    except Exception as e:
        print(e)
        print("failed to create the table")
else:
    print("Couldn't make connection to MYSQL DB, try again in 5 min")
 