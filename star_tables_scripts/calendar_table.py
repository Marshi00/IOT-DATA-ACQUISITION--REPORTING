import pandas as pd
import utilities.connection as uc

PY_CALENDAR_TABLE_START = '2020-01-01'
PY_CALENDAR_TABLE_STOP = '2050-01-01'
PY_CALENDAR_TABLE_FREQUENCY = "T"
CHUNK_SIZE = 86400


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

        print("starting")
        with engine.begin() as connection:
            for i in range(0, len(calendar_df), CHUNK_SIZE):
                chunk = calendar_df[i:i + CHUNK_SIZE]
                chunk.to_sql('dimDate', connection, if_exists='append', index=False)
                print(f"chunk from {i} to {i + CHUNK_SIZE} done")
        print(f"successfully created the calendar Table from {PY_CALENDAR_TABLE_START} to {PY_CALENDAR_TABLE_STOP}")
    except Exception as e:
        print(e)
        print("failed to create the table")
else:
    print("Couldn't make connection to MYSQL DB, try again in 5 min")
