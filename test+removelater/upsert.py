import mysql.connector
import pandas as pd
import utilities.connection as uc

engine = uc.mysql_connection()

# transactional
with engine.begin() as connection:#  the connection is now in a transactional state. This means that any subsequent
    # queries or operations performed with this connection will be part of a single transaction, allowing you
    # to commit or rollback the changes as needed.
    connection.execute("INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)",
                       value1, value2, ...)
    # Perform other operations within the transaction
    connection.execute("UPDATE ...")
    connection.execute("DELETE ...")
    # Commit the transaction explicitly
    connection.commit()



# Single
connection = engine.connect() # 1 active connection to the database. You can use this connection to execute SQL
# queries and interact with the database.
connection.execute("INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)",
                   value1, value2, ...)
# Perform other operations using the connection
connection.execute("SELECT ...")
connection.execute("UPDATE ...")
connection.execute("DELETE ...")
connection.close()  # Close the connection when finished

if engine is not False:
    try:
        calendar_df = dimension_datetime_frame(start=PY_CALENDAR_TABLE_START, end=PY_CALENDAR_TABLE_STOP,
                                               freq=PY_CALENDAR_TABLE_FREQUENCY)

        print("starting")
        with engine.begin() as connection:
            for i in range(0, len(calendar_df), CHUNK_SIZE):
                chunk = calendar_df[i:i + CHUNK_SIZE]
                insert_query = """
                INSERT INTO dimDate (date_column1, date_column2, ...)
                VALUES {}
                ON DUPLICATE KEY UPDATE
                  date_column1 = VALUES(date_column1),
                  date_column2 = VALUES(date_column2),
                  ...
                """.format(','.join(['%s'] * len(chunk.columns)))
                values = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
                connection.execute(insert_query, values)
                print(f"chunk from {i} to {i + CHUNK_SIZE} done")
        print(f"successfully created the calendar Table from {PY_CALENDAR_TABLE_START} to {PY_CALENDAR_TABLE_STOP}")
    except Exception as e:
        print(e)
        print("failed to create the table")
else:
    print("Couldn't make connection to MYSQL DB, try again in 5 min")