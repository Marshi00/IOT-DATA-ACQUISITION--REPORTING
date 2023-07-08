import utilities.connection as uc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# TODO: add logging later
engine = uc.mysql_connection()

batch_size = 3000

try:
    while True:
        # Fetch a batch of rows
        sql_insert_query = """
            INSERT INTO historical_archive (tag_name, min, max, total, time, min_minute, max_minute, valid, datetime_obj, datetime_str, read_num)
            SELECT tag_name, min, max, total, time, min_minute, max_minute, valid, datetime_obj, datetime_str, read_num
            FROM raw_stream
            WHERE status = 'marked'
            LIMIT :batch_size 
        """
        sql_del_query = """
        DELETE FROM raw_stream WHERE status = 'marked' LIMIT :affected_rows
        """
        with engine.begin() as connection:
            try:
                result = connection.execute(text(sql_insert_query), batch_size=batch_size)
                affected_rows = result.rowcount

                # Exit the loop if no more rows are affected
                if affected_rows == 0:
                    break

                # Delete the processed rows
                connection.execute(text(sql_del_query), affected_rows=affected_rows)

            except SQLAlchemyError as e:
                # Handle the exception
                print(f"Error occurred: {str(e)}")
                connection.rollback()  # Rollback the transaction to maintain data consistency
                break

except Exception as e:
    # Handle any other exception
    print(f"Error occurred: {str(e)}")
