import utilities.connection as uc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# TODO: add logging later
engine = uc.mysql_connection()

batch_size = 3000

try:
    while True:
        # Fetch a batch of rows
        sql_upsert_query = """
            INSERT INTO `fact_stream` (`tag_id`, `date_key`, `min`, `max`, `total`, `time`, `min_minute`, `max_minute`, `valid`)
            SELECT dt.`tag_id`, dd.`DateTime`, rs.`min`, rs.`max`, rs.`total`, rs.`time`, rs.`min_minute`, rs.`max_minute`, rs.`valid`
            FROM (
                SELECT *
                FROM `raw_stream`
                WHERE `status` IS NULL
                LIMIT :batch_size
            ) rs
            INNER JOIN `dim_tag` dt ON rs.`tag_name` = dt.`tag_name`
            INNER JOIN `dimdate` dd ON rs.`datetime_obj` = dd.`DateTime`
            ON DUPLICATE KEY UPDATE
                `min` = VALUES(`min`),
                `max` = VALUES(`max`),
                `total` = VALUES(`total`),
                `time` = VALUES(`time`),
                `min_minute` = VALUES(`min_minute`),
                `max_minute` = VALUES(`max_minute`),
                `valid` = VALUES(`valid`);

        """
        sql_update_query = """        
            UPDATE raw_stream SET status = 'marked' WHERE id IN (
            SELECT rs.id
            FROM (
                SELECT id
                FROM `raw_stream`
                WHERE `status` IS NULL
                LIMIT :batch_size
            ) rs
            INNER JOIN `dim_tag` dt ON rs.`tag_name` = dt.`tag_name`
            INNER JOIN `dimdate` dd ON rs.`datetime_obj` = dd.`DateTime`
            )
        """

        with engine.begin() as connection:
            try:
                result = connection.execute(text(sql_upsert_query), batch_size=batch_size)
                affected_rows = result.rowcount

                # Exit the loop if no more rows are affected
                if affected_rows == 0:
                    break

                # update status for batches
                connection.execute(text(sql_update_query), batch_size=batch_size)

            except SQLAlchemyError as e:
                # Handle the exception
                print(f"Error occurred: {str(e)}")
                connection.rollback()  # Rollback the transaction to maintain data consistency
                break

except Exception as e:
    # Handle any other exception
    print(f"Error occurred: {str(e)}")
