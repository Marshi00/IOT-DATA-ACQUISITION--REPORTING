# TODO: format & add FAST API

sql_check_query = """
SELECT tag_name, datetime_obj
FROM (
  SELECT tag_name, datetime_obj, ROW_NUMBER() OVER (PARTITION BY tag_name, datetime_obj ORDER BY id) AS row_num
  FROM raw_stream
) AS subquery
WHERE row_num > 1;
"""

sql_del_query = """
DELETE FROM raw_stream
WHERE id IN (
  SELECT id
  FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY tag_name, datetime_obj ORDER BY id) AS row_num
    FROM raw_stream
  ) AS subquery
  WHERE row_num > 1
);
"""
