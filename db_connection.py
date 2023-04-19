#### BATCH OF INSERTS !!!!!
from sqlalchemy import create_engine

DB_ENDPOINT = "127.0.0.1"
DB = 'scada_iot'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_PORT = '3306'
# Replace the connection parameters with your own values
db_uri = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_ENDPOINT}:{DB_PORT}/{DB}'
engine = create_engine(db_uri)

try:
    conn = engine.connect()
    print("Successfully connected to the MySQL database")

except Exception as e:
    print("Error: Could not make connection to the MySQL database")
    print(e)
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Define the database connection parameters
engine = create_engine('mysql+pymysql://user:password@localhost/mydatabase')
Session = sessionmaker(bind=engine)
session = Session()

# Create the DataFrame with your measurement data
df = pd.DataFrame({
    'TAG': ['my_tag', 'another_tag'],
    'time stamp': ['2023-04-19 12:34:56', '2023-04-19 12:35:56'],
    'min': [10, 15],
    'max': [20, 25],
    'total': [30, 35],
    'min_minutes': [40, 45],
    'max_mintes_valid': [50, 55]
})

# Map the TAG values to their corresponding ids in the tags table
tags = pd.read_sql_query('SELECT id, tag FROM tags', engine)
df = df.merge(tags, on='TAG', how='left')
df = df.drop(columns=['TAG'])

# Set the datetime column to a pandas datetime object
df['time stamp'] = pd.to_datetime(df['time stamp'])

# Convert the DataFrame to a list of dictionaries
data = df.to_dict(orient='records')

# Insert the data into the measurements table
session.bulk_insert_mappings(Measurement, data)
session.commit()

# Close the database session
session.close()

"""
"""
import pymysql
import pandas as pd

# Establish a connection to the database
conn = pymysql.connect(host='localhost', user='user', password='password', db='mydatabase')

# Create the DataFrame with your measurement data
df = pd.DataFrame({
    'TAG': ['my_tag', 'another_tag'],
    'time stamp': ['2023-04-19 12:34:56', '2023-04-19 12:35:56'],
    'min': [10, 15],
    'max': [20, 25],
    'total': [30, 35],
    'min_minutes': [40, 45],
    'max_mintes_valid': [50, 55]
})

# Map the TAG values to their corresponding ids in the tags table
tags = pd.read_sql_query('SELECT id, tag FROM tags', conn)
df = df.merge(tags, on='TAG', how='left')
df = df.drop(columns=['TAG'])

# Set the datetime column to a pandas datetime object
df['time stamp'] = pd.to_datetime(df['time stamp'])

# Insert the data into the measurements table
df.to_sql(name='measurements', con=conn, if_exists='append', index=False)

# Close the database connection
conn.close()

"""
"""
TABLE = "payment"
FIELDS = ""(payment_id INTEGER PRIMARY KEY, 
                    date DATE, 
                    amount MONEY, 
                    rider_id INTEGER);""
try:    
        cursor.execute("DROP TABLE IF EXISTS {0};".format(TABLE))
        query = f"CREATE TABLE {TABLE} "
        query = query + FIELDS
        cursor.execute(query)
        print("Finished creating table {0}".format(TABLE))
except psycopg2.Error as e: 
    print(f"Error: Couldn't recreate the table: {TABLE}, something went wrong")
    print(e)
"""
""" 
try:    
        query = f"SELECT Count (*) FROM  riders"
        cursor.execute(query)
except psycopg2.Error as e: 
    print(e)
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
"""
"""
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", "SAM", 2000, ["Rubber Soul", "Let it be"]))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
"""