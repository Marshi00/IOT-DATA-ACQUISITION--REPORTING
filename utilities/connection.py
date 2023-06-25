from sqlalchemy import create_engine
from utilities.configs import *


def mysql_connection(user=MYSQL_DB_USER, password=MYSQL_DB_PASSWORD, ip=MYSQL_DB_ENDPOINT,
                     port=MYSQL_DB_PORT, db=MYSQL_DB):
    # Replace the connection parameters with your own values
    db_uri = f'mysql+mysqlconnector://{user}:{password}@{ip}:{port}/{db}'
    engine = create_engine(db_uri)

    try:
        conn = engine.connect()
        print("Successfully connected to the MySQL database")
        return engine
    except Exception as e:
        print("Error: Could not make connection to the MySQL database")
        print(e)
        return False
