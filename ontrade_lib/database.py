from psycopg2 import connect
import os

def get_db_connection():
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_name = os.environ["DB_NAME"]
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_conn = connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    return db_conn