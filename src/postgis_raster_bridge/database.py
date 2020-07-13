
import psycopg2
from psycopg2.extras import RealDictCursor
from .db_config import host, port, database, user, password

## Database Functions start ##

def get_con_string():
    #'protocol://username:password@host:port/databse_name'
    return f"""postgresql://{user}:{password}@{host}:{port}/{database}"""

def get_engine():
    from sqlalchemy import create_engine
    return create_engine(get_con_string())

def query_db(sql, connection=None, cursor_factory=RealDictCursor):
    if connection is None:
        connection = psycopg2.connect(get_con_string())
    cur = connection.cursor(cursor_factory=cursor_factory)
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    del cur
    connection.close()
    return data

def get_connection(username, password, host, port, database):
    return psycopg2.connect(f"""postgresql://{user}:{password}@{host}:{port}/{database}""")

## Database Functions end ##