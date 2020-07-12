
import psycopg2
from .db_config import host, port, database, user, password

## Database Functions start ##

def get_con_string():
    #'protocol://username:password@host:port/databse_name'
    return f"""postgresql://{user}:{password}@{host}:{port}/{database}"""

def get_engine():
    from sqlalchemy import create_engine
    return create_engine(get_con_string())

def query_db(sql, connection=None):
    if connection is None:
        connection = psycopg2.connect(get_con_string())
    cur = connection.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    del cur
    connection.close()
    return data

def get_connection(username, password, host, port, database):
    return psycopg2.connect(f"""postgresql://{user}:{password}@{host}:{port}/{database}""")

def get_class_query(classes, class_column):
    class_query = "1=1"
    if classes is not None:
        classes_string = "', '".join(classes)
        class_query = f"""
            {class_column} IN ('{classes_string}')
        """
    return class_query


def get_srid(table_name: str, geom_column='wkb_geometry'):
    sql = f"""
        SELECT FIND_SRID('public', '{table_name}', '{geom_column}') as srid;
    """
    srid = query_db(sql)[0][0]
    return srid

## Database Functions end ##