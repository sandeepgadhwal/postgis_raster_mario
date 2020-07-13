from .database import query_db

## Database APIs Start ##

def get_datatype_id(datatype_name):
    query = f"""
        SELECT ws_datatype."DataTypeID"
        FROM public.ws_datatype
        WHERE ws_datatype."DataTypeName" = '{datatype_name}'
    """
    rows = query_db(query)
    if len(rows) > 0:
        return rows[0][0]
    raise Exception(f"The supplied datatype_name: '{datatype_name}' could not be found in the database.")

def get_srid(table_name: str, geom_column='wkb_geometry'):
    sql = f"""
        SELECT FIND_SRID('public', '{table_name}', '{geom_column}') as srid;
    """
    srid = query_db(sql)[0][0]
    return srid

def get_class_query(classes, class_column):
    class_query = "1=1"
    if classes is not None:
        classes_string = "', '".join(classes)
        class_query = f"""
            {class_column} IN ('{classes_string}')
        """
    return class_query

def get_table_names(filter_table_names=None, connection=None):
    query = """
        SELECT 
            table_name
        FROM 
            information_schema.tables
        WHERE 
            table_schema = 'public'
    """
    if filter_table_names is not None:
        filter_table_names = "', '".join(filter_table_names)
        filter_table_names = f"table_name IN ('{filter_table_names}')"
        query += f"""
            AND 
                {filter_table_names}
        """
    table_names = [x[0] for x in query_db(query)]
    return table_names

## Database APIs End ##