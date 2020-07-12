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

## Database APIs End ##