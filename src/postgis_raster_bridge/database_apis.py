from .database import query_db

## Database APIs Start ##

def get_datatype_id(datatype_name):
    query = f"""
        SELECT ws_datatype."DataTypeID" as data_type_id
        FROM public.ws_datatype
        WHERE ws_datatype."DataTypeName" = '{datatype_name}'
    """
    rows = query_db(query)
    if len(rows) > 0:
        return rows[0]['data_type_id']
    raise Exception(f"The supplied datatype_name: '{datatype_name}' could not be found in the database.")

def get_datatype_info_by_id(datatype_id):
    query = f"""
        SELECT 
            dtg."GroupID",
            dtg."ExportGeoTIFF",
            dtg."ExportJson",
            dtg."ExportArea",
            wtg."TableID",
            wtg."GroupCode",
            wtg."GroupName",
            wt."TableName"
        FROM
            public.ws_datatype_table_groups dtg
            LEFT JOIN
                public.ws_table_groups wtg
                ON
                    dtg."GroupID" = wtg."GroupID"
            LEFT JOIN
                public.ws_tables wt
                ON
                    wt."TableID" = wtg."TableID"
        WHERE
            dtg."DataTypeID" = {datatype_id}
    """
    return query_db(query)

def get_srid(table_name: str, geom_column='wkb_geometry'):
    sql = f"""
        SELECT FIND_SRID('public', '{table_name}', '{geom_column}') as srid;
    """
    srid = query_db(sql)[0]['srid']
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

def get_feature_selection_query(feature_table, classes, geom_column, class_column, code_column):
    geom_column_where_query = f"ST_Intersects(q.geom, t.{geom_column})"
    if feature_table in ['osm_buildings']:
        geom_column_where_query = f"ST_Intersects(q.geom, ST_Centroid(t.{geom_column}))"
    features_selection_query = f"""       
        SELECT
            t.{geom_column} AS geom,
            t.{class_column} AS class,
            t.{code_column} AS code
        FROM                 
            public.{feature_table} t,                
            q            
        WHERE                 
            {geom_column_where_query}  
    """
    class_query = get_class_query(classes, class_column)
    features_selection_query = f"""    
        {features_selection_query}
            AND                
            {class_query}
    """
    return features_selection_query
## Database APIs End ##