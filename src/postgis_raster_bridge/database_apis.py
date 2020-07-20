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
            TRIM(wtg."GroupName") AS "GroupName",
            TRIM(wt."TableName") AS "TableName"
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
        if type(classes) in [list, tuple]:
            classes_string = "', '".join(classes)
        else:
            classes_string = classes
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

def get_class_code_query(classes, class_column):
    class_query = "1=1"
    if classes is not None:
        if type(classes) in [list, tuple]:
            classes_string = ", ".join([str(x) for x in classes])
            class_query = f"""
                {class_column} IN ({classes_string})
            """
        else:
            classes_string = str(classes)
            class_query = f"""
                {class_column}={classes_string}
            """
    return class_query

def get_feature_selection_query(feature_table, class_values, geom_column='wkb_geometry', class_column='fclass', code_column='code', name_column='name'):
    geom_column_where_query = f"ST_Intersects(q.geom, t.{geom_column})"
    # if feature_table in ['osm_buildings']:
    #     geom_column_where_query = f"ST_Intersects(q.geom, ST_Centroid(t.{geom_column}))"
    features_selection_query = f"""       
        SELECT
            t.{geom_column} AS geom,
            t.{class_column} AS class,
            t.{code_column} AS code,
            t.{name_column} AS name
        FROM                 
            public.{feature_table} t,                
            q            
        WHERE                 
            {geom_column_where_query}  
    """
    class_query = get_class_code_query(class_values, code_column)
    features_selection_query = f"""    
        {features_selection_query}
            AND                
            {class_query}
    """
    return features_selection_query

def get_stat_query(selection_query, feature_selection_query, export_area=False):
    additional = ""
    if export_area:
        additional+=",SUM(ST_Area(ST_Transform(f.geom, 3857))) as total_area"
    query = f"""
        WITH 
            {selection_query},
            f AS ({feature_selection_query})
        SELECT
            f.code as code,
            count(f.*) as total_count
            {additional}
        FROM f
        GROUP BY
            f.code
    """
    return query

def get_pois_features_query(selection_query, feature_selection_query):
    query = f"""
        WITH 
            {selection_query},
            f AS ({feature_selection_query})
        SELECT 
            f.code as code,
            ST_Y(ST_Centroid(f.geom)) as lat,
            ST_X(ST_Centroid(f.geom)) as lon,
            f.name as name
        FROM 
            f
        ORDER BY
            code,
            name
    """
    return query

def get_data_point_geojson_query(selection_query, feature_selection_query, raster_band_class_code):
    query = f"""
        WITH 
            {selection_query},
            {feature_selection_query},
            class_features AS (
                SELECT 
                    row_number() over () AS gid,
                    f.*
                FROM features f
                WHERE 
                    f.code = {raster_band_class_code}
            )
        
        SELECT jsonb_build_object(
            'type',     'FeatureCollection',
            'features', jsonb_agg(feature)
        )
        FROM (
            SELECT 
                jsonb_build_object(
                    'type',       'Feature',
                    'id',         gid,
                    'geometry',   ST_AsGeoJSON(geom)::jsonb,
                    'properties', to_jsonb(row) - 'gid' - 'geom'
                ) AS feature
            FROM (SELECT * FROM class_features) row
        ) features;
    """
    return query

def get_comune_population_query(selection_query):
    geom_column = 'wkb_geometry'
    t_srs = get_srid("ist_comuni", geom_column)
    geom_column_where_query = f"ST_Intersects(qp.geom, ic.{geom_column})"
    _feature_selection_query = f"""
        SELECT 
            ic.pro_com as pro_com,
            ic.comune as comune,
            ST_Area(ST_Intersection(ic.{geom_column}, qp.geom)) as area,
            ST_Area(ic.wkb_geometry) as total_area,
            ic.wkb_geometry as geom,
            1 AS code
        FROM                 
            public.ist_comuni ic,                
            qp
        WHERE                 
            {geom_column_where_query}
    """
    feature_selection_query = f"features AS ({_feature_selection_query})"
    localita_selection_query = f"""
        SELECT 
            il.pro_com as pro_com,
            il.popres as popres,
            ST_Area(ST_Intersection(il.{geom_column}, qp.geom)) as area,
            ST_Area(il.{geom_column}) as total_area
        FROM                 
            public.ist_localita il,                
            qp            
        WHERE
            ST_Intersects(qp.geom, il.{geom_column})
    """
    selection_query = f"""
        {selection_query},  
        qp AS (
            SELECT 
                ST_Transform(q.geom, {t_srs}) as geom
            FROM q
        )
    """
    comune_population_query = f"""
        WITH {selection_query},
        {feature_selection_query},
        il AS ({localita_selection_query})
        SELECT 
            ic.pro_com as pro_com,
            ic.comune as comune,
            SUM(ic.area) as area_ic,
            SUM(ic.total_area) AS total_area_ic,
            SUM(il.area) as area_il,
            SUM(il.total_area) as total_area_il,
            SUM(il.popres * il.area / il.total_area) as popres,
            SUM(il.popres) as popres_total
        FROM features ic
        LEFT JOIN 
            il
                ON
                ic.pro_com = il.pro_com
        GROUP BY
            ic.pro_com,
            ic.comune                
    """
    return comune_population_query, feature_selection_query, selection_query

def get_attiva_query(comune_codes):
    attivita_query = f"""
        SELECT 
            iate.cod_ateco3 AS ateco3_code,
            iate.des_ateco3 AS ateco3_name,
            iatt.num_unita AS num_unita,
            iatt.addetti AS addett,
            iatt.pro_com AS pro_com
        FROM  
            ist_ateco3 iate
            LEFT JOIN 
                ist_attecon iatt
                    ON
                    iatt.ateco3 = iate.cod_ateco3
        WHERE 
            iatt.pro_com IN ({(", ").join(comune_codes)})
    """
    return attivita_query

## Database APIs End ##