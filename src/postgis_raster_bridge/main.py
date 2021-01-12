import os
import json
import math
import time
from .database import query_db
from .database_apis import get_datatype_id, get_srid, get_datatype_info_by_id, get_feature_selection_query, \
    get_class_query, get_stat_query, get_pois_features_query, get_data_point_geojson_query
from .subroutines import register_job, project_xy, float_to_string_safe
from .config import jobs_directory, base_path, apihost

def readDataByArea(
        latU: float,
        lonU: float,
        latD: float,
        lonD: float,
        tipoDati: str,
        cellSize: float=100.0,
        positive: int=1,
        negative: int=0,
        nodata: int=254,
        out_srid: int=4326,
        circle:bool=False,
        debug: bool=False
    ):
    """
    :param latU: latitude of the point at the top left
    :param lonU: longitude of the point at the top left
    :param latD: latitude of the point at the bottom right
    :param lonD: longitude of the point at the bottom right
    :param tipoDati: 
        These are possible values:
            • attivita
            • superficieEdificato
            • superficieAreeServizio
            • struttureVarie
            • tutti
    :param cellSize: Distance between center of two pixels on ground in meters (Both in x and y direction).
    :param positive: 
    :param negative: 
    :param nodata: 
    :param out_srid: 
    :param circle: 
    :param debug: Produces Geojson in addition to the raster, the geojson's can be used to compare raster and features.
    :return: 
    """
    # Start Timer
    start = time.time()

    # Register Job
    job_id, job_path = register_job()

    # Infer Outputs to Produce
    outputs_to_produce = ['attivita', 'superficieEdificato', 'superficieAreeServizio', 'struttureVarie', 'pericolositaFrane', 'pericolositaIdraulica']
    if tipoDati != 'tutti':
        if not tipoDati in outputs_to_produce:
            raise Exception(f"Invalid value supplied for parameter tipoDati: {tipoDati}, \n Valid values are {','.join(outputs_to_produce)}")
        outputs_to_produce = [tipoDati]

    #
    # if tipoDati == 'struttureVarie':
    #     cellSize = 5

    # Json Info Files
    preprocess_json_path = os.path.join(job_path, 'parameter_info.json')
    json_path = os.path.join(job_path, 'job_info.json')
    job_lock = os.path.join(job_path, 'job_info.json.lock')

    # Lock Job
    with open(job_lock, 'w') as f:
        pass

    # Prepare template raster parameters
    x_min = lonU
    y_min = latD
    x_max = lonD
    y_max = latU
    x_min_meter, y_max_meter = project_xy(x_min, y_max, 4326, 3857)
    x_max_meter, y_min_meter = project_xy(x_max, y_min, 4326, 3857)
    cell_size_x_dd = abs((x_max - x_min)*cellSize/(x_max_meter - x_min_meter))
    cell_size_y_dd = abs((y_max - y_min)*cellSize/(y_max_meter - y_min_meter))
    _cell_size_x_dd = float_to_string_safe(cell_size_x_dd)
    _cell_size_y_dd = float_to_string_safe(cell_size_y_dd)
    # print(cell_size_x_dd, cell_size_y_dd, _cell_size_x_dd, _cell_size_y_dd)

    # Dimension of Raster
    n_rows = math.ceil((x_max - x_min) / cell_size_x_dd)
    n_cols = math.ceil((y_max - y_min) / cell_size_y_dd)

    # Rasterization template query
    raster_template_sql = f""" 
        SELECT ST_SetBandNoDataValue(ST_MakeEmptyRaster({n_cols}, {n_rows}, {x_min}, {y_max}, {_cell_size_x_dd}, -{_cell_size_y_dd}, {0}, {0}, 4326), {nodata})
    """

    # if 'struttureVarie' in outputs_to_produce:
    #     cellSize = 5
    #     cell_size_x_dd_5m = (x_max - x_min) * cellSize / (x_max_meter - x_min_meter)
    #     cell_size_y_dd_5m = (y_max - y_min) * cellSize / (y_max_meter - y_min_meter)
    #     _cell_size_x_dd_5m = float_to_string_safe(cell_size_x_dd)
    #     _cell_size_y_dd_5m = float_to_string_safe(cell_size_y_dd)
    #
    #     # Dimension of Raster
    #     n_rows_5m = math.ceil((x_max - x_min) / cell_size_x_dd_5m)
    #     n_cols_5m = math.ceil((y_max - y_min) / cell_size_y_dd_5m)
    #
    #     # Rasterization template query
    #     raster_template_sql_5m = f"""
    #         SELECT ST_SetBandNoDataValue(ST_MakeEmptyRaster({n_cols_5m}, {n_rows_5m}, {x_min}, {y_max}, {_cell_size_x_dd_5m}, {_cell_size_y_dd_5m}, {0}, {0}, 4326), {nodata})
    #     """


    # Based on input selection type create selection query
    if circle:
        x_center = (x_max + x_min) * .5
        y_center = (y_max + y_min) * .5
        radius = (abs(x_max - x_min) + abs(y_max - y_min)) * .25

        selection_geom_query = f"""
            SELECT ST_Buffer(ST_SetSRID(ST_Point({x_center}, {y_center}), 4326), {radius}) AS geom
        """
    else:  # Polygon
        selection_geom_query = f"""
            SELECT ST_GeomFromText('POLYGON(({x_min} {y_min}, {x_min} {y_max}, {x_max} {y_max}, {x_max} {y_min}, {x_min} {y_min}))', 4326) AS geom
        """

    # Selection Query and Rasterize Selection query for fishnet
    selection_query = f"""
        q AS (
            {selection_geom_query}
        )
    """
    _selection_query = selection_query
    selection_query_rasterize = f"""
        q_ras AS (
            SELECT
                ST_Union(
                    ST_AsRaster(
                        q.geom,
                        ({raster_template_sql}),
                        '16BUI'::text, 
                        {negative}, 
                        {nodata}
                    ),
                    'Max'
                ) AS ras
            FROM q
        )
    """

    # Fishnet from raster band 1
    fishnet_query = f"""
        q_poly AS (
            SELECT  
                (pp).geom AS geom
            FROM 
                (
                    SELECT ST_PixelAsPolygons(
                        q_ras.ras, 
                        1
                    ) pp
                    FROM q_ras
                ) a
        )
    """

    # Make Json Info
    json_info = {
        "jobId": job_id,
        "parameters": {
            "latU": latU,
            "lonU": lonU,
            "latD": latD,
            "lonD": lonD,
            "cellSize": cellSize,
            "tipoDati": [
                tipoDati
            ]
        },
        "tipoDati": {},
        "outputsToProduce": outputs_to_produce,
        "rasterInfoCommon": {
            "cellSizeX_Meters": cellSize,
            "cellSizeY_meters": cellSize,
            "cellSizeX_dd": cell_size_x_dd,
            "cellSizeY_dd": cell_size_y_dd,
            "Rows": n_rows,
            "Cols": n_cols
        }
        # "geotiffFile": raster_file_path,
        # #"infoFile": json_file_path,
        # "messages": [],
        # "time_taken_query": 0,
        # "time_taken_total": 0,
        # "time_units": "Seconds"
    }
    with open(preprocess_json_path, 'w') as f:
        json.dump(json_info, f)

    geojson_store = os.path.join(job_path, "geojson")
    if debug:
        os.mkdir(geojson_store)

    for _tipo_dati in outputs_to_produce:
        print("--Processing", _tipo_dati)
        tipo_dati_info = {}

        # Start Info Query timer
        start_info_query = time.time()

        # Basics
        datatype_id = get_datatype_id(_tipo_dati)
        if datatype_id == 100:
            from .database_apis import get_attiva_query, get_comune_population_query
            comune_population_query, feature_selection_query, selection_query = get_comune_population_query(selection_query)
            _layer_stats = query_db(comune_population_query, cursor_factory=None)
            comune_codes = [int(x[0]) for x in _layer_stats]
            attivita_stats_mapping = {x:[] for x in comune_codes}
            comune_codes = [str(x) for x in comune_codes]

            attivita_query = get_attiva_query(comune_codes)
            attivita_stats = query_db(attivita_query, cursor_factory=None)
            for x in attivita_stats:
                comune_code = int(x[-1])
                _row = {
                    "ateco3_code": int(x[0]),
                    "ateco3_name": x[1].strip(),
                    "num_unita": int(x[2]),
                    "addett": int(x[3])
                }
                attivita_stats_mapping[comune_code].append(_row)

            comuni_stats_store = []
            for row in _layer_stats:
                comune_code = int(row[0])
                comuni_stats = {
                    "comune_code": comune_code,
                    "comune_name": row[1].strip(),
                    "comune_area": float(row[2]),
                    "area_units": "Sq. Meter",
                    "comune_population": float(row[6]),
                    "comune_area_total": float(row[3]),
                    "comune_pop_total": float(row[7]),
                    "attivita_total": len(attivita_stats_mapping[comune_code]),
                    "attivita": attivita_stats_mapping[comune_code]
                }
                comuni_stats_store.append(comuni_stats)
            tipo_dati_info["comuni"] = comuni_stats_store
            # raise None

            # For Rasterization
            raster_band_class_codes = [1]
            n_bands = 1

        elif datatype_id in [420, 430]:
            # Pericolosita Data
            geom_column = "wkb_geometry"
            geom_column_where_query = f"ST_Intersects(qp.geom, t.{geom_column})"
            projected = False
            # if feature_table in ['osm_buildings']:
            #     geom_column_where_query = f"ST_Intersects(q.geom, ST_Centroid(t.{geom_column}))"

            if datatype_id == 420:
                feature_tables = ["isp_frane"]
                name_columns = ["peric_ita"]
            else:
                feature_tables = ["isp_idraulica_p1", "isp_idraulica_p2", "isp_idraulica_p3"]
                name_columns = ["scenariop1", "scenariop2", "scenariop3"]

            layer_info = []
            total_area = 0
            layer_id = 0
            raster_band_class_codes = []
            feature_selection_query_store = []
            for feature_table, name_column in zip(feature_tables, name_columns):
                t_srs = get_srid(feature_table, geom_column)
                selection_query = f"""
                    {_selection_query},  
                    qp AS (
                        SELECT 
                            ST_Transform(q.geom, {t_srs}) as geom
                        FROM q
                    )
                """
                _feature_selection_query = f"""       
                    SELECT
                        ST_Intersection(t.{geom_column}, qp.geom) AS geom,
                        t.{name_column} AS name,
                        t.{name_column} AS code
                    FROM                 
                        public.{feature_table} t,                
                        qp            
                    WHERE                 
                        {geom_column_where_query}  
                        AND 
                            ST_IsValid(t.{geom_column})='t'
                """
                stat_query = f"""
                    WITH 
                        {selection_query},
                        f AS ({_feature_selection_query})
                    SELECT
                        f.name as name,
                        SUM(ST_Area(f.geom)) as total_area
                    FROM f
                    GROUP BY
                        name
                """
                feature_selection_query_store.append(_feature_selection_query)
                _layer_stats = query_db(stat_query, cursor_factory=None)
                # _layer_stats = {int(x[0]): x[1:] for x in _layer_stats}

                for row in _layer_stats:
                    layer_id+=1
                    total_area+=float(row[1])
                    layer_info.append({
                        "layer_id": layer_id,
                        "layer_name": str(row[0]),
                        "table_name": feature_table,
                        "layer_area": float(row[1])
                    })
                    class_code = row[0].replace("'", "''")
                    class_code = f"'{class_code}'"
                    raster_band_class_codes.append(class_code)
            tipo_dati_info["layers"] = layer_info
            tipo_dati_info['total_area'] = total_area

            # For Rasterization
            n_bands = len(tipo_dati_info["layers"])
            if len(feature_selection_query_store) > 1:
                _feature_selection_query = \
                """
                UNION
                """.join(feature_selection_query_store)
            feature_selection_query = f"features AS ({_feature_selection_query})"

        else:
            # OSM Data
            layer_info = [dict(x) for x in get_datatype_info_by_id(datatype_id)]
            class_mapping = {x['GroupCode']:x['GroupName'] for x in layer_info}
            class_names = list(class_mapping.values())
            class_values = list(class_mapping.keys())
            table_name = layer_info[0]['TableName']
            export_area = layer_info[0]['ExportArea']
            # print(class_mapping)


            _feature_selection_query = get_feature_selection_query(
                feature_table=table_name,
                class_values=class_values,
                geom_column='wkb_geometry',
                class_column='fclass',
                code_column='code'
            )
            feature_selection_query = f"features AS ({_feature_selection_query})"

            # Stat Query
            stat_query = get_stat_query(selection_query, _feature_selection_query, export_area=export_area)
            _layer_stats = query_db(stat_query, cursor_factory=None)
            _layer_stats = {int(x[0]): x[1:] for x in _layer_stats}

            # FLush Stats to Json Info
            band_id = 0
            band_stats = []
            raster_band_class_codes = []
            for _band_info in layer_info:
                class_code = _band_info['GroupID']
                if class_code in _layer_stats:
                    band_id+=1
                    raster_band_class_codes.append(class_code)
                    band_info = {
                        "layer_id": band_id,
                        "class_code": class_code,
                        "class_name": _band_info['GroupName'],
                        "table_name": table_name,
                        "total_features": _layer_stats[class_code][0]
                    }
                    if _band_info['ExportArea']:
                        band_info['layer_area'] = _layer_stats[class_code][1]
                    band_stats.append(band_info)
            #
            tipo_dati_info["layers"] = band_stats
            n_bands = band_id

            # Pois Query
            if datatype_id == 400:
                # for i in range(json_info["tipoDati"][_tipo_dati]):
                #     json_info["tipoDati"][_tipo_dati][i]["pois"] = []
                pois_query = get_pois_features_query(selection_query, _feature_selection_query)
                pois_info = query_db(pois_query, cursor_factory=None)
                pois_store = {_band_info['GroupID']:[] for _band_info in layer_info}
                for row in pois_info:
                    pois_store[row[0]].append({
                        "lat": row[1],
                        "lon": row[2],
                        "name": row[3]
                    })
                #
                for i in range (len(tipo_dati_info["layers"])):
                    tipo_dati_info["layers"][i]['pois'] = pois_store[tipo_dati_info["layers"][i]['class_code']]

        # Stop Info Query Timer
        time_taken_info_query = time.time()-start_info_query

        union_mode = "'MAX'"
        # if datatype_id == 400:
        #     union_mode = "'SUM'"
        if n_bands > 0:
            # Create data points for each bands of each raster
            data_points_store = []
            for band_idx in range(n_bands):
                if datatype_id == 400:
                    data_point_query = f"""
                        data_point_{band_idx+1} AS (
                            SELECT
                                ST_Collect(f.geom) AS geom
                            FROM
                                features f
                            WHERE
                                f.code={raster_band_class_codes[band_idx]}
                        )
                    """
                    # data_point_query = f"""
                    #     data_point_{band_idx+1} AS (
                    #         SELECT
                    #             COUNT(f.*) as count,
                    #             X,
                    #             Y
                    #         FROM (
                    #             SELECT
                    #                 FLOOR((ST_X(f.geom) - 16.82447) / 0.0003074753466752961)::integer+1 AS X,
                    #                 FLOOR((41.145577 - ST_Y(f.geom)) / 0.00015696839984572422)::integer+1 AS Y
                    #             FROM
                    #                 features f
                    #             WHERE
                    #                 f.code={raster_band_class_codes[band_idx]}
                    #         ) f
                    #         GROUP BY
                    #             f.X, f.Y
                    #         ORDER BY count DESC
                    #     )
                    # """
                else:
                    data_point_query = f"""
                        data_point_{band_idx+1} AS (
                            WITH d_ras AS (
                                SELECT  
                                    ST_Union(
                                        ST_AsRaster(
                                            f.geom,
                                            ({raster_template_sql}),
                                            '16BUI'::text, 
                                            {positive}, 
                                            {negative},
                                            true
                                        ),
                                        {union_mode}
                                    ) AS ras
                                FROM
                                    features f
                                WHERE 
                                    f.code={raster_band_class_codes[band_idx]}
                            )
        
                            SELECT 
                                ST_Collect(ST_Centroid((pp).geom)) as geom
                            FROM
                                (
                                    SELECT 
                                        ST_PixelAsPoints(d_ras.ras, 1, True) pp
                                    FROM d_ras
                                ) a
                            WHERE
                                (pp).val!={negative}
                        )
                    """
                data_points_store.append(data_point_query)
            #
            data_points_query = "\n,".join(data_points_store)

            # Create Blank Resultant Raster
            datatype_array = ", ".join(n_bands * ["'16BUI'::text"])
            negative_val_array = n_bands * [negative]
            nodata_val_array = n_bands * [nodata]
            blank_data_raster_query = f"""
                blank_ras1 AS (
                    SELECT
						p.ras AS ras
					FROM (
						{raster_template_sql} AS ras
					) p
                ),
                blank_ras AS (
                    SELECT
                        ST_AsRaster(
                            q.geom,
                            r.ras,
                            ARRAY[{datatype_array}],
                            ARRAY{negative_val_array},
                            ARRAY{nodata_val_array}
                        ) AS ras
                    FROM
                        q,
                        blank_ras1 r
                )
            """
            # blank_data_raster_query = f"""
            #     blank_ras AS (
            #         SELECT
            # 			p.ras AS ras
            # 		FROM (
            # 			{raster_template_sql} AS ras
            # 		) p
            #     )
            # """

            # fill in the blank raster with data points
            _prameterize_blank_raster_query = 'blank_ras.ras'
            data_points_name_list = []
            for band_idx in range(0, n_bands):
                b_idx = band_idx + 1
                data_point = f"data_point_{b_idx}"
                pixel_value = positive
                if datatype_id == 400 and 1==2:
                    _prameterize_blank_raster_query = f"""
                        ST_SetValue(
                            {_prameterize_blank_raster_query},
                            {b_idx},
                            {data_point}.X,
                            {data_point}.Y,
                            {data_point}.count
                        )
                    """
                    # _prameterize_blank_raster_query = f"""
                    #     ST_SetValue(
                    #         ST_AddBand({_prameterize_blank_raster_query}, '16BUI'::text, {negative}, {nodata}),
                    #         {b_idx},
                    #         {data_point}.geom,
                    #         {pixel_value}
                    #     )
                    # """
                else:
                    _prameterize_blank_raster_query = f"""
                        ST_SetValue(
                            {_prameterize_blank_raster_query},
                            {b_idx},
                            {data_point}.geom,
                            {pixel_value}
                        )
                    """
                data_points_name_list.append(data_point)
            prameterize_blank_raster_query = f"""
                parmaterized_ras AS (
                    SELECT {_prameterize_blank_raster_query} AS ras
                    FROM blank_ras, {", ".join(data_points_name_list)}
                )
            """

            # Reproject Raster if Required
            # reproject_raster_query = """
            #     ST_Union(
            #         p.ras,
            #         'Max'
            #     )
            # """
            reproject_raster_query = """
                ST_Union(
                    p.ras,
                    'Max'
                )
            """
            if not out_srid == 4326:
                reproject_raster_query = f"""
                    ST_Transform(
                        {reproject_raster_query},
                        {out_srid}
                    )
                """

            # Make Raster Query
            out_raster_query = f"""
                WITH 
                    {selection_query},
                    {selection_query_rasterize},
                    {fishnet_query},
                    {feature_selection_query},
                    {data_points_query},
                    {blank_data_raster_query},        
                    {prameterize_blank_raster_query},
    
                out_raster AS (
                    SELECT {reproject_raster_query} AS ras
                    FROM
                        parmaterized_ras p
                )
    
                SELECT 
                    ST_AsTIFF(
                        out_raster.ras,
                        'LZW'
                    ) as outraster
                FROM out_raster
            """

            # Execute Raster Query
            start_raster_query = time.time()

            # print(out_raster_query)
            output_raster_content = query_db(out_raster_query, cursor_factory=None)[0][0]
            # print('output_raster_content', output_raster_content)
            time_taken_raster_query = time.time()-start_raster_query

            output_raster_filepath = os.path.join(job_path, f'{_tipo_dati}.tiff')
            output_raster_weburl = f"{apihost}/{base_path}/{str(job_id)}/{_tipo_dati}.tiff"

            # FLush Raster to Disk
            with open(output_raster_filepath, 'wb') as f:
                f.write(bytes(output_raster_content))
        else:
            output_raster_filepath = None#os.path.join(job_path, f'{_tipo_dati}_nodata.tiff')
            output_raster_weburl = None#f"{apihost}/{base_path}/{str(job_id)}/{_tipo_dati}_nodata.tiff"
            time_taken_raster_query = 0
            # Make NoData Raster
            # with open(output_raster_filepath, 'wb') as f:
            #     pass



        # Flush Info to Json
        json_info['tipoDati'][_tipo_dati] = {
            "outputGeotiffFile": output_raster_weburl,
            "outputGeotiffFilepath": output_raster_filepath,
            "timeTakenInfoQuery": time_taken_info_query,
            "timeTakenRasterQuery": time_taken_raster_query,
            "time_units": "Seconds",
            **tipo_dati_info,
            "rasterInfo": {
                "cellSizeX_Meters": cellSize,
                "cellSizeY_meters": cellSize,
                "cellSizeX_dd": cell_size_x_dd,
                "cellSizeY_dd": cell_size_y_dd,
                "Rows": n_rows,
                "Cols": n_cols,
                "Bands": n_bands
            }
        }

        # Additional Debug Operations
        if debug:
            print("--Producing Debugging Datasets", _tipo_dati)
            geojson_store_tipo_dati = os.path.join(geojson_store, _tipo_dati)
            os.mkdir(geojson_store_tipo_dati)
            for band_idx in range(n_bands):
                # Produce Geojson
                geojson_file = f"{band_idx+1}.geojson"
                geojson_path = os.path.join(geojson_store_tipo_dati, geojson_file)
                geojson_query = get_data_point_geojson_query(selection_query, feature_selection_query, raster_band_class_codes[band_idx])
                geojson_out = query_db(geojson_query, cursor_factory=None)[0][0]
                with open(geojson_path, 'w') as f:
                    json.dump(geojson_out, f)

    #  Flush Json Info to disk
    json_info.update({
        "time_taken_total": time.time()-start,
        "time_units": "Seconds"
    })
    with open(json_path, 'w') as f:
        json.dump(json_info, f)

    # Unlock Job
    os.remove(job_lock)

    # Leak Memory !
    #
    # Ha you know what it does
    # No ?
    # Just messing with ya
    #
    return json_info
