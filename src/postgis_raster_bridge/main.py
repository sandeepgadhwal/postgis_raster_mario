import os
import json
import math
import time
from .database import query_db
from .database_apis import get_datatype_id, get_srid, get_datatype_info_by_id, get_feature_selection_query, get_class_query
from .subroutines import register_job, project_xy
from .config import jobs_directory, base_path

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
        expand_bands: bool=True
    ):
    """
    :param latU: latitude of the point at the top left
    :param lonU: longitude of the point at the top left
    :param latD: latitude of the point at the bottom right
    :param lonD: longitude of the point at the bottom right
    :param cellSize: Distance between center of two pixels on ground in meters (Both in x and y direction).
    :param tipoDati: 
        These are possible values:
            • attivita
            • superficieEdificato
            • superficieAreeServizio
            • struttureVarie
            • tutti
    :return:
    """
    # Start Timer
    start = time.time()

    # Register Job
    job_id, job_path = register_job()

    # Infer Outputs to Produce
    outputs_to_produce = ['superficieEdificato', 'superficieAreeServizio', 'struttureVarie']
    if tipoDati != 'tutti':
        if not tipoDati in outputs_to_produce:
            raise Exception(f"Invalid value supplied for parameter tipoDati: {tipoDati}, \n Valid values are {','.join(outputs_to_produce)}")
        outputs_to_produce = [tipoDati]

    # Json Info Files
    preprocess_json_path = os.path.join(job_path, 'parameter_info.json')
    json_path = os.path.join(job_path, 'job_info.json')
    job_lock = os.path.join(job_path, 'job_info.json.lock')

    # Lock Job
    with open(job_lock, 'w') as f:
        pass

    # Prepare template raster parameters
    x_min = latD
    y_min = lonU
    x_max = latU
    y_max = lonD
    x_min_meter, y_min_meter = project_xy(x_min, y_min, 4326, 3857)
    x_max_meter, y_max_meter = project_xy(x_max, y_max, 4326, 3857)
    cell_size_x_dd = (x_max - x_min)*cellSize/(x_max_meter - x_min_meter)
    cell_size_y_dd = (y_max - y_min)*cellSize/(y_max_meter - y_min_meter)

    # Dimension of Raster
    n_rows = math.ceil((x_max - x_min) / cell_size_x_dd)
    n_cols = math.ceil((y_max - y_min) / cell_size_y_dd)

    # Rasterization template query
    raster_template_sql = f""" 
        SELECT ST_SetBandNoDataValue(ST_MakeEmptyRaster({n_cols}, {n_rows}, {x_min}, {y_max}, {cell_size_x_dd}, {cell_size_y_dd}, {0}, {0}, 4326), {nodata})
    """
    print(raster_template_sql)

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
    selection_query_rasterize = f"""
        q_ras AS (
            SELECT
                ST_Union(
                    ST_AsRaster(
                        q.geom,
                        ({raster_template_sql}),
                        '8BUI'::text, 
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
        "outputsToProduce": outputs_to_produce,
        "rasterInfo": {
            "cell_size_x_dd": cell_size_x_dd,
            "cell_size_y_dd": cell_size_y_dd,
            "n_rows": n_rows,
            "n_cols": n_cols
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

    for _tipo_dati in outputs_to_produce:

        # Basics
        datatype_id = get_datatype_id(tipoDati)
        datatype_info = get_datatype_info_by_id(datatype_id)
        data_table = datatype_info[0]['TableName']
        classes=[['building']]#[row['GroupName'] for row in datatype_info]
        output_raster_filepath = os.path.join(job_path, f'{_tipo_dati}.tiff')
        output_raster_weburl = f"{base_path}/{str(job_id)}/{_tipo_dati}.tiff"

        # Make Data Retrieval Query
        data_ret_query = ""

        # Use the Selection query to select features from source table
        feature_selection_query_store = []
        for i, dtype in enumerate(datatype_info):
            _features_selection_query = get_feature_selection_query(
                feature_table=data_table,
                classes=classes[i],
                geom_column='wkb_geometry',
                class_column='fclass',
                code_column='code'
            )
            features_selection_query = f"""
                features_{i} AS (
                    {_features_selection_query}      
                )
            """
            feature_selection_query_store.append(features_selection_query)
        feature_selection_query = "\n,".join(feature_selection_query_store)

        # Create data points for each bands of each raster
        band_idx = 0
        data_points_store = []
        for f_i, feature_table in enumerate(datatype_info):
            f_classes = classes[f_i]
            class_query = ""
            if not f_classes == ['all']:
                class_query = f"WHERE {get_class_query(f_classes, 't.class')}"
            f_classes_query = f"""
                WITH 
                    {selection_query},
                    {feature_selection_query_store[f_i]}
                SELECT 
                    t.class as class,
                    MIN(t.code)::text as code,
                    COUNT(t.class) as num_features
                FROM 
                    features_{f_i} t
                {class_query}
                GROUP BY
                    class
            """
            f_classes_db = query_db(f_classes_query, cursor_factory=None)
            class_mapping = {x[0]: x for x in f_classes_db}
            if f_classes == ['all']:
                f_classes_store = f_classes_db
            else:
                f_classes_store = []
                default = [None, None, 0]
                for _f_class in f_classes:
                    row = [_f_class, class_mapping.get(_f_class, default)[1], class_mapping.get(_f_class, default)[2]]
                    f_classes_store.append(row)
            if expand_bands:
                for f_class_row in f_classes_store:
                    band_idx += 1
                    class_name = f_class_row[0]
                    class_code = f_class_row[1]
                    num_features = f_class_row[2]

                    # Prepare band info
                    # band_info = {
                    #     "band_id": band_idx,
                    #     "layer_name": rev_op_tables[feature_table],
                    #     "layer_table": feature_table,
                    #     "class_code": [class_code],
                    #     "class_name": [class_name],
                    #     "features": [num_features]
                    # }
                    # raster_info['layers'].append(band_info)

                    # Prepare data point query
                    class_code_flt = ""
                    if class_code is not None:
                        class_code_flt = f"""
                            AND
                            f.code={class_code}
                        """
                    # data_point_query = f"""
                    #     data_point_{band_idx} AS (
                    #         SELECT ST_Collect(ST_Centroid(q.geom)) as geom
                    #         FROM
                    #             features_{f_i} f,
                    #             q_poly q
                    #         WHERE
                    #             ST_Intersects(f.geom, q.geom)
                    #             {class_code_flt}
                    #     )
                    # """
                    data_point_query = f"""
                        data_point_{band_idx} AS (
                            WITH d_ras AS (
                                SELECT  
                                    ST_Union(
                                        ST_AsRaster(
                                            f.geom,
                                            ({raster_template_sql}),
                                            '8BUI'::text, 
                                            {positive}, 
                                            {negative},
                                            true
                                        ),
                                        'Max'
                                    ) AS ras
                                FROM
                                    features_{f_i} f
                                WHERE 
                                    f.code={class_code}
                            )

                            SELECT 
                                ST_Collect(ST_Centroid((pp).geom)) as geom
                            FROM
                                (
                                    SELECT 
                                        ST_PixelAsPoints(d_ras.ras, 1) pp
                                    FROM d_ras
                                ) a
                            WHERE
                                (pp).val={positive}
                        )
                    """
                    data_points_store.append(data_point_query)
            else:
                band_idx += 1
                # band_info = {
                #     "band_id": band_idx,
                #     "layer_name": rev_op_tables[feature_table],
                #     "layer_table": feature_table,
                #     "class_code": [x[1] for x in f_classes_store],
                #     "class_name": [x[0] for x in f_classes_store],
                #     "features": [x[2] for x in f_classes_store]
                # }
                # raster_info['layers'].append(band_info)
                data_point_query = f"""
                    data_point_{band_idx} AS (
                        SELECT ST_Collect(ST_Centroid(q.geom)) as geom
                        FROM 
                            features_{f_i} f,
                            q_poly q
                        WHERE 
                            ST_Intersects(q.geom, f.geom)     
                    )
                """
                data_points_store.append(data_point_query)
        data_points_query = "\n,".join(data_points_store)

        # Create Blank Resultant Raster
        datatype_array = ", ".join(band_idx * ["'8BUI'::text"])
        negative_val_array = band_idx * [negative]
        nodata_val_array = band_idx * [nodata]
        blank_data_raster_query = f"""
            blank_ras AS (
                SELECT
                        ST_AsRaster(
                            q.geom,
                            ({raster_template_sql}),
                            ARRAY[{datatype_array}], 
                            ARRAY{negative_val_array}, 
                            ARRAY{nodata_val_array}
                        ) AS ras
                FROM q
            )
        """

        # fill in the blank raster with data points
        _prameterize_blank_raster_query = 'blank_ras.ras'
        data_points_name_list = []
        for _b_idx in range(0, band_idx):
            b_idx = _b_idx + 1
            data_point = f"data_point_{b_idx}"
            _prameterize_blank_raster_query = f"""
                ST_SetValue(
                    {_prameterize_blank_raster_query},
                    {b_idx},
                    {data_point}.geom,
                    {positive}
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
        output_raster_content = query_db(out_raster_query)[0]['outraster']
        time_taken_raster_query = time.time()-start_raster_query


        # FLush Raster to Disk
        with open(output_raster_filepath, 'wb') as f:
            f.write(bytes(output_raster_content))

        # Make Layer Info Query
        layer_info_query = data_ret_query

        # Execute layer Info Query
        start_info_query = time.time()
        layer_info = ""#query_db(layer_info_query)
        time_taken_info_query = time.time()-start_info_query

        # Format Layer Info
        layers = [layer_info]

        # Flush Info to Json
        json_info[_tipo_dati] = {
            #"dataRetQuery": data_ret_query,
            #"RasterQuery": out_raster_query,
            #"layerInfoQuery": layer_info,
            "ouputGeotiffFile": output_raster_weburl,
            "timeTakenInfoQuery": time_taken_info_query,
            "timeTakenRasterQuery": time_taken_raster_query,
            "time_units": "Seconds",
            "layers": layers
        }

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
