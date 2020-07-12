import os
import json
import math
import time
from .database import query_db
from .database_apis import get_datatype_id
from .subroutines import make_query, register_job, project_xy
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

    # Prepare tempelate raster parameters
    x_min = latU
    y_min = lonD
    x_max = latD
    y_max = lonU
    x_min_meter, y_min_meter = project_xy(x_min, y_min, 4326, 3857)
    x_max_meter, y_max_meter = project_xy(x_max, y_max, 4326, 3857)
    cell_size_x_dd = (x_max - x_min)*cellSize/(x_max_meter - x_min_meter)
    cell_size_y_dd = (y_max - y_min)*cellSize/(y_max_meter - y_min_meter)

    # Dimension of Raster
    n_rows = math.ceil((x_max - x_min) / cell_size_x_dd)
    n_cols = math.ceil((y_max - y_min) / cell_size_y_dd)

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
        output_raster_filepath = os.path.join(job_path, f'{_tipo_dati}.tiff')
        output_raster_weburl = f"base_path/{str(job_id)}/{_tipo_dati}.tiff"

        # Make Data Retrieval Query
        data_ret_query =

        # Make Raster Query
        raster_query = data_ret_query

        # Execute Raster Query
        start_raster_query = time.time()
        output_raster_content = query_db(raster_query)
        time_taken_raster_query = time.time()-start_raster_query


        # FLush Raster to Disk
        with open(output_raster_filepath, 'wb') as f:
            f.write(bytes(output_raster_content))

        # Make Layer Info Query
        layer_info_query = data_ret_query

        # Execute layer Info Query
        start_info_query = time.time()
        layer_info = query_db(layer_info_query)
        time_taken_info_query = time.time()-start_info_query

        # Format Layer Info
        layers = [layer_info]

        # Flush Info to Json
        json_path[_tipo_dati] = {
            "dataRetQuery": data_ret_query,
            "RasterQuery": raster_query,
            "layerInfoQuery": layer_info,
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
    with open(json_info, 'w') as f:
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
