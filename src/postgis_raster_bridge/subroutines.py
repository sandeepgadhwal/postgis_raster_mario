import os
import pyproj
from .config import jobs_directory

## Geometry Functions start ##
def project_xy(x: float, y: float, source_srs: int, target_srs: int):
    x, y = pyproj.transform(pyproj.Proj(init=f'epsg:{source_srs}'), pyproj.Proj(init=f'epsg:{target_srs}'), x, y)
    if type(x) == tuple:
        (x,), (y,) = x, y
    return x, y

def center_hw_to_polygon(x, y, height, width):
    x_left = x - width/2
    x_right = x_left + width
    y_lower = y - height/2
    y_upper = y_lower + height
    return x_left, y_lower, x_right, y_upper

## Geometry Functions end ##

def make_query(datatype_id):
    raise NotImplemented


    feature_to_raster_sql = f"""
        WITH {out_raster_sql},
        out_raster AS (
            SELECT
                ST_Transform(
                    r.ras,
                    {out_srid}
                ) AS ras
            FROM
                raster_w_values r
        )
        SELECT 
            ST_AsTIFF(
                out_raster.ras,
                'LZW'
            )
        FROM out_raster
    """

def register_job():
    job_id = create_job_id()
    while True:
        job_path = os.path.join(jobs_directory, str(job_id))
        if os.path.exists(job_path):
            job_id = create_job_id()
            continue
        else:
            os.mkdir(job_path)
        break
    return job_id, job_path

def create_job_id():
    if not os.path.exists(jobs_directory):
        os.mkdir(jobs_directory)
    job_store_path = os.path.join(jobs_directory, 'job_index.txt')
    #job_store_path = 'a.txt'
    prev_job_id = 0
    if os.path.exists(job_store_path):
        with open(job_store_path, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            prev_job_id = int(f.read().decode()[:-1])
    cur_job_id = prev_job_id+1
    with open(job_store_path, 'a+') as f:
        f.write(f"{cur_job_id}\n")
    return cur_job_id


