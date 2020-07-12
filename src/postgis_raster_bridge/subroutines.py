import pyproj

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