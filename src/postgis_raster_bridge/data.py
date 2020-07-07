import os
import tempfile
import zipfile
import shutil
import subprocess
import glob
from osgeo import ogr

from .utils import download_file
from .db_config import host, port, database, user, password

def update_data(force_download=True):
    # Download File
    latest_osm_file = "https://download.geofabrik.de/europe/italy/sud-latest-free.shp.zip"
    temp_filestore = tempfile.gettempdir()
    file_save_path = os.path.join(temp_filestore, os.path.basename(latest_osm_file))
    if not os.path.exists(file_save_path) and force_download:
        download_file(latest_osm_file, file_save_path)

    # Unzip File
    print("--Extracting files")
    extractpath = os.path.join(temp_filestore, 'sud_osm')
    if os.path.exists(extractpath):
        shutil.rmtree(extractpath)
    with zipfile.ZipFile(file_save_path) as zip_ref:
        zip_ref.extractall(extractpath)

    # Flush to postgres
    shapefiles = glob.glob(os.path.join(extractpath, '*.shp'))
    print(shapefiles)
    status = {}
    for i, shapefile in enumerate(shapefiles):
        print(f"--ingesting ({i}/{len(shapefiles)}) {shapefile}")
        table_name = os.path.basename(shapefile).lower().strip().replace(' ', '_')[:-4]
        epsg_code = 4326
        geometry_type = None
        ds = ogr.Open(shapefile)
        lyr = ds.GetLayer(0)
        geom_type = lyr.GetGeomType()
        if geom_type in [3]:
            geometry_type = "MultiPolygon"
        elif geom_type in [2]:
            geometry_type = "MultiLineString"
        elif geom_type in [1]:
            geometry_type = "MultiPoint"
        if geometry_type is not None:
            geometry_type_string = f" -nlt {geometry_type}"
        else:
            geometry_type_string = ""
        command = f"""
        ogr2ogr -f "PostgreSQL" PG:"host={host} port={port} dbname={database} user={user} password={password}" "{shapefile}" --config PG_USE_COPY YES  -nln {table_name} {geometry_type_string} -overwrite -progress -lco geometry_name=wkb_geometry -lco precision=NO -t_srs EPSG:{epsg_code}
        """.strip()
        print(command)
        output = subprocess.check_output(command, shell=True)
        print(output)
        status[table_name] = [True, output]
    #
    return status
