import json
import time
from .database import query_db
from .database_apis import get_datatype_id
from .subroutines import make_query

def readDataByArea(
        latU: float,
        lonU: float,
        latD: float,
        lonD: float,
        tipoDati: str
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
    :return: 
    """
    start = time.time()
    datatype_id = get_datatype_id(tipoDati)
    query = make_query(datatype_id)

    start_query = time.time()
    raster = query_db(query)[0][0]
    time_query = time.time()-start_query
    # Write Raster

    with open(output_raster, 'wb') as f:
        f.write(bytes(raster))

    json_info = {
        "parameters": {
            "latU": latU,
            "lonU": lonU,
            "latD": latD,
            "lonD": lonD,
            "tipoDati": [
                tipoDati
            ]
        },
        "geotiff_file": "filename.tiff",
        "messages": [],
        "time_taken_query": time_query,
        "time_taken_total": time.time()-start,
        "time_units": "Seconds"
    }

    return json_info

    with open(json_info, 'w') as f:
        json.dump(json_info, f)