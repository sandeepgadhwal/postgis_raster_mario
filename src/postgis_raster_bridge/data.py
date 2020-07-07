import requests

latest_osm_file = "https://download.geofabrik.de/europe/italy/sud-latest-free.shp.zip"

with request.get(latest_osm_file) a