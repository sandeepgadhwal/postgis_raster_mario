# postgis_raster_mario
postgis_raster_mario


## Configure Environment
Install conda from [here](https://www.anaconda.com/products/individual)
  
Use the following command to configure environment
```shell script
conda create -n postgisrasterenv -f environment.yaml
```

## Activate Enviornment
use the following command
```shell script
conda activate postgisrasterenv
```

## Run Flask App
use the following command
```shell script
cd src
sudo chmod +x start_flask_app.sh
start_flask_app.sh
```

## Change Flask App configuration
If you want to change the port, hostname of flask app. 
Edit the file 'start_flask_app.sh', It looks like this.
```shell script
export FLASK_APP=flask_app.py
export FLASK_RUN_HOST=localhost
export FLASK_RUN_PORT=5000
flask run
```

## Change the Main App Configuration
If you want to change the configuration of main app such as 
- the location to save images
- hostname
- port
- API Base Path
  
Edit the file './src/postgis_raster_bridge/config.py', It looks like this.
```python
# Working directory to write images
working_directory = "/home/sandy/workspace/mario ricci/2-postgis2raster-sud-v2/image_store/"

# Base path for accessing images thorugh URL
apihost = "http://localhost:5000"
base_path = "/sevara/v1"
image_result_path = base_path + "/results"
```

## Change Main App database configuration
If you want to change the configuration of databse such as 
- DB server Port
- DB server Hostname
- Database Name
- Database User
- Database User password  

Edit the file './src/postgis_raster_bridge/db_config.py', It looks like this.
```python
host = 'localhost'
port = 5432
database = 'postgis_raster_mario'
user = 'postgres'
password = 'postgres'
```