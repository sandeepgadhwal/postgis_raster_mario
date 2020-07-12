import os

# Working directory to write images
working_directory = "/home/sandy/workspace/mario ricci/2-postgis2raster-sud-v2"
jobs_directory = os.path.join(working_directory, "sevara_jobs")

# Base path for accessing images thorugh URL
apihost = "http://localhost:5000"
base_path = "/sevara/v1/jobs/readDataByArea"